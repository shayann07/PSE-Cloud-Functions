// ─────────────────────────────────────────────────────────────
// SALARY PROGRAM (single-file version for index.js)
// ─────────────────────────────────────────────────────────────
const { onCall, HttpsError } = require("firebase-functions/v2/https");
const { onSchedule }         = require("firebase-functions/v2/scheduler");
const { logger }             = require("firebase-functions");
const { initializeApp, getApps } = require("firebase-admin/app");
const { getFirestore, Timestamp, FieldValue } = require("firebase-admin/firestore");

// Init Admin SDK once (safe even if index.js already initialized elsewhere)
if (!getApps().length) initializeApp();
const db = getFirestore();

/* ---------- helpers ---------- */
const chunk10 = (arr) => {
  const out = [];
  for (let i = 0; i < arr.length; i += 10) out.push(arr.slice(i, i + 10));
  return out;
};

function addDays(ts, days) {
  const d = ts.toDate();
  d.setUTCDate(d.getUTCDate() + days);
  return Timestamp.fromDate(d);
}

function addMonths(ts, months) {
  const d = ts.toDate();
  d.setUTCMonth(d.getUTCMonth() + months);
  return Timestamp.fromDate(d);
}

function toPeriodKey(ts) {
  const d = ts.toDate();
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, "0");
  return `${y}-${m}`;
}

function salaryForActiveDirectBusiness(usd) {
  // tiers from your spec/photo
  if (usd >= 100000) return { tier: 8, amount: 12000 };
  if (usd >=  50000) return { tier: 7, amount:  5000 };
  if (usd >=  30000) return { tier: 6, amount:  2500 };
  if (usd >=  15000) return { tier: 5, amount:  1200 };
  if (usd >=   7000) return { tier: 4, amount:   500 };
  if (usd >=   4000) return { tier: 3, amount:   250 };
  if (usd >=   2000) return { tier: 2, amount:   100 };
  if (usd >=   1500) return { tier: 1, amount:    50 };
  return { tier: 0, amount: 0 };
}

/**
 * Level-1 + active only:
 * Sum accounts.investment.totalDeposit for users where referralCode==rootUid AND status=="active"
 */
async function computeActiveDirectBusiness(rootUid) {
  const actives = await db.collection("users")
    .where("referralCode", "==", rootUid)
    .where("status", "==", "active")
    .get();

  const uids = actives.docs.map(d => d.get("uid")).filter(Boolean);
  if (!uids.length) return 0;

  let total = 0;
  for (const chunk of chunk10(uids)) {
    const accSnap = await db.collection("accounts").where("userId", "in", chunk).get();
    accSnap.forEach(acc => {
      const inv = acc.get("investment") || {};
      total += inv.totalDeposit || 0;
    });
  }
  return total;
}

/* ─────────────────────────────────────────────────────────────
 * ensureSalaryProfile (callable)
 *  - Creates/ensures salaryProfiles/{userId}
 *  - windowStart = users.createdAt ; windowEnd = +30 days
 *  - status = "window_open"
 *  - idempotent
 * ───────────────────────────────────────────────────────────── */
const ensureSalaryProfile = onCall({ region: "us-central1" }, async (req) => {
  const userId = req.data && req.data.userId;
  if (!userId) throw new HttpsError("invalid-argument", "userId is required");

  const userSnap = await db.collection("users").where("uid", "==", userId).limit(1).get();
  if (userSnap.empty) throw new HttpsError("not-found", "User not found");

  const user = userSnap.docs[0];
  const createdAt = user.get("createdAt") || Timestamp.now();
  const status    = user.get("status") || "inactive";

  const ref = db.collection("salaryProfiles").doc(userId);
  const prof = await ref.get();

  if (prof.exists) {
    logger.info(`ensureSalaryProfile: exists for ${userId}`);
    return { ok: true, existed: true };
  }

  await ref.set({
    userId,
    status: "window_open",
    windowStart: createdAt,
    windowEnd: addDays(createdAt, 30),
    snapshotDirectBusiness: 0,
    salaryAmount: 0,
    tier: 0,
    nextPayoutAt: null,
    lastPayoutAt: null,
    createdAt: FieldValue.serverTimestamp(),
    updatedAt: FieldValue.serverTimestamp(),
    userStatusAtInit: status
  }, { merge: true });

  logger.info(`ensureSalaryProfile: created for ${userId}`);
  return { ok: true, existed: false };
});

/* ─────────────────────────────────────────────────────────────
 * getSalaryProfile (callable) — returns profile for UI
 * ───────────────────────────────────────────────────────────── */
const getSalaryProfile = onCall({ region: "us-central1" }, async (req) => {
  const userId = req.data && req.data.userId;
  if (!userId) throw new HttpsError("invalid-argument", "userId is required");

  const ref = db.collection("salaryProfiles").doc(userId);
  const doc = await ref.get();
  if (!doc.exists) return { exists: false };

  return { exists: true, profile: doc.data() };
});

/* ─────────────────────────────────────────────────────────────
 * salaryScheduler (scheduled daily)
 *  Phase A: lock windows at D+30 (snapshot ADB, compute tier)
 *  Phase B: pay monthly when nextPayoutAt <= now
 *  Idempotent via transactions docId: salary_{userId}_{yyyy-MM}
 * ───────────────────────────────────────────────────────────── */
const salaryScheduler = onSchedule(
  { region: "us-central1", schedule: "every 24 hours", timeZone: "Etc/UTC" },
  async () => {
    const now = Timestamp.now();

    // Phase A: Lock windows
    const toLock = await db.collection("salaryProfiles")
      .where("status", "==", "window_open")
      .where("windowEnd", "<=", now)
      .get();

    for (const doc of toLock.docs) {
      const { userId, windowEnd } = doc.data();
      const userSnap = await db.collection("users").where("uid", "==", userId).limit(1).get();

      if (userSnap.empty) {
        await doc.ref.update({ status: "ended", updatedAt: FieldValue.serverTimestamp(), reason: "user_missing" });
        continue;
      }

      const user = userSnap.docs[0];
      const isActive = user.get("status") === "active";
      if (!isActive) {
        await doc.ref.update({
          status: "ended",
          updatedAt: FieldValue.serverTimestamp(),
          reason: "user_inactive_on_lock"
        });
        continue;
      }

      const adb = await computeActiveDirectBusiness(userId);
      const { tier, amount } = salaryForActiveDirectBusiness(adb);

      if (amount <= 0) {
        await doc.ref.update({
          status: "ended",
          snapshotDirectBusiness: adb,
          tier, salaryAmount: 0,
          updatedAt: FieldValue.serverTimestamp(),
          reason: "no_tier"
        });
        continue;
      }

      await doc.ref.update({
        status: "active",
        snapshotDirectBusiness: adb,
        tier, salaryAmount: amount,
        nextPayoutAt: windowEnd, // first payout at D+30
        updatedAt: FieldValue.serverTimestamp()
      });

      logger.info(`Locked salary for ${userId}: ADB=${adb}, tier=${tier}, $${amount}/mo`);
    }

    // Phase B: Payouts due
    const due = await db.collection("salaryProfiles")
      .where("status", "==", "active")
      .where("nextPayoutAt", "<=", now)
      .get();

    for (const doc of due.docs) {
      const p = doc.data();
      const userId = p.userId;
      const periodKey = toPeriodKey(p.nextPayoutAt);

      // still active?
      const userSnap = await db.collection("users").where("uid", "==", userId).limit(1).get();
      const stillActive = !userSnap.empty && userSnap.docs[0].get("status") === "active";
      if (!stillActive) {
        await doc.ref.update({ status: "ended", updatedAt: FieldValue.serverTimestamp(), reason: "user_inactive_on_pay" });
        continue;
      }

      const txRef = db.collection("transactions").doc(`salary_${userId}_${periodKey}`);
      const accSnap = await db.collection("accounts").where("userId", "==", userId).limit(1).get();
      if (accSnap.empty) {
        logger.warn(`No account for ${userId}, skipping salary`);
        continue;
      }
      const accRef = accSnap.docs[0].ref;

      await db.runTransaction(async (tr) => {
        const existing = await tr.get(txRef);
        if (existing.exists) return; // idempotent

        const accDoc = await tr.get(accRef);
        const inv = accDoc.get("investment") || {};
        const cb  = (inv.currentBalance || 0) + (p.salaryAmount || 0);
        const rb  = (inv.remainingBalance || 0) + (p.salaryAmount || 0);

        tr.update(accRef, {
          "investment.currentBalance": cb,
          "investment.remainingBalance": rb
        });

        tr.set(txRef, {
          userId,
          amount: p.salaryAmount,
          type: "salary",
          address: "Salary Program",
          status: "credited",
          balanceUpdated: true,
          periodKey,
          timestamp: FieldValue.serverTimestamp()
        });

        tr.update(doc.ref, {
          lastPayoutAt: p.nextPayoutAt,
          nextPayoutAt: addMonths(p.nextPayoutAt, 1),
          updatedAt: FieldValue.serverTimestamp()
        });
      });

      logger.info(`Paid salary for ${userId} period=${periodKey} amount=${p.salaryAmount}`);
    }
  }
);

// Exports (keep your other exports above/below as needed)
exports.ensureSalaryProfile = ensureSalaryProfile;
exports.getSalaryProfile    = getSalaryProfile;
exports.salaryScheduler     = salaryScheduler;
