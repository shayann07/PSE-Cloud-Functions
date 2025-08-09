// functions/index.js
const { onCall, HttpsError } = require("firebase-functions/v2/https");
const { onDocumentWritten } = require("firebase-functions/v2/firestore");
const { getFirestore, FieldValue, Timestamp } = require("firebase-admin/firestore");
const functions = require("firebase-functions");
const admin = require("firebase-admin");

if (!admin.apps.length) admin.initializeApp();

const db = getFirestore();

// Milestones (active direct members → one-time USD bonus)
const MILESTONES = [
  { members: 50,    bonus: 100 },
  { members: 100,   bonus: 200 },
  { members: 500,   bonus: 1500 },
  { members: 1000,  bonus: 4000 },
  { members: 5000,  bonus: 25000 },
  { members: 10000, bonus: 60000 },
  { members: 30000, bonus: 150000 },
  { members: 50000, bonus: 300000 },
];

/**
 * Core worker:
 * - counts active directs (users where referralCode == userId AND status == 'active')
 * - awards any newly crossed milestones exactly once
 * - credits accounts.{investment.currentBalance, investment.remainingBalance}
 * - writes a transaction
 * - updates leadership/{userId}
 */
async function computeAndAward(userId) {
  if (!userId) throw new HttpsError("invalid-argument", "Missing userId");

  // 1) Count active directs
  // NOTE: requires composite index on (referralCode ASC, status ASC)
  const directsSnap = await db
    .collection("users")
    .where("referralCode", "==", userId)
    .where("status", "==", "active")
    .get();

  const directActiveCount = directsSnap.size;

  // 2) Transaction to avoid double-award
  return await db.runTransaction(async (tx) => {
    const leadRef = db.collection("leadership").doc(userId);
    const leadDoc = await tx.get(leadRef);
    const previouslyAwarded = (leadDoc.exists && Array.isArray(leadDoc.get("awarded")))
      ? leadDoc.get("awarded")
      : []; // store milestone "members" ints e.g. [50, 100]

    const nowEligible = MILESTONES
      .filter(m => directActiveCount >= m.members)
      .map(m => m.members);

    const newlyCrossed = nowEligible.filter(m => !previouslyAwarded.includes(m));
    let totalBonus = 0;

    if (newlyCrossed.length > 0) {
      // 3) Find account doc for this user
      const acctSnap = await tx.get(
        db.collection("accounts").where("userId", "==", userId).limit(1)
      );
      if (acctSnap.empty) {
        throw new HttpsError("failed-precondition", `No account for userId=${userId}`);
      }
      const accountRef = acctSnap.docs[0].ref;

      // 4) Sum bonuses
      const bonuses = newlyCrossed
        .map(m => MILESTONES.find(x => x.members === m)?.bonus || 0);
      totalBonus = bonuses.reduce((a, b) => a + b, 0);

      // 5) Credit balances
      tx.update(accountRef, {
        "investment.currentBalance": FieldValue.increment(totalBonus),
        "investment.remainingBalance": FieldValue.increment(totalBonus),
        updatedAt: Timestamp.now(),
      });

      // 6) Transaction record
      const txnRef = db.collection("transactions").doc();
      tx.set(txnRef, {
        id: txnRef.id,
        userId,
        type: "leadership_bonus",
        amount: totalBonus,
        currency: "USD",
        status: "completed",
        createdAt: Timestamp.now(),
        meta: {
          milestonesAwarded: newlyCrossed,
        },
        note: `Leadership bonus for milestones: ${newlyCrossed.join(", ")}`,
      });
    }

    // 7) Upsert leadership progress
    tx.set(leadRef, {
      userId,
      directActiveCount,
      awarded: Array.from(new Set([...previouslyAwarded, ...newlyCrossed])).sort((a,b)=>a-b),
      lastCheckedAt: Timestamp.now(),
      lastAwardAmount: totalBonus,
    }, { merge: true });

    return { directActiveCount, newlyCrossed, totalBonus };
  });
}

/**
 * Callable: client asks to recompute right now
 * Usage (Android): Firebase.functions.getHttpsCallable("checkLeadershipBonus").call({ userId })
 */
exports.checkLeadershipBonus = onCall(async (req) => {
  try {
    const userId = req.data?.userId;
    const result = await computeAndAward(userId);
    return result;
  } catch (err) {
    functions.logger.error("checkLeadershipBonus failed", err);
    throw err;
  }
});

/**
 * Trigger: when a user document changes, re-check the upline leader
 * Fires when status/referralCode flips, handling both old and new upline IDs.
 */
exports.onUserWriteUpdateLeadership = onDocumentWritten("users/{docId}", async (event) => {
  const before = event.data?.before?.data() || {};
  const after  = event.data?.after?.data()  || {};

  const uplineBefore = before.referralCode;
  const uplineAfter  = after.referralCode;

  // If relevant fields changed (referralCode/status), recompute uplines
  const affected = new Set();
  if (uplineBefore) affected.add(uplineBefore);
  if (uplineAfter)  affected.add(uplineAfter);

  // Only bother if status potentially changed counts
  const statusChanged = before.status !== after.status;

  if (statusChanged || uplineBefore !== uplineAfter) {
    await Promise.all(Array.from(affected).map(async (uid) => {
      try { await computeAndAward(uid); } catch (e) { functions.logger.warn(e); }
    }));
  }
});