"use strict";

/**
 * PSE – Nightly ROI + Team Profit (HYPER-LOGGED & TXN-SAFE)
 * - Phase A: ROI first (sets accounts.earnings.dailyProfit = today's ROI total + lastRoiDate=today)
 * - Phase B: Team Profit second (uses downlines' dailyProfit ONLY if lastRoiDate===today)
 * - Plan expiry by totalAccumulated >= totalPayoutAmount
 * - Idempotent with roiLogs (per user+day), roiPlanLogs (per plan+day), teamLogs (per user+day)
 * - All queries happen outside transactions. Inside tx: gather all doc reads first via tx.get(...),
 *   then perform writes (no tx.get after any write).
 */

const { onSchedule } = require("firebase-functions/v2/scheduler");
const { logger } = require("firebase-functions");
const admin = require("firebase-admin");
const pLimit = require("p-limit");

admin.initializeApp();
const db = admin.firestore();
const FieldValue = admin.firestore.FieldValue;
const Timestamp = admin.firestore.Timestamp;

/* ───────────────────────────────────────────────────────────────
   Time helpers (Asia/Karachi)
   ─────────────────────────────────────────────────────────────── */
const PK_OFFSET_MS = 5 * 60 * 60 * 1000;

function ymdPK(d = new Date()) {
  const pk = new Date(d.getTime() + PK_OFFSET_MS);
  return pk.toISOString().slice(0, 10); // YYYY-MM-DD
}

/* ───────────────────────────────────────────────────────────────
   Utils
   ─────────────────────────────────────────────────────────────── */
const chunk = (arr, n) =>
  [...Array(Math.ceil(arr.length / n))].map((_, i) => arr.slice(i * n, i * n + n));

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function runTxn(fn, max = 8) {
  let attempt = 0;
  while (true) {
    attempt++;
    logger.debug(`↻ runTxn attempt #${attempt}`);
    try {
      return await db.runTransaction(fn, { maxAttempts: 1 });
    } catch (e) {
      const retryable = e.code === 10 || e.code === 4; // ABORTED / DEADLINE_EXCEEDED
      logger.warn(
        `⚠️ runTxn failure (attempt=${attempt}) code=${e.code || "?"} message=${e.message}`
      );
      if (!retryable || attempt >= max) {
        logger.error(`⛔ runTxn giving up after ${attempt} attempts`, e);
        throw e;
      }
      const backoff = Math.min(1500, 75 * 2 ** attempt);
      logger.info(`⏳ runTxn backoff ${backoff}ms then retry`);
      await sleep(backoff);
    }
  }
}

/* ───────────────────────────────────────────────────────────────
   Phase A — ROI (per user)
   ─────────────────────────────────────────────────────────────── */
/**
 * Idempotency:
 *   - One roiLog per user/day prevents double-credit for the user on same day.
 *   - Additionally, we create roiPlanLogs per plan/day (for analytics); we DO NOT read them.
 */
async function creditRoiForUser(userDoc) {
  const uid = userDoc.get("uid");
  const status = (userDoc.get("status") || "").toLowerCase();
  const dateKey = ymdPK();

  logger.info(`▶ [ROI] user=${uid} enter status=${status}`);
  if (!uid) {
    logger.warn(`⛔ [ROI] missing uid on user docId=${userDoc.id}`);
    return;
  }
  if (status !== "active") {
    logger.debug(`⏭ [ROI] skip user=${uid} (not active)`);
    return;
  }

  // Load account (outside tx)
  const acctSnap = await db.collection("accounts").where("userId", "==", uid).limit(1).get();
  if (acctSnap.empty) {
    logger.warn(`⛔ [ROI] user=${uid} account not found`);
    return;
  }
  const acctRef = acctSnap.docs[0].ref;
  logger.debug(`ℹ️ [ROI] user=${uid} accountRef=${acctRef.path}`);

  // Load active plans now (outside tx)
  const plansSnap = await db
    .collection("userPlans")
    .where("userId", "==", uid)
    .where("status", "==", "active")
    .get();

  logger.info(`ℹ️ [ROI] user=${uid} activePlans=${plansSnap.size}`);
  if (plansSnap.empty) {
    logger.debug(`⏭ [ROI] user=${uid} no active plans`);
    return;
  }

  // Per-user daily log (not per-plan): ensures we never double-credit ROI in same PK day.
  const roiLogRef = db.collection("roiLogs").doc(`${uid}_${dateKey}`);

  // Pre-create the transaction doc ref (so we can set it inside tx without generating an ID there)
  const roiTxnRef = db.collection("transactions").doc();

  // Prepare refs to read inside transaction
  const planRefs = plansSnap.docs.map((d) => d.ref);

  await runTxn(async (tx) => {
    // ---- READS FIRST --------------------------------------------------------
    const [acctTxnSnap, roiLogSnap] = await Promise.all([tx.get(acctRef), tx.get(roiLogRef)]);

    if (roiLogSnap.exists) {
      logger.debug(`🔁 [ROI] user=${uid} already credited today (${roiLogRef.id})`);
      return;
    }

    // Read all plan docs within tx
    const planTxnSnaps = [];
    for (const pRef of planRefs) {
      planTxnSnaps.push(await tx.get(pRef));
    }

    // ---- COMPUTE IN MEMORY --------------------------------------------------
    const earnings = acctTxnSnap.get("earnings") || {};
    const invest = acctTxnSnap.get("investment") || {};
    logger.debug(
      `📒 [ROI] user=${uid} pre-account earnings.dailyProfit=${earnings.dailyProfit || 0}, totalEarned=${earnings.totalEarned || 0}, balances={cur:${invest.currentBalance || 0}, rem:${invest.remainingBalance || 0}}`
    );

    let totalRoiToday = 0;
    let anyPlanUpdated = false;
    let processed = 0;
    let credited = 0;
    let expiredNow = 0;

    // We will update all active plans by adding their roiAmount to totalAccumulated and lastCollectedDate.
    // While iterating, compute which plans will expire to decide user deactivation later.
    // Include roiAmount for per-plan log.
    const planUpdates = []; // { ref, roiAmount, newAccum, expire, expireOnly }

    for (const pSnap of planTxnSnaps) {
      processed++;
      const planId = pSnap.id;
      const planStatus = pSnap.get("status");
      const roiAmount = Number(pSnap.get("roiAmount") || 0);
      const currentAccum = Number(pSnap.get("totalAccumulated") || 0);
      const cap = Number(pSnap.get("totalPayoutAmount") || Infinity);

      logger.debug(
        `• [ROI] user=${uid} plan=${planId} status=${planStatus} roiAmount=${roiAmount} totalAccumulated=${currentAccum} cap=${cap}`
      );

      if (planStatus !== "active") {
        logger.debug(`  ⏭ [ROI] user=${uid} plan=${planId} not active at txn time`);
        continue;
      }

      // If already at/over cap, expire WITHOUT crediting more
      if (currentAccum >= cap) {
        planUpdates.push({ ref: pSnap.ref, roiAmount: 0, expire: true, expireOnly: true });
        expiredNow++;
        logger.info(
          `  ⏹ [ROI] user=${uid} plan=${planId} already >= cap (${currentAccum}/${cap}) → EXPIRE (no ROI credit)`
        );
        continue;
      }

      if (roiAmount <= 0) {
        logger.debug(`  ⏭ [ROI] user=${uid} plan=${planId} roiAmount<=0`);
        continue;
      }

      const newAccum = currentAccum + roiAmount;
      const expire = newAccum >= cap;

      planUpdates.push({
        ref: pSnap.ref,
        roiAmount,
        newAccum,
        expire,
        expireOnly: false,
      });
      totalRoiToday += roiAmount;
      anyPlanUpdated = true;
      credited++;

      logger.info(
        `  ✅ [ROI] user=${uid} plan=${planId} +${roiAmount} → totalAccumulated=${newAccum}${
          expire ? " (EXPIRE)" : ""
        }`
      );
      if (expire) expiredNow++;
    }

    logger.info(
      `Σ [ROI] user=${uid} processed=${processed}, credited=${credited}, expiredNow=${expiredNow}, totalRoiToday=${totalRoiToday}`
    );

    // If nothing to credit, we may still have expireOnly updates (plans hit cap). Handle them.
    if (!anyPlanUpdated || totalRoiToday <= 0) {
      const nowTs = Timestamp.now();

      // Apply expireOnly updates if any
      for (const { ref, expireOnly } of planUpdates) {
        if (expireOnly) {
          tx.update(ref, { status: "expired", lastCollectedDate: nowTs });
        }
      }

      // If *all* active plans just expired via expireOnly, deactivate the user
      const hadActive = planTxnSnaps.some((s) => (s.get("status") || "") === "active");
      if (hadActive) {
        const anyStillActive = planTxnSnaps.some((s) => {
          if ((s.get("status") || "") !== "active") return false;
          const upd = planUpdates.find((u) => u.ref.path === s.ref.path);
          if (!upd) return true; // untouched active plan remains active
          return !upd.expire; // non-expiring update remains active
        });
        if (!anyStillActive) {
          tx.update(userDoc.ref, { status: "inactive" });
          logger.info(`📉 [ROI] user=${uid} deactivated (plans hit cap without ROI credit)`);
        }
      }

      logger.debug(`⏭ [ROI] user=${uid} nothing to credit`);
      return;
    }

    // Determine if any active plan remains after our updates
    let anyActiveAfter = false;
    for (const pSnap of planTxnSnaps) {
      const wasActive = (pSnap.get("status") || "") === "active";
      if (!wasActive) continue;

      const upd = planUpdates.find((u) => u.ref.path === pSnap.ref.path);
      if (!upd) {
        anyActiveAfter = true;
        break;
      }
      if (!upd.expire) {
        anyActiveAfter = true;
        break;
      }
    }

    // ---- WRITES (NO MORE READS) --------------------------------------------
    const nowTs = Timestamp.now();

    // 1) Update account (replace dailyProfit, increment others, set lastRoiDate=today)
    tx.update(acctRef, {
      "earnings.dailyProfit": totalRoiToday, // replace
      "earnings.lastRoiDate": dateKey,
      "earnings.totalEarned": FieldValue.increment(totalRoiToday),
      "earnings.totalRoi": FieldValue.increment(totalRoiToday),
      "investment.currentBalance": FieldValue.increment(totalRoiToday),
      "investment.remainingBalance": FieldValue.increment(totalRoiToday),
    });

    // 2) Update each plan and create per-plan daily logs for credited plans
    for (const { ref, roiAmount, newAccum, expire, expireOnly } of planUpdates) {
      if (expireOnly) {
        tx.update(ref, {
          status: "expired",
          lastCollectedDate: nowTs,
        });
      } else {
        tx.update(ref, {
          totalAccumulated: newAccum,
          lastCollectedDate: nowTs,
          ...(expire ? { status: "expired" } : null),
        });

        // Per-plan per-day log (idempotent by deterministic ID; we don't read it)
        const planId = ref.id;
        const roiPlanLogRef = db.collection("roiPlanLogs").doc(`${uid}_${planId}_${dateKey}`);
        tx.set(roiPlanLogRef, {
          userId: uid,
          planId,
          date: dateKey,
          amount: roiAmount,
          creditedAt: nowTs,
          expiredNow: !!expire,
          type: "roi",
        });
      }
    }

    // 3) Single transaction record for ROI (all plans)
    tx.set(roiTxnRef, {
      transactionId: roiTxnRef.id,
      userId: uid,
      type: "dailyRoi",
      amount: totalRoiToday,
      address: "Daily ROI (all active plans)",
      status: "collected",
      balanceUpdated: true,
      timestamp: nowTs,
    });
    logger.info(
      `💸 [ROI] user=${uid} account updated; transactionId=${roiTxnRef.id} amount=${totalRoiToday}`
    );

    // 4) Idempotency log (per user/day)
    tx.set(roiLogRef, { userId: uid, date: dateKey, creditedAt: nowTs });

    // 5) Possibly deactivate user now if no active plans remain after our updates
    if (!anyActiveAfter) {
      tx.update(userDoc.ref, { status: "inactive" });
      logger.info(`📉 [ROI] user=${uid} deactivated (no active plans remain)`);
    }
  });

  logger.info(`◀ [ROI] user=${uid} done`);
}

/* ───────────────────────────────────────────────────────────────
   Phase B — Team Profit (per user)
   ─────────────────────────────────────────────────────────────── */
/**
 * Idempotency:
 *   One teamLog per user/day prevents double-credit of team profit the same day.
 * Computation:
 *   Build levels outside tx. Sum downlines' accounts.earnings.dailyProfit **only if**
 *   accounts.earnings.lastRoiDate === today (prevents stale carry-over).
 * Credits:
 *   - Increment account teamProfit/totalEarned/currentBalance/remainingBalance
 *   - Add totalTeamProfit to totalAccumulated of each active plan; expire when >= cap
 *   - One transaction record of type "teamProfit"
 */
async function creditTeamForUser(userDoc) {
  const uid = userDoc.get("uid");
  const status = (userDoc.get("status") || "").toLowerCase();
  const dateKey = ymdPK();

  logger.info(`▶ [TEAM] user=${uid} enter status=${status}`);
  if (!uid) return;
  if (status !== "active") {
    logger.debug(`⏭ [TEAM] skip user=${uid} (not active)`);
    return;
  }

  // If already logged for today, skip quickly (outside tx)
  const teamLogRef = db.collection("teamLogs").doc(`${uid}_${dateKey}`);
  if ((await teamLogRef.get()).exists) {
    logger.debug(`🔁 [TEAM] user=${uid} already credited today (${teamLogRef.id})`);
    return;
  }

  // Load team settings & build levels outside tx
  const settingsSnap = await db.collection("teamSettings").orderBy("level").get();
  const settings = settingsSnap.docs.map((d) => d.data());
  logger.info(`ℹ️ [TEAM] user=${uid} levels=${settings.length}`);

  // BFS frontier
  let frontier = [uid];
  let totalTeamProfit = 0;
  const levelSummaries = [];

  for (const cfg of settings) {
    const level = Number(cfg.level);
    const req = Number(cfg.requiredMembers || 0);
    const pct = Number(cfg.profitPercentage || 0);

    logger.debug(`• [TEAM] user=${uid} L${level} frontier=${frontier.length} req=${req} pct=${pct}`);

    // Children (active) of current frontier
    const childDocs = [];
    for (const parentChunk of chunk(frontier, 10)) {
      logger.debug(
        `  🔎 [TEAM] user=${uid} L${level} querying users with referralCode in ${JSON.stringify(
          parentChunk
        )}`
      );
      const q = await db
        .collection("users")
        .where("referralCode", "in", parentChunk)
        .where("status", "==", "active")
        .get();
      logger.debug(`  📄 [TEAM] user=${uid} L${level} fetched active directs=${q.size}`);
      childDocs.push(...q.docs);
    }

    const childUids = childDocs.map((d) => d.get("uid")).filter(Boolean);

    // Sum their dailyProfit from accounts **only if lastRoiDate is today**
    let levelDailyProfit = 0;
    for (const uidChunk of chunk(childUids, 10)) {
      logger.debug(
        `  🔎 [TEAM] user=${uid} L${level} load accounts for ${JSON.stringify(uidChunk)}`
      );
      const accSnap = await db.collection("accounts").where("userId", "in", uidChunk).get();
      accSnap.forEach((a) => {
        const earn = a.get("earnings") || {};
        const d = Number(earn.dailyProfit || 0);
        const last = (earn.lastRoiDate || "").toString();
        if (last === dateKey) {
          levelDailyProfit += d;
        } else {
          // stale dailyProfit ignored
        }
      });
    }

    const unlocked = childUids.length >= req;
    const share = unlocked ? (levelDailyProfit * pct) / 100 : 0;
    totalTeamProfit += share;
    levelSummaries.push({
      level,
      directs: childUids.length,
      levelDailyProfit,
      unlocked,
      share,
    });

    logger.info(
      `  Σ [TEAM] user=${uid} L${level} directs=${childUids.length} dailyProfit=${levelDailyProfit} unlocked=${unlocked} share=${share}`
    );

    frontier = childUids; // next depth
  }

  logger.info(
    `Σ [TEAM] user=${uid} totalTeamProfit=${totalTeamProfit}; detail=${JSON.stringify(
      levelSummaries
    )}`
  );
  if (totalTeamProfit <= 0) {
    logger.debug(`⏭ [TEAM] user=${uid} nothing to credit`);
    return;
  }

  // Load account & active plans (outside tx)
  const acctSnap = await db.collection("accounts").where("userId", "==", uid).limit(1).get();
  if (acctSnap.empty) {
    logger.warn(`⛔ [TEAM] user=${uid} account not found`);
    return;
  }
  const acctRef = acctSnap.docs[0].ref;

  const activePlansSnap = await db
    .collection("userPlans")
    .where("userId", "==", uid)
    .where("status", "==", "active")
    .get();

  logger.debug(`ℹ️ [TEAM] user=${uid} activePlansForBump=${activePlansSnap.size}`);

  // Precreate transaction doc ID
  const teamTxnRef = db.collection("transactions").doc();

  // Prepare refs for tx reads
  const planRefs = activePlansSnap.docs.map((d) => d.ref);

  await runTxn(async (tx) => {
    // ---- READS FIRST --------------------------------------------------------
    const [acctTxnSnap, teamLogSnap] = await Promise.all([tx.get(acctRef), tx.get(teamLogRef)]);
    if (teamLogSnap.exists) {
      logger.debug(`🔁 [TEAM] user=${uid} log appeared during txn; skip credit`);
      return;
    }

    const planTxnSnaps = [];
    for (const pRef of planRefs) {
      planTxnSnaps.push(await tx.get(pRef));
    }

    // ---- COMPUTE ------------------------------------------------------------
    // Build updates for each active plan
    const planUpdates = []; // { ref, newAccum, expire, expireOnly }
    let bumped = 0;
    let expiredNow = 0;

    for (const pSnap of planTxnSnaps) {
      const planId = pSnap.id;
      const status = pSnap.get("status");
      if ((status || "") !== "active") {
        logger.debug(`  ⏭ [TEAM] user=${uid} plan=${planId} not active at txn time`);
        continue;
      }
      const currentAccum = Number(pSnap.get("totalAccumulated") || 0);
      const cap = Number(pSnap.get("totalPayoutAmount") || Infinity);

      // If already at/over cap, expire WITHOUT team credit
      if (currentAccum >= cap) {
        planUpdates.push({ ref: pSnap.ref, expire: true, expireOnly: true });
        expiredNow++;
        logger.info(
          `  ⏹ [TEAM] user=${uid} plan=${planId} already >= cap (${currentAccum}/${cap}) → EXPIRE (no team credit)`
        );
        continue;
      }

      const newAccum = currentAccum + totalTeamProfit;
      const expire = newAccum >= cap;

      planUpdates.push({ ref: pSnap.ref, newAccum, expire, expireOnly: false });
      bumped++;
      if (expire) {
        logger.info(`  ⏹ [TEAM] user=${uid} plan=${planId} EXPIRED after team credit (cap=${cap})`);
        expiredNow++;
      } else {
        logger.debug(
          `  ➕ [TEAM] user=${uid} plan=${planId} +${totalTeamProfit} → ${newAccum}/${cap}`
        );
      }
    }

    // Determine if any plan remains active after our updates
    let anyActiveAfter = false;
    for (const pSnap of planTxnSnaps) {
      const wasActive = (pSnap.get("status") || "") === "active";
      if (!wasActive) continue;
      const upd = planUpdates.find((u) => u.ref.path === pSnap.ref.path);
      if (!upd) {
        anyActiveAfter = true;
        break;
      }
      if (!upd.expire) {
        anyActiveAfter = true;
        break;
      }
    }

    // ---- WRITES (NO MORE READS) --------------------------------------------
    // 1) increment account fields (NOTE: do NOT touch earnings.dailyProfit here)
    tx.update(acctRef, {
      "earnings.teamProfit": FieldValue.increment(totalTeamProfit),
      "earnings.totalEarned": FieldValue.increment(totalTeamProfit),
      "investment.currentBalance": FieldValue.increment(totalTeamProfit),
      "investment.remainingBalance": FieldValue.increment(totalTeamProfit),
    });
    logger.info(`💰 [TEAM] user=${uid} account increments queued amount=${totalTeamProfit}`);

    // 2) apply plan updates
    const nowTs = Timestamp.now();
    for (const { ref, newAccum, expire, expireOnly } of planUpdates) {
      if (expireOnly) {
        tx.update(ref, {
          status: "expired",
          lastCollectedDate: nowTs,
        });
      } else {
        tx.update(ref, {
          totalAccumulated: newAccum,
          lastCollectedDate: nowTs,
          ...(expire ? { status: "expired" } : null),
        });
      }
    }

    // 3) single transaction record
    tx.set(teamTxnRef, {
      transactionId: teamTxnRef.id,
      userId: uid,
      type: "teamProfit",
      amount: totalTeamProfit,
      address: "Team Profit (Levels 1–8)",
      status: "collected",
      balanceUpdated: true,
      timestamp: nowTs,
    });
    logger.info(`🧾 [TEAM] user=${uid} transactionId=${teamTxnRef.id} amount=${totalTeamProfit}`);

    // 4) idempotency log
    tx.set(teamLogRef, { userId: uid, date: dateKey, creditedAt: nowTs });

    // 5) possibly deactivate user
    if (!anyActiveAfter) {
      tx.update(userDoc.ref, { status: "inactive" });
      logger.info(`📉 [TEAM] user=${uid} deactivated (no active plans remain)`);
    }

    logger.debug(`  ✔ [TEAM] user=${uid} plansBumped=${bumped}, expiredNow=${expiredNow}`);
  });

  logger.info(`◀ [TEAM] user=${uid} done`);
}

/* ───────────────────────────────────────────────────────────────
   Orchestration
   ─────────────────────────────────────────────────────────────── */
async function runPhase(name, perUser, concurrency = 40) {
  logger.info(`???? Phase START: ${name} (concurrency=${concurrency}) ????`);
  const limit = pLimit(concurrency);

  const PAGE = 500;
  let last = null;
  let page = 0;
  let usersTotal = 0;

  while (true) {
    let q = db
      .collection("users")
      .where("status", "in", ["active"]) // exclude inactive/blocked implicitly
      .orderBy("__name__")
      .limit(PAGE);

    if (last) q = q.startAfter(last);
    const snap = await q.get();

    if (snap.empty) {
      logger.info(`? Phase ${name}: no more users (page=${page})`);
      break;
    }

    page += 1;
    usersTotal += snap.size;
    logger.info(`? Phase ${name}: page=${page} batchSize=${snap.size}`);

    await Promise.all(
      snap.docs.map((u, idx) =>
        limit(async () => {
          const start = Date.now();
          logger.debug(`  ▶ ${name} user#${idx + 1}/${snap.size} docId=${u.id} uid=${u.get("uid")}`);
          try {
            await perUser(u);
          } catch (e) {
            logger.error(`  ❌ ${name} uid=${u.get("uid")} docId=${u.id} error=${e.message}`, e);
          } finally {
            const ms = Date.now() - start;
            logger.debug(`  ◀ ${name} uid=${u.get("uid")} elapsedMs=${ms}`);
          }
        })
      )
    );

    last = snap.docs[snap.docs.length - 1];
  }

  logger.info(`???? Phase DONE: ${name} usersSeen=${usersTotal} ????`);
}

exports.nightlyRoiAndTeam = onSchedule(
  {
    schedule: "0 0 * * *", // 00:00 PKT daily
    timeZone: "Asia/Karachi",
    timeoutSeconds: 540,
    memory: "1GiB",
    retryConfig: { retryCount: 3, minBackoffSeconds: 120 },
  },
  async () => {
    const jobStart = new Date();
    logger.info(
      `???????? Nightly job START at ${jobStart.toISOString()} (PK-date=${ymdPK(jobStart)}) ????????`
    );

    try {
      await runPhase("ROI", creditRoiForUser, 50); // Phase A
    } catch (e) {
      logger.error(`❌ Phase ROI crashed: ${e.message}`, e);
    }

    try {
      await runPhase("Team Profit", creditTeamForUser, 30); // Phase B
    } catch (e) {
      logger.error(`❌ Phase Team Profit crashed: ${e.message}`, e);
    }

    const jobEnd = new Date();
    logger.info(
      `???????? Nightly job DONE at ${jobEnd.toISOString()} elapsedMs=${jobEnd - jobStart} ?????????`
    );
  }
);
