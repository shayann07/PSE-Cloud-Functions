/**
 * computeTeamLevelsAndCreditProfit – UI-only version
 *
 * Input : { userId: "<root-uid>" }
 * Output: {
 *   levels          : Array<LevelStats & { users: [...] }>,
 *   profitBooked    : false,
 *   creditedAmount  : 0,
 *   directBusiness  : number,   // sum of investment.totalDeposit for level-1 users
 *   selfDeposit     : number    // investment.totalDeposit for rootUid
 * }
 *
 * Notes
 * ─────
 * • Still read-only (no balance updates, transactions, or guard logs).
 * • Adds two extra queries at the end to fetch the new numbers.
 */

const { onCall, HttpsError } = require("firebase-functions/v2/https");
const { logger } = require("firebase-functions");
const { initializeApp, getApps } = require("firebase-admin/app");
const { getFirestore } = require("firebase-admin/firestore");

if (!getApps().length) initializeApp();
const db = getFirestore();

/* -------- helpers -------- */
function chunk10(arr) {
  const out = [];
  for (let i = 0; i < arr.length; i += 10) out.push(arr.slice(i, i + 10));
  return out;
}

/**
 * computeTeamLevelsAndCreditProfit
 *
 * Unlocking rule (NEW):
 *   - A level is unlocked if (# of active Level-1 directs) >= teamSettings.requiredMembers
 *
 * Everything else (stats per depth, deposits, etc.) stays the same.
 */
exports.computeTeamLevelsAndCreditProfit = onCall(
  { region: "us-central1", timeoutSeconds: 60, memory: "512MiB" },
  async (req) => {
    const rootUid = req.data && req.data.userId;
    if (!rootUid) throw new HttpsError("invalid-argument", "userId is required");

    // 1) Load level config
    const settingsSnap = await db.collection("teamSettings").orderBy("level").get();
    const settings = settingsSnap.docs.map((d) => d.data());

    // 2) We’ll traverse by depth to build the UI stats…
    //    but unlocking for *all* levels will depend on Level-1 active directs only.
    let frontier = [rootUid];
    const levels = [];

    // to store the Level-1 active direct count once we compute it
    let directActiveCount = null;

    for (const cfg of settings) {
      // Fetch users who were directly referred by *any* uid in the current frontier
      const userDocs = [];
      for (const chunk of chunk10(frontier)) {
        const qs = await db.collection("users").where("referralCode", "in", chunk).get();
        userDocs.push(...qs.docs);
      }

      // Build a lightweight list for the UI
      const userList = userDocs.map((d) => ({
        uid: d.get("uid"),
        firstName: d.get("firstName") || "",
        lastName: d.get("lastName") || "",
        status: d.get("status"),
      }));

      const activeUids = userList.filter((u) => u.status === "active").map((u) => u.uid);

      // Sum deposits + daily profit for *this depth* (unchanged behavior)
      let levelDeposit = 0;
      let levelDailyProfit = 0;
      if (activeUids.length) {
        for (const chunk of chunk10(activeUids)) {
          const accSnap = await db.collection("accounts").where("userId", "in", chunk).get();
          accSnap.forEach((acc) => {
            const inv = acc.get("investment") || {};
            const earn = acc.get("earnings") || {};
            levelDeposit += inv.totalDeposit || 0;
            levelDailyProfit += earn.dailyProfit || 0;
          });
        }
      }

      // --- NEW: compute directActiveCount once (when we’re at Level 1) ---
      if (cfg.level === 1) {
        directActiveCount = activeUids.length; // number of active directs
      }

      // Unlock logic now uses Level-1 active directs for EVERY level.
      // (If settings aren’t strictly 1..N, this still works because we
      //  compute directActiveCount as soon as we hit whatever cfg has level===1.)
      const unlocked =
        directActiveCount !== null
          ? directActiveCount >= (cfg.requiredMembers || 0)
          : false; // if teamSettings had no level 1 row, play safe

      levels.push({
        level: cfg.level,
        requiredMembers: cfg.requiredMembers,
        profitPercentage: cfg.profitPercentage,
        totalUsers: userDocs.length,
        activeUsers: activeUids.length,
        inactiveUsers: userDocs.length - activeUids.length,
        totalDeposit: levelDeposit,
        levelUnlocked: unlocked,
        users: userList,
      });

      // Keep traversing depth-by-depth to produce correct stats
      frontier = activeUids;
    }

    // 3) Extra figures (unchanged)
    let directBusiness = 0;
    if (levels.length > 0) {
      const level1Uids = levels[0].users.map((u) => u.uid);
      for (const chunk of chunk10(level1Uids)) {
        const accSnap = await db.collection("accounts").where("userId", "in", chunk).get();
        accSnap.forEach((acc) => {
          const inv = acc.get("investment") || {};
          directBusiness += inv.totalDeposit || 0;
        });
      }
    }

    let selfDeposit = 0;
    const selfAccSnap = await db
      .collection("accounts")
      .where("userId", "==", rootUid)
      .limit(1)
      .get();
    if (!selfAccSnap.empty) {
      const inv = selfAccSnap.docs[0].get("investment") || {};
      selfDeposit = inv.totalDeposit || 0;
    }

    logger.info(
      `Levels for ${rootUid} – L1 active directs=${directActiveCount ?? 0}, ` +
        `directBusiness=${directBusiness}, selfDeposit=${selfDeposit}`
    );

    return {
      levels,
      profitBooked: false,
      creditedAmount: 0,
      directBusiness,
      selfDeposit,
    };
  }
);
