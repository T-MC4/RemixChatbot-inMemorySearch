import {
  getDQStats,
  getDialStats,
  getLeadStats,
  getOneMinConversionStats,
  getSetsStats,
} from "./max.model.js";

import {
  getClosesStats,
  getPayPlanStats,
  getPiFStats,
  getCashStats,
} from "./midas.model.js";

export async function getStats(startDate, endDate, orgId) {
  try {
    const stats = [];

    // Stat: Leads
    const leadsStats = await getLeadStats(startDate, endDate, orgId);
    stats.push({
      name: "Leads",
      data: leadsStats.map(({ date, count }) => ({
        date,
        value: count,
      })),
    });

    // Stat: Dials
    const dialsStats = await getDialStats(startDate, endDate, orgId);
    stats.push({
      name: "Dials",
      data: dialsStats.map(({ date, count }) => ({
        date,
        value: count,
      })),
    });

    // Stat: 1-Min Convo
    const convoStats = await getOneMinConversionStats(
      startDate,
      endDate,
      orgId
    );
    stats.push({
      name: "1-Minute Conversations",
      data: convoStats.map(({ date, count }) => ({
        date,
        value: count,
      })),
    });

    // Stat: Sets
    const setsStats = await getSetsStats(startDate, endDate, orgId);
    stats.push({
      name: "Sets",
      data: setsStats.map(({ date, count }) => ({
        date,
        value: count,
      })),
    });

    // Stat: DQ's
    const dqStats = await getDQStats(startDate, endDate, orgId);
    stats.push({
      name: "DQ's",
      data: dqStats.map(({ date, count }) => ({
        date,
        value: count,
      })),
    });

    // Stat: Closes
    const closesStats = await getClosesStats(startDate, endDate, orgId);
    stats.push({
      name: "Closes",
      data: closesStats.map(({ date, count }) => ({
        date,
        value: count,
      })),
    });

    // Stat: PiF's
    const piFStats = await getPiFStats(startDate, endDate, orgId);
    stats.push({
      name: "PiF's",
      data: piFStats.map(({ date, count }) => ({
        date,
        value: count,
      })),
    });

    // Stat: Pay Plan
    const payPlanStats = await getPayPlanStats(startDate, endDate, orgId);
    stats.push({
      name: "Pay Plan",
      data: payPlanStats.map(({ date, count }) => ({
        date,
        value: count,
      })),
    });

    // Stat: Cash
    const cashStats = await getCashStats(startDate, endDate, orgId);
    stats.push({
      name: "Cash",
      data: cashStats.map(({ date, totalCash }) => ({
        date,
        value: totalCash,
      })),
    });

    return stats;
  } catch (err) {
    console.error("Failed to retrieve stats:", err);
    throw err;
  }
}
