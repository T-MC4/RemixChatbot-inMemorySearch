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
      data: leadsStats,
    });

    // Stat: Dials
    const dialsStats = await getDialStats(startDate, endDate, orgId);
    stats.push({
      name: "Dials",
      data: dialsStats,
    });

    // Stat: 1-Min Convo
    const convoStats = await getOneMinConversionStats(
      startDate,
      endDate,
      orgId
    );
    stats.push({
      name: "1-Minute Conversations",
      data: convoStats,
    });

    // Stat: Sets
    const setsStats = await getSetsStats(startDate, endDate, orgId);
    stats.push({
      name: "Sets",
      data: setsStats,
    });

    // Stat: DQ's
    const dqStats = await getDQStats(startDate, endDate, orgId);
    stats.push({
      name: "DQ's",
      data: dqStats,
    });

    // Stat: Closes
    const closesStats = await getClosesStats(startDate, endDate, orgId);
    stats.push({
      name: "Closes",
      data: closesStats,
    });

    // Stat: PiF's
    const piFStats = await getPiFStats(startDate, endDate, orgId);
    stats.push({
      name: "PiF's",
      data: piFStats,
    });

    // Stat: Pay Plan
    const payPlanStats = await getPayPlanStats(startDate, endDate, orgId);
    stats.push({
      name: "Pay Plan",
      data: payPlanStats,
    });

    // Stat: Cash
    const cashStats = await getCashStats(startDate, endDate, orgId);
    stats.push({
      name: "Cash",
      data: cashStats,
    });

    return stats;
  } catch (err) {
    console.error("Failed to retrieve stats:", err);
    throw err;
  }
}

export async function getStatsByName(startDate, endDate, orgId, statName) {
  try {
    switch (statName) {
      case "Leads":
        return await getLeadsStats(startDate, endDate, orgId)[0];
      case "Dials":
        return await getDialsStats(startDate, endDate, orgId)[0];
      case "1-Minute Conversations":
        return await getConvoStats(startDate, endDate, orgId)[0];
      case "Sets":
        return await getSetsStats(startDate, endDate, orgId)[0];
      case "DQ's":
        return await getDQStats(startDate, endDate, orgId)[0];
      case "Closes":
        return await getClosesStats(startDate, endDate, orgId)[0];
      case "PiF's":
        return await getPiFStats(startDate, endDate, orgId)[0];
      case "Pay Plan":
        return await getPayPlanStats(startDate, endDate, orgId)[0];
      case "Cash":
        return await getCashStats(startDate, endDate, orgId)[0];
      default:
        return -1;
    }
  } catch (err) {
    console.error("Failed to retrieve stats:", err);
    throw err;
  }
}

async function getStatData(statName, statPromise) {
  const { date, count, totalCash } = await statPromise;
  const data = [];

  if (count !== undefined) {
    data.push({ date, value: count });
  }

  if (totalCash !== undefined) {
    data.push({ date, value: totalCash });
  }

  return {
    name: statName,
    data,
  };
}
