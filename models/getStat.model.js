import {
  getDQStats,
  getDialStats,
  getLeadStats,
  getOneMinConversionStats,
  getSetsStats,
  getMaxStats,
} from "./max.model.js";

import {
  getClosesStats,
  getPayPlanStats,
  getPiFStats,
  getCashStats,
  getMidasStats,
} from "./midas.model.js";

export async function getStats(startDate, endDate, orgId) {
  try {
    const maxStats = await getMaxStats(startDate, endDate, orgId);
    const midasStats = await getMidasStats(startDate, endDate, orgId);

    return [...maxStats, ...midasStats];
  } catch (err) {
    console.error("Failed to retrieve stats:", err);
    throw err;
  }
}

export async function getStatsByName(startDate, endDate, orgId, statName) {
  try {
    switch (statName) {
      case "Leads":
        return await getLeadStats(startDate, endDate, orgId)[0];
      case "Dials":
        return await getDialStats(startDate, endDate, orgId)[0];
      case "1-Minute Conversations":
        return await getOneMinConversionStats(startDate, endDate, orgId)[0];
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