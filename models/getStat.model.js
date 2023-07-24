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
    let data = -1;

    if (statName === "Leads") {
      data = await getLeadStats(startDate, endDate, orgId);
    } else if (statName === "Dials") {
      data = await getDialStats(startDate, endDate, orgId);
    } else if (statName === "1-Minute Conversations") {
      data = await getOneMinConversionStats(startDate, endDate, orgId);
    } else if (statName === "Sets") {
      data = await getSetsStats(startDate, endDate, orgId);
    } else if (statName === "DQ's") {
      data = await getDQStats(startDate, endDate, orgId);
    } else if (statName === "Closes") {
      data = await getClosesStats(startDate, endDate, orgId);
    } else if (statName === "PiF's") {
      data = await getPiFStats(startDate, endDate, orgId);
    } else if (statName === "Pay Plan") {
      data = await getPayPlanStats(startDate, endDate, orgId);
    } else if (statName === "Cash") {
      data = await getCashStats(startDate, endDate, orgId);
    } else {
      data = -1;
    }

    return data[0];
  } catch (err) {
    console.error("Failed to retrieve stats:", err);
    throw err;
  }
}
