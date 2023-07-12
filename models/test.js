import { getStates, initState } from "./stat.model.js";

(async () => {
  const startDate = "2023-01-01";
  const endDate = "2023-01-01";
  const orgId = "1";
  await initState(orgId);
  const states = await getStates(startDate, endDate, orgId);
  console.log(states.length);
})();
