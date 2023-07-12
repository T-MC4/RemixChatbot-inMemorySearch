import { getStates } from "./stat.model.js";

(async () => {
  const startDate = "2023-01-01";
  const endDate = "2023-01-31";
  const orgId = "1";

  const states = await getStates(startDate, endDate, orgId);
  console.log(states);
})();
