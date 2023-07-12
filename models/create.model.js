import { createChatDataTable, createChatListTable } from "./chat.model.js";
import { createFiltersTable } from "./filter.model.js";
import { createStatsTable, createStatValuesTable } from "./stat.model.js";

(async () => {
  await createChatListTable();
  await createChatDataTable();
  await createFiltersTable();
  await createStatsTable();
  await createStatValuesTable();
})();
