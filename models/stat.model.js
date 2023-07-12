import {
  connectToSherlockSnowflake,
  consumeStream,
} from "../utils/snowflakeUtils.js";
import { v4 as uuidv4 } from "uuid";
import { getStats, getStatsByName } from "./getStat.model.js";

const fixedStats = [
  "Leads",
  "Dials",
  "1-Minute Conversations",
  "Sets",
  "DQ's",
  "Closes",
  "PiF's",
  "Pay Plan",
  "Cash",
  "Contacted Leads",
  "Responses",
  "Appts Set",
  "CPM",
  "CTR",
  "Opt-in Rate",
  "Ad Spend",
  "Shows",
];

export async function initState(orgId) {
  try {
    const conn = await connectToSherlockSnowflake();
    const existingRecord = await conn.execute({
      sqlText: `
      SELECT 
        title
      FROM
        Stats
      WHERE 
        orgId = ${orgId}
      `,
    });
    const rows = await consumeStream(existingRecord.streamRows());

    if (rows.length < 17) {
      for (const stat of fixedStats) {
        await conn.execute({
          sqlText: `
          -- Query to create a stat item
          INSERT INTO Stats (statId, orgId, title)
          VALUES (?, ?, ?)`,
          binds: [uuidv4(), orgId, stat],
        });
      }
      console.log("Stats initialized successfully.");
    }
  } catch (error) {
    console.error(
      "Failed to execute statement due to the following error: " + error.message
    );
    throw error;
  }
}

export async function getStateId(orgId) {
  try {
    const conn = await connectToSherlockSnowflake();
    const existingRecord = await conn.execute({
      sqlText: `
      SELECT 
        title,
        statId
      FROM
        Stats
      WHERE 
        orgId = ${orgId}
      `,
    });
    const rows = await consumeStream(existingRecord.streamRows());

    const stateIds = {};

    for (const row of rows) {
      const title = row["TITLE"];
      const statId = row["STATID"];
      stateIds[title] = statId;
    }

    return stateIds;
  } catch (error) {
    console.error(
      "Failed to execute statement due to the following error: " + error.message
    );
    throw error;
  }
}

export async function getStateValues(startDate, endDate, orgId) {
  try {
    const conn = await connectToSherlockSnowflake();

    // Create a date range table using a recursive CTE
    const dateRangeTableStatement = conn.execute({
      sqlText: `
      WITH date_range_table (date_rec) AS (
        SELECT
          to_date('${startDate}') -- Start date
        UNION ALL
        SELECT
          to_date(dateadd(day, 1, date_rec))
        FROM
          date_range_table
        WHERE
          date_rec < '${endDate}' -- End date
      )
      SELECT
        date_rec AS "date",
        COALESCE(state.value, 0) AS "statValue",
        COALESCE(state.title, 'No Title') AS "title"
      FROM
        date_range_table
      LEFT JOIN (
        SELECT
          s.title,
          COALESCE(sv.indata, sub.date_rec) AS indata,
          COALESCE(sv.value, 0) AS value,
          s.orgid
        FROM
          Stats s 
        LEFT JOIN
          (SELECT date_rec FROM date_range_table) sub ON 1=1
        LEFT JOIN
          StatValues sv ON sv.statId = s.statId 
      ) state
        ON to_date(state.inData) = date_rec
      WHERE
        state.orgId = '${orgId}'
      ORDER BY
        "date" ASC;
      
      `,
    });

    const stateValues = {};
    const rows = await consumeStream(dateRangeTableStatement.streamRows());
    for (const row of rows) {
      const date = row["date"];
      const statValue = row["statValue"] || 0;
      const title = row["title"];
      if (title !== null) {
        if (!stateValues[title]) {
          stateValues[title] = [];
        }

        stateValues[title].push({ date, value: statValue });
      }
    }

    const stats = Object.entries(stateValues).map(([title, data]) => ({
      name: title,
      data,
    }));

    console.log("Get DB State Values:");

    return stats;
  } catch (err) {
    console.error(
      "Failed to execute statement due to the following error: " + err.message
    );
    throw err;
  }
}

export async function getStates(startDate, endDate, orgId) {
  try {
    const stats = await getStats(startDate, endDate, orgId);
    const stateValues = await getStateValues(startDate, endDate, orgId);
    const stateIds = await getStateId(orgId);
    const combinedData = {};

    // Merge stats data
    for (const stat of stats) {
      const { name, data } = stat;
      combinedData[name] = data;
    }

    // Merge state values data
    for (const stateValue of stateValues) {
      const { name, data } = stateValue;
      if (!combinedData[name]) {
        combinedData[name] = [];
      }

      for (const entry of data) {
        const { date, value } = entry;
        const existingEntry = combinedData[name].find((item) => {
          return (
            item.date.getFormat("yyyy-mm-dd") === date.getFormat("yyyy-mm-dd")
          );
        });
        if (existingEntry) {
          existingEntry.value += value;
        } else {
          combinedData[name].push({ date, value });
        }
      }
    }

    const result = Object.entries(combinedData).map(([name, data]) => ({
      name,
      data,
      id: stateIds[name],
    }));

    // console.log("Combined Result:", result);

    return result;
  } catch (err) {
    console.error("Failed to retrieve states:", err);
    throw err;
  }
}

export async function createStatItem(orgId, title) {
  try {
    initState(orgId);

    const conn = await connectToSherlockSnowflake();
    const stateId = uuidv4();

    const statement = conn.execute({
      sqlText: `
      -- Query to create a stat item
      INSERT INTO Stats (statId, orgId, title)
      VALUES (?, ?, ?, ?, ?)`,
      binds: [stateId, orgId, title],
    });
    console.log("Stat item created successfully.");
    return stateId;
  } catch (error) {
    console.error(
      "Failed to execute statement due to the following error: " + err.message
    );
    throw err;
  }
}

export async function updateStatItemName(orgId, statId, title) {
  try {
    const conn = await connectToSherlockSnowflake();
    const statement = conn.execute({
      sqlText: `
      -- Query to update stat item name
      UPDATE Stats 
      SET title = ${title}
      WHERE orgId = ${orgId} AND statId = ${statId}`,
    });
    console.log("Stat item name updated successfully.");
  } catch (error) {
    console.error(
      "Failed to execute statement due to the following error: " + err.message
    );
    throw err;
  }
}
export async function getFinalUpdateValue(statId, value, date) {
  const conn = await connectToSherlockSnowflake();
  const statTitle = await conn.execute({
    sqlText: `
    SELECT 
      title
    FROM 
      Stats
    WHERE 
      statId = ${statId}
    `,
  });
  const title = statTitle[0]["TITLE"];
  const data = getStatsByName(date, date, orgId, title);
  if (data === -1) {
    return value;
  } else {
    return value - data[0]["value"];
  }
}

export async function updateStatItemValue(statId, value, date) {
  try {
    const conn = await connectToSherlockSnowflake();
    const existingRecord = await conn.execute({
      sqlText: `
      SELECT stateValueId
      FROM StatValues
      WHERE statId = ${statId}
        AND inDate = ${date}
      `,
    });

    const rows = await consumeStream(existingRecord.streamRows());
    const newValue = await getFinalUpdateValue(statId, value, date);

    if (rows.length > 0) {
      // Update the existing record
      const stateValueId = rows[0]["STATEVALUEID"];
      const statement = conn.execute({
        sqlText: `
        UPDATE StatValues
        SET value = ${newValue}
        WHERE stateValueId = ${stateValueId}
        `,
      });
      console.log("Stat item value updated successfully.");
    } else {
      // Insert a new record
      const stateValueId = uuidv4();
      const statement = conn.execute({
        sqlText: `
        INSERT INTO StatValues (stateValueId, statId, value, inDate) 
        VALUES (?, ?, ?, ?)
        `,
        binds: [stateValueId, statId, newValue, date],
      });
      console.log("New stat item value inserted successfully.");
    }
  } catch (error) {
    console.error(
      "Failed to execute statement due to the following error: " + error.message
    );
    throw error;
  }
}

export async function deleteStatItems(orgId, statId) {
  try {
    const conn = await connectToSherlockSnowflake();
    const statement = conn.execute({
      sqlText: `
      -- Query to delete stat items
      DELETE FROM Stats
      WHERE orgId = ${orgId} AND statId = ${statId}`,
    });
    console.log("Stat items deleted successfully.");
  } catch (error) {
    console.error(
      "Failed to execute statement due to the following error: " + err.message
    );
    throw err;
  }
}

export async function createStatsTable() {
  try {
    const conn = await connectToSherlockSnowflake();
    const statement = conn.execute({
      sqlText: `
            CREATE TABLE IF NOT EXISTS Stats (
              statId VARCHAR(255) PRIMARY KEY,
              orgId VARCHAR(255),
              title TEXT
            )
          `,
    });

    console.log("Stats table created successfully.");
  } catch (err) {
    console.error(
      "Failed to execute statement due to the following error: " + err.message
    );
    throw err;
  }
}

export async function createStatValuesTable() {
  try {
    const conn = await connectToSherlockSnowflake();
    const statement = conn.execute({
      sqlText: `
              CREATE TABLE IF NOT EXISTS StatValues (
                statValueId VARCHAR(255) PRIMARY KEY,
                statId VARCHAR(255),
                value NUMBER,
                inData DATE
              )
            `,
    });

    console.log("StatValues table created successfully.");
  } catch (err) {
    console.error(
      "Failed to execute statement due to the following error: " + err.message
    );
    throw err;
  }
}
