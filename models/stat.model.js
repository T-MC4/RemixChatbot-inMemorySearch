import { connectToSnowflake, consumeStream } from "../utils/snowflakeUtils.js";
import { v4 as uuidv4 } from "uuid";
import { getStats } from "./getStat.model.js";

export async function initState(orgId) {
  try {
    const conn = await connectToSnowflake();

    const stats = [
      { title: "Leads" },
      { title: "Dials" },
      { title: "1-Minute Conversation" },
      { title: "Sets" },
      { title: "DQ's" },
      { title: "Closes" },
      { title: "PiF's" },
      { title: "Pay Plan" },
      { title: "Cash" },
    ];

    for (const stat of stats) {
      const { title } = stat;
      await conn.execute({
        sqlText: `
          -- Query to create a stat item
          INSERT INTO Stats (statId, orgId, title)
          VALUES (?, ?, ?)`,
        binds: [uuidv4(), orgId, title],
      });
    }

    console.log("Stats initialized successfully.");
  } catch (error) {
    console.error(
      "Failed to execute statement due to the following error: " + error.message
    );
    throw error;
  }
}

export async function getStateValues(startDate, endDate, orgId) {
  try {
    const conn = await connectToSnowflake();

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
          sv.value AS "statValue",
          s.title AS "title"
        FROM
          date_range_table
        LEFT JOIN
          StatValues sv ON to_date(sv.inData) = date_rec
        LEFT JOIN
          Stats s ON sv.statId = s.statId
        WHERE
          s.orgId = '${orgId}'
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

      if (!stateValues[title]) {
        stateValues[title] = [];
      }

      stateValues[title].push({ date, value: statValue });
    }

    const stats = Object.entries(stateValues).map(([title, data]) => ({
      name: title,
      data,
    }));

    console.log("State Values:", stats);

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
        const existingEntry = combinedData[name].find(
          (item) => item.date === date
        );
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
    }));

    console.log("Combined Result:", result);

    return result;
  } catch (err) {
    console.error("Failed to retrieve states:", err);
    throw err;
  }
}

export async function createStatItem(orgId, title) {
  try {
    const conn = await connectToSnowflake();
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
    const conn = await connectToSnowflake();
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

export async function updateStatItemValue(statId, value, date) {
  try {
    const conn = await connectToSnowflake();
    const existingRecord = await conn.execute({
      sqlText: `
      SELECT stateValueId
      FROM StatValues
      WHERE statId = ${statId}
        AND inDate = ${date}
      `,
    });
    const rows = await consumeStream(existingRecord.streamRows());

    if (rows.length > 0) {
      // Update the existing record
      const stateValueId = rows[0]["STATEVALUEID"];
      const statement = conn.execute({
        sqlText: `
        UPDATE StatValues
        SET value = ${value}
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
        binds: [stateValueId, statId, value, date],
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
    const conn = await connectToSnowflake();
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
    const conn = await connectToSnowflake();
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
    const conn = await connectToSnowflake();
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
