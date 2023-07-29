import {
  connectToSherlockSnowflake,
  consumeStream,
} from "../utils/snowflakeUtils.js";
import { v4 as uuidv4 } from "uuid";
import { getStats, getStatsByName } from "./getStat.model.js";

const fixedStats = [
  {
    title: "Leads",
    category: "Paid Ads",
    formatter: "fixedDecimalPointsFormatter",
  },
  {
    title: "Dials",
    category: "Appointment Setting",
    formatter: "fixedDecimalPointsFormatter",
  },
  {
    title: "1-Minute Conversations",
    category: "Appointment Setting",
    formatter: "fixedDecimalPointsFormatter",
  },
  {
    title: "Sets",
    category: "Appointment Setting",
    formatter: "fixedDecimalPointsFormatter",
  },
  {
    title: "DQ's",
    category: "Appointment Setting",
    formatter: "fixedDecimalPointsFormatter",
  },
  {
    title: "Closes",
    category: "Closing",
    formatter: "fixedDecimalPointsFormatter",
  },
  {
    title: "PiF's",
    category: "Closing",
    formatter: "fixedDecimalPointsFormatter",
  },
  {
    title: "Pay Plan",
    category: "Closing",
    formatter: "fixedDecimalPointsFormatter",
  },
  { title: "Cash", category: "Closing", formatter: "dollarFormatter" },
  {
    title: "Contacted Leads",
    category: "Organic Marketing",
    formatter: "fixedDecimalPointsFormatter",
  },
  {
    title: "Responses",
    category: "Organic Marketing",
    formatter: "fixedDecimalPointsFormatter",
  },
  {
    title: "Appts Set",
    category: "Organic Marketing",
    formatter: "fixedDecimalPointsFormatter",
  },
  { title: "CPM", category: "Paid Ads", formatter: "dollarFormatter" },
  { title: "CTR", category: "Paid Ads", formatter: "percentageFormatter" },
  {
    title: "Opt-in Rate",
    category: "Paid Ads",
    formatter: "fixedDecimalPointsFormatter",
  },
  { title: "Ad Spend", category: "Paid Ads", formatter: "dollarFormatter" },
  {
    title: "Shows",
    category: "Closing",
    formatter: "fixedDecimalPointsFormatter",
  },
];

export async function initState(orgId) {
  try {
    const conn = await connectToSherlockSnowflake();
    const existingRecord = conn.execute({
      sqlText: `
      SELECT 
        title
      FROM
        Stats
      WHERE 
        orgId = '${orgId}'
      `,
    });
    const rows = await consumeStream(existingRecord.streamRows());

    if (rows.length < 17) {
      let query = `
      -- Query to create a stat item
      INSERT INTO Stats (statId, orgId, title, category, formatter, isFixed)
      VALUES `;
      const binds = [];
      const stats = {};
      for (const stat of fixedStats) {
        query += `(?, ?, ?, ?, ?, ?),`;
        const id = uuidv4();
        binds.push(
          id,
          orgId,
          stat["title"],
          stat["category"],
          stat["formatter"],
          true
        );
        stats[stat["title"]] = {
          statId: id,
          category: stat["category"],
          formatter: stat["formatter"],
          isFixed: true,
        };
      }
      query = query.slice(0, -1);
      const statement = conn.execute({
        sqlText: query,
        binds,
      });
      await consumeStream(statement.streamRows());
      console.log("Stats initialized successfully.");
      return stats;
    }
    return null;
  } catch (error) {
    console.error(
      "Failed to execute statement due to the following error: " + error.message
    );
    throw error;
  }
}

export async function getStatsId(orgId) {
  try {
    const stats = await initState(orgId);
    if (stats) {
      return stats;
    }
    const conn = await connectToSherlockSnowflake();
    const existingRecord = conn.execute({
      sqlText: `
      SELECT 
        title,
        statId,
        category,
        formatter,
        isFixed,
        createdAt
      FROM
        Stats
      WHERE 
        orgId = '${orgId}'
      ORDER BY
        createdAt ASC;
      `,
    });
    const rows = await consumeStream(existingRecord.streamRows());
    console.log();
    const statIds = {};

    for (const row of rows) {
      const title = row["TITLE"];
      const statId = row["STATID"];
      const category = row["CATEGORY"];
      const formatter = row["FORMATTER"];
      const isFixed = row["ISFIXED"];
      statIds[title] = {
        statId,
        category,
        formatter,
        isFixed,
      };
    }

    return statIds;
  } catch (error) {
    console.error(
      "Failed to execute statement due to the following error: " + error.message
    );
    throw error;
  }
}

function addValuesByDate(data) {
  let dateWiseSum = {};

  for (let entry of data) {
    let abs_date = entry.abs_date;
    let date = entry.date;
    let value = entry.value;

    if (date in dateWiseSum) {
      dateWiseSum[abs_date] = {
        value: dateWiseSum[abs_date].value + value,
        date,
      };
    } else {
      dateWiseSum[abs_date] = {
        value,
        date,
      };
    }
  }

  // Convert the dateWiseSum object back to an array of objects
  let result = Object.keys(dateWiseSum).map((date) => ({
    date: dateWiseSum[date].date,
    value: dateWiseSum[date].value,
  }));

  return result;
}

export async function getStateValues(startDate, endDate, orgId) {
  try {
    await initState(orgId);
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
        to_char(to_date(date_rec), '%b %d') AS "date",
        to_date(date_rec) AS "abs_date",
        COALESCE(state.value, 0) AS "statValue",
        COALESCE(state.title, 'No Title') AS "title"
      FROM
        date_range_table
      LEFT JOIN (
        SELECT
          s.title,
          COALESCE(sv.indate, sub.date_rec) AS indate,
          COALESCE(sv.value, 0) AS value,
          s.orgid
        FROM
          Stats s 
        LEFT JOIN
          (SELECT date_rec FROM date_range_table) sub ON 1=1
        LEFT JOIN
          StatValues sv ON sv.statId = s.statId AND sub.date_rec = sv.indate
      ) state
        ON to_date(state.inDate) = date_rec
      WHERE
        state.orgId = '${orgId}'
      ORDER BY
        "abs_date" ASC;
      `,
    });

    const stateValues = {};
    const rows = await consumeStream(dateRangeTableStatement.streamRows());
    for (const row of rows) {
      const date = row["date"];
      const statValue = row["statValue"] || 0;
      const title = row["title"];
      const abs_date = row["abs_date"];
      if (title !== null) {
        if (!stateValues[title]) {
          stateValues[title] = [];
        }

        stateValues[title].push({ date, value: statValue, abs_date });
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
    await initState(orgId);
    const stats = await getStats(startDate, endDate, orgId);
    const stateValues = await getStateValues(startDate, endDate, orgId);
    const statIds = await getStatsId(orgId);

    const combinedData = {};

    // Merge stats data
    for (const stat of stateValues) {
      const { name, data } = stat;
      combinedData[name] = data;
    }

    // Merge state values data
    for (const stateValue of stats) {
      const { name, data } = stateValue;
      if (!combinedData[name]) {
        combinedData[name] = data;
        continue;
      }

      for (const entry of data) {
        const { date, value, abs_date } = entry;
        const existingEntry = combinedData[name].find((item) => {
          return (
            item.abs_date.getFormat("yyyy-mm-dd") ===
            abs_date.getFormat("yyyy-mm--dd")
          );
        });
        if (existingEntry) {
          existingEntry.value += value;
        } else {
          combinedData[name].push({ date, value, abs_date });
        }
      }
    }

    const result = Object.entries(statIds).map(([name, statData]) => ({
      name,
      data: addValuesByDate(combinedData[name]).map(({ date, value }) => ({
        date,
        [name]: value,
      })),
      sum: combinedData[name].reduce((acc, { value = 0 }) => acc + value, 0),
      id: statData["statId"],
      category: statData["category"],
      formatter: statData["formatter"],
      isFixed: statData["isFixed"],
    }));

    return result;
  } catch (err) {
    console.error("Failed to retrieve states:", err);
    throw err;
  }
}

export async function createStatItem(orgId, title, category, formatter) {
  try {
    await initState(orgId);

    const conn = await connectToSherlockSnowflake();
    const statId = uuidv4();

    const statement = conn.execute({
      sqlText: `
      -- Query to create a stat item
      INSERT INTO Stats (statId, orgId, title, category, formatter, isFixed)
      VALUES (?, ?, ?, ?, ?, ?)`,
      binds: [statId, orgId, title, category, formatter, false],
    });
    await consumeStream(statement.streamRows());

    console.log("Stat item created successfully.");
    return statId;
  } catch (error) {
    console.error(
      "Failed to execute statement due to the following error: " + err.message
    );
    throw err;
  }
}

export async function updateStatItemName(orgId, statId, title) {
  try {
    const Ids = await getStatsId(orgId);
    const fixedStatIds = Object.values(Ids).filter((stat) => stat.isFixed);
    const fixedStatIdsArray = fixedStatIds.map((stat) => stat.statId);
    const isFixed = fixedStatIdsArray.includes(statId);

    if (isFixed) {
      return false;
    }

    const conn = await connectToSherlockSnowflake();
    const statement = conn.execute({
      sqlText: `
      -- Query to update stat item name
      UPDATE Stats 
      SET title = '${title}'
      WHERE statId = '${statId}' AND orgId = '${orgId}'`,
    });
    await consumeStream(statement.streamRows());

    console.log("Stat item name updated successfully.");
    return true;
  } catch (error) {
    console.error(
      "Failed to execute statement due to the following error: " + err.message
    );
    throw err;
  }
}

export async function updateStatItemFormatter(orgId, statId, formatter) {
  try {
    const conn = await connectToSherlockSnowflake();
    const statement = conn.execute({
      sqlText: `
      -- Query to update stat item name
      UPDATE Stats 
      SET formatter = '${formatter}'
      WHERE statId = '${statId}' AND orgId = '${orgId}'`,
    });
    await consumeStream(statement.streamRows());

    console.log("Stat item formatter updated successfully.");
  } catch (error) {
    console.error(
      "Failed to execute statement due to the following error: " + err.message
    );
    throw err;
  }
}

export async function getFinalUpdateValue(orgId, statId, value, date) {
  const conn = await connectToSherlockSnowflake();
  const statTitle = conn.execute({
    sqlText: `
    SELECT 
      title
    FROM 
      Stats
    WHERE 
      statId = '${statId}'
    `,
  });
  const rows = await consumeStream(statTitle.streamRows());

  const title = rows[0]["TITLE"];
  const data = await getStatsByName(date, date, orgId, title);
  if (data === -1) {
    return value;
  } else {
    return value - data["value"];
  }
}

export async function updateStatItemValue(orgId, statId, value, date) {
  try {
    const conn = await connectToSherlockSnowflake();
    const existingRecord = conn.execute({
      sqlText: `
      SELECT statValueId
      FROM StatValues
      WHERE statId = '${statId}'
        AND inDate = '${date}'
      `,
    });

    const rows = await consumeStream(existingRecord.streamRows());
    const newValue = await getFinalUpdateValue(orgId, statId, value, date);

    if (rows.length > 0) {
      // Update the existing record
      const statValueId = rows[0]["STATVALUEID"];
      const statement = conn.execute({
        sqlText: `
        UPDATE StatValues
        SET value = ${newValue}
        WHERE statValueId = '${statValueId}'
        `,
      });
      console.log("Stat item value updated successfully.");
    } else {
      // Insert a new record
      const statValueId = uuidv4();
      const statement = conn.execute({
        sqlText: `
        INSERT INTO StatValues (statValueId, statId, value, inDate) 
        VALUES (?, ?, ?, ?)
        `,
        binds: [statValueId, statId, newValue, date],
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

export async function deleteStatItems(orgId, deleteStatIds) {
  try {
    const Ids = await getStatsId(orgId);
    const fixedStatIds = Object.values(Ids).filter((stat) => stat.isFixed);
    const fixedStatIdsArray = fixedStatIds.map((stat) => stat.statId);
    const isFixed = deleteStatIds.some((stat) =>
      fixedStatIdsArray.includes(stat)
    );

    if (isFixed) {
      return false;
    }

    const conn = await connectToSherlockSnowflake();

    const placeholders = deleteStatIds.map(() => "?").join(",");

    const statDeleteQuery = `
      -- Query to delete stat items
      DELETE FROM Stats
      WHERE orgId = ? AND statId IN (${placeholders})`;
    const statValueDeleteQuery = `
      -- Query to delete stat items
      DELETE FROM StatValues
      WHERE statId IN (${placeholders})`;

    const values = [orgId, ...deleteStatIds];

    const statDeleteStatement = conn.execute({
      sqlText: statDeleteQuery,
      binds: values,
    });
    await consumeStream(statDeleteStatement.streamRows());
    
    const statValueDeleteStatement = conn.execute({
      sqlText: statValueDeleteQuery,
      binds: values,
    });
    await consumeStream(statValueDeleteStatement.streamRows());

    console.log("Stat items deleted successfully.");
    return true;
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
              title TEXT,
              category TEXT,
              formatter TEXT,
              isFixed BOOLEAN,
              createdAt DATETIME DEFAULT CURRENT_TIMESTAMP
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
                value REAL,
                inDate DATE
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
