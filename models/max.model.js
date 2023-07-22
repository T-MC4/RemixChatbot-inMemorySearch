import {
  connectToMaxSnowflake,
  consumeStream,
} from "../utils/snowflakeUtils.js";

export async function getLeadStats(startDate, endDate, orgId) {
  try {
    const conn = await connectToMaxSnowflake();

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
            to_date(date_rec) as "date",
            COALESCE(
              (
                SELECT
                  COUNT(*)
                FROM
                  LEAD
                WHERE
                  to_date(CREATED_AT) = to_date(date_rec)
                  AND ORG_ID = '${orgId}'
              ), 0) as "count"
          FROM
            date_range_table
          ORDER BY
            "date" ASC;
        `,
    });

    const leadStats = [];
    const rows = await consumeStream(dateRangeTableStatement.streamRows());
    for (const row of rows) {
      const date = row["date"];
      const value = row["count"];
      leadStats.push({ date, value });
    }

    console.log("Get Lead Stats:");

    return leadStats;
  } catch (err) {
    console.error(
      "Failed to execute statement due to the following error: " + err.message
    );
    throw err;
  }
}

export async function getDialStats(startDate, endDate, orgId) {
  try {
    const conn = await connectToMaxSnowflake();

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
            to_date(date_rec) as "date",
          COALESCE(
            (
              SELECT
                COUNT(*)
              FROM
                EVENT
              WHERE
                to_date(CREATED_AT) = to_date(date_rec)
                AND ORG_ID = '${orgId}'
                AND EVENT_TYPE = 'CALL_INITIATED'
            ), 0) as "count"
          FROM
            date_range_table
          ORDER BY
            "date" ASC;
        `,
    });

    const dialStats = [];
    const rows = await consumeStream(dateRangeTableStatement.streamRows());
    for (const row of rows) {
      const date = row["date"];
      const value = row["count"];
      dialStats.push({ date, value });
    }

    console.log("Get Dial Stats:");

    return dialStats;
  } catch (err) {
    console.error(
      "Failed to execute statement due to the following error: " + err.message
    );
    throw err;
  }
}

export async function getOneMinConversionStats(startDate, endDate, orgId) {
  try {
    const conn = await connectToMaxSnowflake();

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
            to_date(date_rec) as "date",
          COALESCE(
            (
              SELECT
                COUNT(*)
              FROM
                EVENT
              WHERE
                to_date(CREATED_AT) = to_date(date_rec)
                AND ORG_ID = '${orgId}'
                AND EVENT_TYPE = 'CONFERENCE_END'
                AND EVENT_DETAILS:leadConnectedTime = 60000
            ), 0) as "count"
          FROM
            date_range_table
          ORDER BY
            "date" ASC;
        `,
    });

    const oneMinConversionStats = [];
    const rows = await consumeStream(dateRangeTableStatement.streamRows());
    for (const row of rows) {
      const date = row["date"];
      const value = row["count"];
      oneMinConversionStats.push({ date, value });
    }

    console.log("Get 1-Min Conversion Stats:");

    return oneMinConversionStats;
  } catch (err) {
    console.error(
      "Failed to execute statement due to the following error: " + err.message
    );
    throw err;
  }
}

export async function getSetsStats(startDate, endDate, orgId) {
  try {
    const conn = await connectToMaxSnowflake();

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
            to_date(date_rec) AS "date",
          COALESCE(
            (
                SELECT
                    COUNT(*) AS "count"
                FROM
                    EVENT
                WHERE
                    EVENT_TYPE = 'CALL_END_DISPOSITION_SET'
                    AND ORG_ID = '${orgId}'
                    AND to_date(CREATED_AT) = to_date(date_rec)
            ), 0) AS "count"
          FROM
            date_range_table
          ORDER BY
            "date" ASC;
        `,
    });

    const setsStats = [];
    const rows = await consumeStream(dateRangeTableStatement.streamRows());
    for (const row of rows) {
      const date = row["date"];
      const value = row["count"];
      setsStats.push({ date, value });
    }

    console.log("Get Sets Stats:");

    return setsStats;
  } catch (err) {
    console.error(
      "Failed to execute statement due to the following error: " + err.message
    );
    throw err;
  }
}

export async function getDQStats(startDate, endDate, orgId) {
  try {
    const conn = await connectToMaxSnowflake();

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
            to_date(date_rec) AS "date",
            COALESCE(
              (
                SELECT
                  COUNT(*) AS "count"
                FROM
                  EVENT
                WHERE
                    EVENT_TYPE = 'CALL_END_DISPOSITION_SET'
                    AND ORG_ID = '${orgId}'
                    AND EVENT_DETAILS:disposition.label = 'DQ'
                    AND to_date(CREATED_AT) = to_date(date_rec)
              ), 0) AS "count"
          FROM
            date_range_table
          ORDER BY
            "date" ASC;
        `,
    });

    const dqStats = [];
    const rows = await consumeStream(dateRangeTableStatement.streamRows());
    for (const row of rows) {
      const date = row["date"];
      const value = row["count"];
      dqStats.push({ date, value });
    }

    console.log("Get DQ Stats:");

    return dqStats;
  } catch (err) {
    console.error(
      "Failed to execute statement due to the following error: " + err.message
    );
    throw err;
  }
}

function convertMaxStats(rows) {
  var result = [];

  // Extracting the keys from the first object in the array
  var keys = Object.keys(rows[0]);

  // Removing the 'date' key from the keys array
  var index = keys.indexOf("date");
  if (index > -1) {
    keys.splice(index, 1);
  }
  
  index = keys.indexOf("abs_date");
  if (index > -1) {
    keys.splice(index, 1);
  }

  // Iterating over each key and creating the desired format
  keys.forEach(function (key) {
    var obj = {
      name: key,
      data: [],
    };

    rows.forEach(function (item) {
      obj.data.push({
        date: item.date,
        value: item[key],
        abs_date: item.abs_date,
      });
    });

    result.push(obj);
  });

  return result;
}

export async function getMaxStats(startDate, endDate, orgId) {
  try {
    const conn = await connectToMaxSnowflake();

    const dateRangeTableStatement = conn.execute({
      sqlText: `
      WITH date_range_table (date_rec) AS (
        SELECT to_date('${startDate}') -- Start date
        UNION ALL
        SELECT to_date(dateadd(day, 1, date_rec))
        FROM date_range_table
        WHERE date_rec < '${endDate}' -- End date
      )
      SELECT
        to_char(to_date(date_rec), '%b %d') AS "date",
        to_date(date_rec) AS "abs_date",
        COALESCE(
          (SELECT COUNT(*) FROM LEAD WHERE to_date(CREATED_AT) = to_date(date_rec) AND ORG_ID = '${orgId}'),
          0) AS "Leads",
        COALESCE(
          (SELECT COUNT(*) FROM EVENT WHERE to_date(CREATED_AT) = to_date(date_rec) AND ORG_ID = '${orgId}' AND EVENT_TYPE = 'CALL_INITIATED'),
          0) AS "Dials",
        COALESCE(
          (SELECT COUNT(*) FROM EVENT WHERE to_date(CREATED_AT) = to_date(date_rec) AND ORG_ID = '${orgId}' AND EVENT_TYPE = 'CONFERENCE_END' AND EVENT_DETAILS:leadConnectedTime = 60000),
          0) AS "1-Minute Conversations",
        COALESCE(
          (SELECT COUNT(*) FROM EVENT WHERE to_date(CREATED_AT) = to_date(date_rec) AND ORG_ID = '${orgId}' AND EVENT_TYPE = 'CALL_END_DISPOSITION_SET'),
          0) AS "Sets",
        COALESCE(
          (SELECT COUNT(*) FROM EVENT WHERE to_date(CREATED_AT) = to_date(date_rec) AND ORG_ID = '${orgId}' AND EVENT_TYPE = 'CALL_END_DISPOSITION_SET' AND EVENT_DETAILS:disposition.label = 'DQ'),
          0) AS "DQ's"
      FROM
        date_range_table
      ORDER BY
        "abs_date" ASC;
        `,
    });

    const rows = await consumeStream(dateRangeTableStatement.streamRows());
    const maxStats = convertMaxStats(rows);
    return maxStats;
  } catch (err) {
    console.error(
      "Failed to execute statement due to the following error: " + err.message
    );
    throw err;
  }
}
