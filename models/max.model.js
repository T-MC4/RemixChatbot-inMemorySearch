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

export async function getDialStats(startDate, endDate, orgId, userId) {
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
                AND USER_ID = '${userId}'
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

export async function getOneMinConversionStats(
  startDate,
  endDate,
  orgId,
  userId
) {
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
                AND USER_ID = '${userId}'
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
