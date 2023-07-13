import { connectToMidasSnowflake, consumeStream } from "../utils/snowflakeUtils.js";

export async function getClosesStats(startDate, endDate, orgId) {
  try {
    const conn = await connectToMidasSnowflake();

    
    const dateRangeTableStatement = conn.execute({
      sqlText: `
          WITH date_range_table (date_rec) AS (
            SELECT
              TO_DATE('${startDate}') -- Start date
            UNION ALL
            SELECT
              TO_DATE(DATEADD(day, 1, date_rec))
            FROM
              date_range_table
            WHERE
              date_rec < '${endDate}' -- End date
          )
          SELECT
            date_rec AS "date",
            COALESCE(
              (
                SELECT
                  COUNT(*)
                FROM
                  CHECKOUT c
                JOIN
                  CUSTOMER cust ON c.CUSTOMER_ID = cust.ID
                JOIN
                  ADMIN a ON a.MERCHANT_ID = c.MERCHANT_ID
                JOIN
                  AGREEMENT sales ON c.SALES_AGREEMENT_ID = sales.ID
                WHERE
                  a.ORGANIZATION_ID = '${orgId}'
                  AND (
                    sales.STATUS <> 'PENDING'
                    OR (
                      c.STATUS ILIKE '%SUCCESS%'
                      OR c.STATUS ILIKE '%RPA%'
                      OR c.STATUS ILIKE '%PAID%'
                      OR (
                        c.STATUS <> 'PENDING'
                        AND c.DOWN_PAYMENT_AMOUNT > 0
                      )
                    )
                  )
                  AND to_date(c.CREATED_AT) = to_date(date_rec)
              ), 0) AS "count"
          FROM
            date_range_table
          ORDER BY
            "date" ASC;
        `,
    });

    const closesStats = [];
    const rows = await consumeStream(dateRangeTableStatement.streamRows());
    for (const row of rows) {
      const date = row["date"];
      const value = row["count"];
      closesStats.push({ date, value });
    }

    console.log("Get Closes Stats:");

    return closesStats;
  } catch (err) {
    console.error(
      "Failed to execute statement due to the following error: " + err.message
    );
    throw err;
  }
}

export async function getPiFStats(startDate, endDate, orgId) {
  try {
    const conn = await connectToMidasSnowflake();

    const dateRangeTableStatement = conn.execute({
      sqlText: `
          WITH date_range_table (date_rec) AS (
            SELECT
              TO_DATE('${startDate}') -- Start date
            UNION ALL
            SELECT
              TO_DATE(DATEADD(day, 1, date_rec))
            FROM
              date_range_table
            WHERE
              date_rec < '${endDate}' -- End date
          )
          SELECT
            date_rec AS "date",
            COALESCE(
              (
                SELECT
                  COUNT(*)
                FROM
                  CHECKOUT c
                JOIN
                  CUSTOMER cust ON c.CUSTOMER_ID = cust.ID
                JOIN
                  ADMIN a ON a.MERCHANT_ID = c.MERCHANT_ID
                WHERE
                  a.ORGANIZATION_ID = '${orgId}'
                  AND (
                    c.STATUS ILIKE '%SW_SUCCESS%'
                    OR c.STATUS ILIKE '%FULLY_PAID%'
                  )
                  AND to_date(c.CREATED_AT) = to_date(date_rec)
              ), 0) AS "count"
          FROM
            date_range_table
          ORDER BY
            "date" ASC;
        `,
    });


    const piFStats = [];
    const rows = await consumeStream(dateRangeTableStatement.streamRows());
    for (const row of rows) {
      const date = row["date"];
      const value = row["count"];
      piFStats.push({ date, value });
    }

    console.log("Get PiF Stats:");

    return piFStats;
  } catch (err) {
    console.error(
      "Failed to execute statement due to the following error: " + err.message
    );
    throw err;
  }
}

export async function getPayPlanStats(startDate, endDate, orgId) {
  try {
    const conn = await connectToMidasSnowflake();

    const dateRangeTableStatement = conn.execute({
      sqlText: `
          WITH date_range_table (date_rec) AS (
            SELECT
              TO_DATE('${startDate}') -- Start date
            UNION ALL
            SELECT
              TO_DATE(DATEADD(day, 1, date_rec))
            FROM
              date_range_table
            WHERE
              date_rec < '${endDate}' -- End date
          )
          SELECT
            date_rec AS "date",
            COALESCE(
              (
                SELECT
                  COUNT(*)
                FROM
                  CHECKOUT c
                JOIN
                  CUSTOMER cust ON c.CUSTOMER_ID = cust.ID
                JOIN
                  ADMIN a ON a.MERCHANT_ID = c.MERCHANT_ID
                WHERE
                  a.ORGANIZATION_ID = '${orgId}'
                  AND (
                    c.STATUS ILIKE '%NLC_SUCCESS%'
                    OR c.STATUS ILIKE '%RPA%'
                  )
                  AND to_date(c.CREATED_AT) = to_date(date_rec)
              ), 0) AS "count"
          FROM
            date_range_table
          ORDER BY
            "date" ASC;
        `,
    });

    const payPlanStats = [];
    const rows = await consumeStream(dateRangeTableStatement.streamRows());
    for (const row of rows) {
      const date = row["date"];
      const value = row["count"];
      payPlanStats.push({ date, value });
    }

    console.log("Get Pay Plan Stats:");

    return payPlanStats;
  } catch (err) {
    console.error(
      "Failed to execute statement due to the following error: " + err.message
    );
    throw err;
  }
}

export async function getCashStats(startDate, endDate, orgId) {
  try {
    const conn = await connectToMidasSnowflake();

    
    const dateRangeTableStatement = conn.execute({
      sqlText: `
          WITH date_range_table (date_rec) AS (
            SELECT
              TO_DATE('${startDate}') -- Start date
            UNION ALL
            SELECT
              TO_DATE(DATEADD(day, 1, date_rec))
            FROM
              date_range_table
            WHERE
              date_rec < '${endDate}' -- End date
          )
          SELECT
            date_rec AS "date",
            COALESCE(
              (
                SELECT
                  SUM(
                    CASE
                      WHEN c.STATUS ILIKE '%SUCCESS%' THEN c.DOWN_PAYMENT_AMOUNT + c.FLEXDOWN_AMOUNT + c.TOTAL_AMOUNT
                      ELSE c.DOWN_PAYMENT_AMOUNT + c.FLEXDOWN_AMOUNT
                    END
                  )
                FROM
                  CHECKOUT c
                JOIN
                  CUSTOMER cust ON c.CUSTOMER_ID = cust.ID
                JOIN
                  ADMIN a ON a.MERCHANT_ID = c.MERCHANT_ID
                JOIN
                  AGREEMENT sales ON c.SALES_AGREEMENT_ID = sales.ID
                WHERE
                  a.ORGANIZATION_ID = '${orgId}'
                  AND (
                    sales.STATUS <> 'PENDING'
                    OR (
                      c.STATUS ILIKE '%SUCCESS%'
                      OR c.STATUS ILIKE '%RPA%'
                      OR c.STATUS ILIKE '%PAID%'
                      OR (
                        c.STATUS <> 'PENDING'
                        AND c.DOWN_PAYMENT_AMOUNT > 0
                      )
                    )
                  )
                  AND to_date(c.CREATED_AT) = to_date(date_rec)
              ), 0) AS "count"
          FROM
            date_range_table
          ORDER BY
            "date" ASC;
        `,
    });
    
    const cashStats = [];
    const rows = await consumeStream(dateRangeTableStatement.streamRows());
    for (const row of rows) {
      const date = row["date"];
      const value = row["count"];
      cashStats.push({ date, value });
    }

    console.log("Get Cash Stats:");

    return cashStats;
  } catch (err) {
    console.error(
      "Failed to execute statement due to the following error: " + err.message
    );
    throw err;
  }
}
