import {
  connectToMidasSnowflake,
  consumeStream,
} from "../utils/snowflakeUtils.js";

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

function convertMidasStats(rows) {
  var result = [];

  // Extracting the keys from the first object in the array
  var keys = Object.keys(rows[0]);

  // Removing the 'date' key from the keys array
  var index = keys.indexOf("date");
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
      });
    });

    result.push(obj);
  });

  return result;
}

export async function getMidasStats(startDate, endDate, orgId) {
  try {
    const conn = await connectToMidasSnowflake();

    const dateRangeTableStatement = conn.execute({
      sqlText: `
        WITH date_range_table (date_rec) AS (
          SELECT TO_DATE('${startDate}') -- Start date
          UNION ALL
          SELECT TO_DATE(DATEADD(day, 1, date_rec))
          FROM date_range_table
          WHERE date_rec < '${endDate}' -- End date
        )
        SELECT
          to_char(to_date(date_rec), '%b %d') AS "date",
          COALESCE(closed_count, 0) AS "Closes",
          COALESCE(pif_count, 0) AS "PiF's",
          COALESCE(payplan_count, 0) AS "Pay Plan",
          COALESCE(cash_count, 0) AS "Cash"
        FROM
          date_range_table
        LEFT JOIN (
          SELECT
            to_date(c.CREATED_AT) AS date,
            COUNT(*) AS closed_count
          FROM
            CHECKOUT c
            JOIN ADMIN a ON a.MERCHANT_ID = c.MERCHANT_ID
            JOIN AGREEMENT sales ON c.SALES_AGREEMENT_ID = sales.ID
          WHERE
            a.ORGANIZATION_ID = '${orgId}'
            AND (
              sales.STATUS <> 'PENDING'
              OR (
                c.STATUS ILIKE '%SUCCESS%'
                OR c.STATUS ILIKE '%RPA%'
                OR c.STATUS ILIKE '%PAID%'
                OR (c.STATUS <> 'PENDING' AND c.DOWN_PAYMENT_AMOUNT > 0)
              )
            )
          GROUP BY to_date(c.CREATED_AT)
        ) closed ON date_rec = closed.date
        LEFT JOIN (
          SELECT
            to_date(c.CREATED_AT) AS date,
            COUNT(*) AS pif_count
          FROM
            CHECKOUT c
            JOIN ADMIN a ON a.MERCHANT_ID = c.MERCHANT_ID
          WHERE
            a.ORGANIZATION_ID = '${orgId}'
            AND (
              c.STATUS ILIKE '%SW_SUCCESS%'
              OR c.STATUS ILIKE '%FULLY_PAID%'
            )
          GROUP BY to_date(c.CREATED_AT)
        ) pif ON date_rec = pif.date
        LEFT JOIN (
          SELECT
            to_date(c.CREATED_AT) AS date,
            COUNT(*) AS payplan_count
          FROM
            CHECKOUT c
            JOIN ADMIN a ON a.MERCHANT_ID = c.MERCHANT_ID
          WHERE
            a.ORGANIZATION_ID = '${orgId}'
            AND (
              c.STATUS ILIKE '%NLC_SUCCESS%'
              OR c.STATUS ILIKE '%RPA%'
            )
          GROUP BY to_date(c.CREATED_AT)
        ) payplan ON date_rec = payplan.date
        LEFT JOIN (
          SELECT
            to_date(c.CREATED_AT) AS date,
            SUM(
              CASE
                WHEN c.STATUS ILIKE '%SUCCESS%' THEN c.DOWN_PAYMENT_AMOUNT + c.FLEXDOWN_AMOUNT + c.TOTAL_AMOUNT
                ELSE c.DOWN_PAYMENT_AMOUNT + c.FLEXDOWN_AMOUNT
              END
            ) AS cash_count
          FROM
            CHECKOUT c
            JOIN ADMIN a ON a.MERCHANT_ID = c.MERCHANT_ID
            JOIN AGREEMENT sales ON c.SALES_AGREEMENT_ID = sales.ID
          WHERE
            a.ORGANIZATION_ID = '${orgId}'
            AND (
              sales.STATUS <> 'PENDING'
              OR (
                c.STATUS ILIKE '%SUCCESS%'
                OR c.STATUS ILIKE '%RPA%'
                OR c.STATUS ILIKE '%PAID%'
                OR (c.STATUS <> 'PENDING' AND c.DOWN_PAYMENT_AMOUNT > 0)
              )
            )
          GROUP BY to_date(c.CREATED_AT)
        ) cash ON date_rec = cash.date
        ORDER BY "date" ASC;
      
        `,
    });

    const rows = await consumeStream(dateRangeTableStatement.streamRows());

    const midasStats = convertMidasStats(rows);

    return midasStats;
  } catch (err) {
    console.error(
      "Failed to execute statement due to the following error: " + err.message
    );
    throw err;
  }
}
