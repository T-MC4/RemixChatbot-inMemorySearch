import { connectToSherlockSnowflake, consumeStream } from "../utils/snowflakeUtils.js";
import { v4 as uuidv4 } from "uuid";

export async function getCustomFilter(userId) {
  try {
    const conn = await connectToSherlockSnowflake();
    const statement = conn.execute({
      sqlText: `
      -- Query to fetch custom filter
      SELECT 
        f.filterId,
        f.title,
        f.options
      FROM 
        Filters AS f
      WHERE 
        f.userId = '${userId}'`,
    });

    const customFilter = [];
    const rows = await consumeStream(statement.streamRows());

    for (const row of rows) {
      const filterId = row["FILTERID"];
      const title = row["TITLE"];
      const options = row["OPTIONS"];
      customFilter.push({ filterId, title, options });
    }
    return customFilter;
  } catch (err) {
    console.error(
      "Failed to execute statement due to the following error: " + err.message
    );
    throw err;
  }
}

export async function createCustomFilter(userId, title, options) {
  try {
    const conn = await connectToSherlockSnowflake();
    const filterId = uuidv4();
    const statement = conn.execute({
      sqlText: `
      -- Query to create a custom filter
      INSERT INTO Filters (filterId, userId, title, options) 
      SELECT ?, ?, ?, PARSE_JSON(?)`,
      binds: [filterId, userId, title, JSON.stringify(options)],
    });
    console.log("Custom filter created successfully.");
    return filterId;
  } catch (err) {
    console.error(
      "Failed to execute statement due to the following error: " + err.message
    );
    throw err;
  }
}

export async function deleteCustomFilter(userId, filterId) {
  try {
    const conn = await connectToSherlockSnowflake();
    const statement = conn.execute({
      sqlText: `
      -- Query to delete a custom filter
      DELETE FROM Filters 
      WHERE userId = '${userId}' AND filterId = '${filterId}'`,
    });
    console.log("Custom filter deleted successfully.");
    return true;
  } catch (err) {
    console.error(
      "Failed to execute statement due to the following error: " + err.message
    );
    throw err;
  }
}

export async function createFiltersTable() {
  try {
    const conn = await connectToSherlockSnowflake();
    const statement = conn.execute({
      sqlText: `
          CREATE TABLE IF NOT EXISTS Filters (
            filterId VARCHAR(255) PRIMARY KEY,
            userId VARCHAR(255),
            title TEXT,
            options VARIANT
          )
        `,
    });

    console.log("Filters table created successfully.");
  } catch (err) {
    console.error(
      "Failed to execute statement due to the following error: " + err.message
    );
    throw err;
  }
}
