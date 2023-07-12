import snowflake from "snowflake-sdk";
import { config } from "dotenv";
config();

const connSherlockOptions = {
  account: process.env.SNOWFLAKE_ACCOUNT,
  username: process.env.SNOWFLAKE_USERNAME,
  password: process.env.SNOWFLAKE_PASSWORD,
  warehouse: process.env.SNOWFLAKE_WAREHOUSE,
  database: process.env.SNOWFLAKE_DATABASE,
  schema: process.env.SNOWFLAKE_SCHEMA,
  role: process.env.SNOWFLAKE_ROLE,
};

const connMaxOptions = {
  account: process.env.SNOWFLAKE_ACCOUNT,
  username: process.env.SNOWFLAKE_USERNAME,
  password: process.env.SNOWFLAKE_PASSWORD,
  warehouse: process.env.SNOWFLAKE_WAREHOUSE,
  database: process.env.SNOWFLAKE_MAX_DATABASE,
  schema: process.env.SNOWFLAKE_MAX_SCHEMA,
  role: process.env.SNOWFLAKE_ROLE,
};

const connMidasOptions = {
  account: process.env.SNOWFLAKE_ACCOUNT,
  username: process.env.SNOWFLAKE_USERNAME,
  password: process.env.SNOWFLAKE_PASSWORD,
  warehouse: process.env.SNOWFLAKE_WAREHOUSE,
  database: process.env.SNOWFLAKE_MIDAS_DATABASE,
  schema: process.env.SNOWFLAKE_MIDAS_SCHEMA,
  role: process.env.SNOWFLAKE_ROLE,
};

export async function connectToSherlockSnowflake() {
  const connection = snowflake.createConnection(connSherlockOptions);
  try {
    await connection.connect();
    return connection;
  } catch (err) {
    throw err;
  }
}

export async function connectToMaxSnowflake() {
  const connection = snowflake.createConnection(connMaxOptions);
  try {
    await connection.connect();
    return connection;
  } catch (err) {
    throw err;
  }
}

export async function connectToMidasSnowflake() {
  const connection = snowflake.createConnection(connMidasOptions);
  try {
    await connection.connect();
    return connection;
  } catch (err) {
    throw err;
  }
}

export function consumeStream(stream) {
  return new Promise((resolve, reject) => {
    const rows = [];

    stream.on("error", (err) => {
      reject(new Error("Unable to consume all rows"));
    });

    stream.on("data", (row) => {
      // Consume result row...
      rows.push(row);
    });

    stream.on("end", () => {
      resolve(rows);
    });
  });
}
