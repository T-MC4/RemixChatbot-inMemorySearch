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

let sherlockConnection = null;
let maxConnection = null;
let midasConnection = null;

export async function connectToSherlockSnowflake() {
  if (!sherlockConnection) {
    sherlockConnection = snowflake.createConnection(connSherlockOptions);
    try {
      await sherlockConnection.connect();
    } catch (err) {
      sherlockConnection = null;
      throw err;
    }
  }
  return sherlockConnection;
}

export async function connectToMaxSnowflake() {
  if (!maxConnection) {
    maxConnection = snowflake.createConnection(connMaxOptions);
    try {
      await maxConnection.connect();
    } catch (err) {
      maxConnection = null;
      throw err;
    }
  }
  return maxConnection;
}

export async function connectToMidasSnowflake() {
  if (!midasConnection) {
    midasConnection = snowflake.createConnection(connMidasOptions);
    try {
      await midasConnection.connect();
    } catch (err) {
      midasConnection = null;
      throw err;
    }
  }
  return midasConnection;
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
