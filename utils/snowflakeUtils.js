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
  if (sherlockConnection) {
    const is_valid = sherlockConnection.isValidAsync();
    if (is_valid) {
      return sherlockConnection;
    }
  }

  sherlockConnection = snowflake.createConnection(connSherlockOptions);
  try {
    await sherlockConnection.connect();
    return sherlockConnection;
  } catch (err) {
    sherlockConnection = null;
    throw err;
  }
}

export async function connectToMaxSnowflake() {
  if (maxConnection) {
    const is_valid = maxConnection.isValidAsync();
    if (is_valid) {
      return maxConnection;
    }
  }

  maxConnection = snowflake.createConnection(connMaxOptions);
  try {
    await maxConnection.connect();
    return maxConnection;
  } catch (err) {
    maxConnection = null;
    throw err;
  }
}

export async function connectToMidasSnowflake() {
  if (midasConnection) {
    const is_valid = midasConnection.isValidAsync();
    if (is_valid) {
      return midasConnection;
    }
  }

  midasConnection = snowflake.createConnection(connMidasOptions);
  try {
    await midasConnection.connect();
    return midasConnection;
  } catch (err) {
    midasConnection = null;
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
