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
let sherlockConnectionTimestamp = null;

let maxConnection = null;
let maxConnectionTimestamp = null;

let midasConnection = null;
let midasConnectionTimestamp = null;

const CONNECTION_EXPIRATION_TIME = 60 * 60 * 1000; // 1 hour in milliseconds

export async function connectToSherlockSnowflake() {
  if (sherlockConnection && isConnectionValid(sherlockConnectionTimestamp)) {
    return sherlockConnection;
  }

  sherlockConnection = snowflake.createConnection(connSherlockOptions);
  try {
    await sherlockConnection.connect();
    updateConnectionTimestamp('sherlock');
    return sherlockConnection;
  } catch (err) {
    sherlockConnection = null;
    throw err;
  }
}

export async function connectToMaxSnowflake() {
  if (maxConnection && isConnectionValid(maxConnectionTimestamp)) {
    return maxConnection;
  }

  maxConnection = snowflake.createConnection(connMaxOptions);
  try {
    await maxConnection.connect();
    updateConnectionTimestamp('max');
    return maxConnection;
  } catch (err) {
    maxConnection = null;
    throw err;
  }
}

export async function connectToMidasSnowflake() {
  if (midasConnection && isConnectionValid(midasConnectionTimestamp)) {
    return midasConnection;
  }

  midasConnection = snowflake.createConnection(connMidasOptions);
  try {
    await midasConnection.connect();
    updateConnectionTimestamp('midas');
    return midasConnection;
  } catch (err) {
    midasConnection = null;
    throw err;
  }
}

function isConnectionValid(timestamp) {
  if (!timestamp) {
    return false;
  }

  const currentTime = new Date().getTime();
  const connectionTime = timestamp.getTime();
  const elapsedMilliseconds = currentTime - connectionTime;

  return elapsedMilliseconds <= CONNECTION_EXPIRATION_TIME;
}

function updateConnectionTimestamp(connectionType) {
  const currentTime = new Date();
  switch (connectionType) {
    case 'sherlock':
      sherlockConnectionTimestamp = currentTime;
      break;
    case 'max':
      maxConnectionTimestamp = currentTime;
      break;
    case 'midas':
      midasConnectionTimestamp = currentTime;
      break;
    // Add cases for other connections if necessary
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
