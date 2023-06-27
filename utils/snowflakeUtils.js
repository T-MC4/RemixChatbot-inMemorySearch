import snowflake from "snowflake-sdk";
import { config } from "dotenv";
config();

const connectionOptions = {
  account: process.env.SNOWFLAKE_ACCOUNT,
  username: process.env.SNOWFLAKE_USERNAME,
  password: process.env.SNOWFLAKE_PASSWORD,
  warehouse: process.env.SNOWFLAKE_WAREHOUSE,
  database: process.env.SNOWFLAKE_DATABASE,
  schema: process.env.SNOWFLAKE_SCHEMA,
  role: process.env.SNOWFLAKE_ROLE,
};


export async function connectToSnowflake() {
  const connection = snowflake.createConnection(connectionOptions);
  try {
    await connection.connect();
    return connection;
  } catch (err) {
    throw err;
  }
}
