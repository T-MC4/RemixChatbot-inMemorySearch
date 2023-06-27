import { connectToSnowflake } from "../utils/snowflakeUtils.js";
import { v4 as uuidv4 } from "uuid";

export async function getChatList(userId) {
  try {
    const conn = await connectToSnowflake();
    const statement = conn.execute({
      sqlText: `
        -- Query to fetch chat list
        SELECT
          c.chatId,
          c.chatName
        FROM
          ChatList AS c
        WHERE
          c.userId = '${userId}';
      `,
    });

    const chatList = [];
    while (statement.next()) {
      const chatId = statement.getColumnValue(1);
      const chatName = statement.getColumnValue(2);
      chatList.push({ chatId, chatName });
    }

    console.log("Chat List:", chatList);
    return chatList;
  } catch (err) {
    console.error(
      "Failed to execute statement due to the following error: " + err.message
    );
    return null;
  }
}

export async function getChatData(userId, chatId) {
  try {
    const conn = await connectToSnowflake();
    const statement = conn.execute({
      sqlText: `
        -- Query to fetch chat data
        SELECT
          text,
          isIn,
          time
        FROM
          ChatData AS cd
        WHERE
          cd.userId = '${userId}'
          AND cd.chatId = '${chatId}';
      `,
    });

    const chatData = [];
    while (statement.next()) {
      const text = statement.getColumnValue(1);
      const isIn = statement.getColumnValue(2);
      const time = statement.getColumnValue(3);
      chatData.push({ text, isIn, time });
    }

    console.log("Chat Data:", chatData);
    return chatData;
  } catch (err) {
    console.error(
      "Failed to execute statement due to the following error: " + err.message
    );
    return null;
  }
}

export async function createChat(userId, from, to, chatName) {
  try {
    const conn = await connectToSnowflake();
    const chatId = uuidv4(); // Function to generate a unique UUID
    const statement = conn.execute({
      sqlText: `
        -- Query to create a chat
        INSERT INTO ChatList (userId, chatId, chatName, fromDate, toDate)
        VALUES ('${userId}', '${chatId}', '${chatName}', '${from}', '${to}');
      `,
    });
    console.log("Chat created with ID:", chatId);
    return chatId;
  } catch (err) {
    console.error(
      "Failed to execute statement due to the following error: " + err.message
    );
    return null;
  }
}

export async function pushMessage(userId, chatId, isIn, text) {
  try {
    const conn = await connectToSnowflake();
    const statement = conn.execute({
      sqlText: `
        -- Query to push a message
        INSERT INTO ChatData (userId, chatId, isIn, text, time)
        VALUES ('${userId}', '${chatId}', ${isIn}, '${text}', CURRENT_TIMESTAMP);
      `,
    });

    console.log("Message pushed successfully.");
    return true;
  } catch (err) {
    console.error(
      "Failed to execute statement due to the following error: " + err.message
    );
    return false;
  }
}

export async function updateChatName(userId, chatId, chatName) {
  try {
    const conn = await connect;

    ToSnowflake();
    const statement = conn.execute({
      sqlText: `
        -- Query to update chat name
        UPDATE ChatList
        SET chatName = '${chatName}'
        WHERE userId = '${userId}' AND chatId = '${chatId}';
      `,
    });

    console.log("Chat name updated successfully.");
    return true;
  } catch (err) {
    console.error(
      "Failed to execute statement due to the following error: " + err.message
    );
    return false;
  }
}

export async function deleteChat(userId, chatId) {
  try {
    const conn = await connectToSnowflake();
    const statement = conn.execute({
      sqlText: `
        -- Query to delete a chat
        DELETE FROM ChatList
        WHERE userId = '${userId}' AND chatId = '${chatId}';
        -- Query to delete a chat data
        DELETE FROM ChatData
        WHERE userId = '${userId}' AND chatId = '${chatId}';
      `,
    });

    console.log("Chat deleted successfully.");
    return true;
  } catch (err) {
    console.error(
      "Failed to execute statement due to the following error: " + err.message
    );
    return false;
  }
}

export async function createChatListTable() {
  try {
    const conn = await connectToSnowflake();
    const statement = conn.execute({
      sqlText: `
        CREATE TABLE ChatList (
          userId VARCHAR(255),
          chatId VARCHAR(255),
          chatName VARCHAR(255),
          fromDate DATE,
          toDate DATE,
          PRIMARY KEY (userId, chatId)
        );
      `,
    });

    console.log("ChatList table created successfully.");
  } catch (err) {
    console.error(
      "Failed to execute statement due to the following error: " + err.message
    );
  }
}

export async function createChatDataTable() {
  try {
    const conn = await connectToSnowflake();
    const statement = conn.execute({
      sqlText: `
        CREATE TABLE ChatData (
          userId VARCHAR(255),
          chatId VARCHAR(255),
          isIn BOOLEAN,
          text VARCHAR(1000),
          time TIMESTAMP_NTZ,
          PRIMARY KEY (userId, chatId, time)
        );
      `,
    });

    console.log("ChatData table created successfully.");
  } catch (err) {
    console.error(
      "Failed to execute statement due to the following error: " + err.message
    );
  }
}
