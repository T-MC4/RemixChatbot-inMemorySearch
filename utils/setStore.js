import {
  S3Client,
  ListObjectsV2Command,
  GetObjectCommand,
  PutObjectCommand,
  DeleteObjectCommand,
  CopyObjectCommand,
} from "@aws-sdk/client-s3";
import { v4 as uuidv4 } from "uuid";
import dotenv from "dotenv";
dotenv.config();

// File store bucket name
const setStoreBucketName =
  process.env.AWS_SET_STORE_BUCKET_NAME || "sherlock-org-sets";

// Create S3 client
const setStore = new S3Client({
  region: "auto",
  endpoint: `https://${process.env.AWS_ACCOUNT_ID_SET_STORE}.r2.cloudflarestorage.com`,
  credentials: {
    accessKeyId: `${process.env.AWS_ACCOUNT_ACCESS_KEY_ID_SET_STORE}`,
    secretAccessKey: `${process.env.AWS_ACCOUNT_ACCESS_KEY_SECRET_SET_STORE}`,
  },
});

export async function getSessionList(userId) {
  try {
    const params = {
      Bucket: setStoreBucketName,
      Delimiter: "/",
      Prefix: `${userId}/`,
      StartAfter: "",
    };

    const response = await setStore.send(new ListObjectsV2Command(params));
    const jsonFiles = response.Contents?.filter((obj) =>
      obj.Key?.endsWith(".json")
    );
    const sessionList = jsonFiles.map((file) => {
      const fileName = file.Key?.split("/").pop()?.replace(".json", "");
      const sessionId = fileName?.split("_")[1];
      const sessionName = fileName?.split("_")[0];
      return { sessionId, sessionName };
    });

    return {
      success: true,
      items: sessionList || [],
      message: "Success.",
    };
  } catch (error) {
    return {
      success: false,
      items: null,
      message: error.message,
    };
  }
}

export async function saveSession(userId, data, sessionName) {
  try {
    const sessionId = uuidv4();
    const jsonContent = JSON.stringify(data);
    const sessionFileName = `${
      sessionName || new Date().getTime()
    }_${sessionId}.json`;
    const params = {
      Bucket: setStoreBucketName,
      Key: `${userId}/${sessionFileName}`,
      Body: jsonContent,
      ContentType: "application/json",
    };

    await setStore.send(new PutObjectCommand(params));

    return {
      success: true,
      sessionId,
      message: "Success.",
    };
  } catch (error) {
    return {
      success: false,
      sessionId: null,
      message: error.message,
    };
  }
}
export async function updateSessionName(userId, sessionId, sessionName) {
  try {
    const sessionList = await getSessionList(userId);
    const oldSessionName = sessionList.items.find(
      (session) => session.sessionId === sessionId
    );
    if (!oldSessionName) {
      throw new Error("Old SessionName not found.");
    }
    const sessionData = await getSessionData(userId, sessionId);
    if (!sessionData.success) {
      throw new Error("Session not found.");
    }

    const oldSessionKey = `${userId}/${oldSessionName.sessionName}_${sessionId}.json`;
    const newSessionKey = `${userId}/${sessionName}_${sessionId}.json`;
    // Copy the file with the new name to the destination
    const copyParams = {
      Bucket: setStoreBucketName,
      CopySource: `${setStoreBucketName}/${oldSessionKey}`,
      Key: newSessionKey,
    };
    await setStore.send(new CopyObjectCommand(copyParams));

    // Delete the original file
    const deleteParams = {
      Bucket: setStoreBucketName,
      Key: oldSessionKey,
    };
    await setStore.send(new DeleteObjectCommand(deleteParams));

    return {
      success: true,
      message: "Success.",
    };
  } catch (error) {
    return {
      success: false,
      message: error.message,
    };
  }
}

export async function getSessionData(userId, sessionId) {
  try {
    const sessionList = await getSessionList(userId);
    const session = sessionList.items.find(
      (session) => session.sessionId === sessionId
    );
    if (!session) {
      throw new Error("Session not found.");
    }

    const params = {
      Bucket: setStoreBucketName,
      Key: `${userId}/${session.sessionName}_${sessionId}.json`,
    };

    const response = await setStore.send(new GetObjectCommand(params));
    const body = await response.Body?.transformToString();
    const data = JSON.parse(body);

    return {
      success: true,
      data,
      message: "Success.",
    };
  } catch (error) {
    return {
      success: false,
      data: null,
      message: error.message,
    };
  }
}

export async function deleteSession(userId, sessionId) {
  try {
    const sessionList = await getSessionList(userId);
    const session = sessionList.items.find(
      (session) => session.sessionId === sessionId
    );
    if (!session) {
      throw new Error("Session not found.");
    }

    const sessionKey = `${userId}/${session.sessionName}_${sessionId}.json`;
    const deleteParams = {
      Bucket: setStoreBucketName,
      Key: sessionKey,
    };

    await setStore.send(new DeleteObjectCommand(deleteParams));

    return {
      success: true,
      message: "Success.",
    };
  } catch (error) {
    return {
      success: false,
      message: error.message,
    };
  }
}

export async function updateSessionData(userId, sessionId, data) {
  try {
    const sessionList = await getSessionList(userId);
    const session = sessionList.items.find(
      (session) => session.sessionId === sessionId
    );
    if (!session) {
      throw new Error("Session not found.");
    }
    const jsonContent = JSON.stringify(data);
    const sessionKey = `${userId}/${session.sessionName}_${sessionId}.json`;
    const updateParams = {
      Bucket: setStoreBucketName,
      Key: sessionKey,
      Body: jsonContent,
      ContentType: "application/json",
    };

    await setStore.send(new PutObjectCommand(updateParams));

    return {
      success: true,
      message: "Success.",
    };
  } catch (error) {
    return {
      success: false,
      message: error.message,
    };
  }
}
