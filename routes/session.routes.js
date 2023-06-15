import express from "express";
import {
  getSessionList,
  getSessionData,
  saveSession,
  updateSessionData,
  updateSessionName,
  deleteSession,
} from "../utils/setStore.js";

const router = express.Router();

// Get Session List
router.get("/:userId", async (req, res) => {
  const { userId } = req.params;
  const result = await getSessionList(userId);
  res.json({
    ...result,
  });
});

// Get Session Data
router.get("/:userId/:sessionId", async (req, res) => {
  const { userId, sessionId } = req.params;
  const result = await getSessionData(userId, sessionId);
  res.json({
    ...result,
  });
});

// Save Session
router.post("/:userId", async (req, res) => {
  const { userId } = req.params;
  const { data, sessionName } = req.body;
  const result = await saveSession(userId, data, sessionName);
  res.json({
    ...result,
  });
});

// Update Session Data
router.put("/:userId/:sessionId", async (req, res) => {
  const { userId, sessionId } = req.params;
  const { data } = req.body;
  const result = await updateSessionData(userId, sessionId, data);
  res.json({
    ...result,
  });
});

// Update Session Name
router.put("/:userId/:sessionId/name", async (req, res) => {
  const { userId, sessionId } = req.params;
  const { sessionName } = req.body;
  const result = await updateSessionName(userId, sessionId, sessionName);
  res.json({
    ...result,
  });
});

// Delete Session
router.delete("/:userId/:sessionId", async (req, res) => {
  const { sessionId, userId } = req.params;
  const result = await deleteSession(userId, sessionId);
  res.json({
    ...result,
  });
});

export default router;
