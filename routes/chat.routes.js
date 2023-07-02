import {
  createChat,
  getChatData,
  getChatList,
  deleteChat,
  updateChatName,
  pushMessage,
} from "../models/chat.model.js";
import express from "express";

const router = express.Router();

// GET /api/chat/:userId
router.get("/:userId", async (req, res) => {
  const { userId } = req.params;

  if (!userId) {
    return res.status(400).json({ success: false, message: "Invalid userId." });
  }

  try {
    const chatList = await getChatList(userId);
    res.json({ success: true, items: chatList, message: "Success." });
  } catch (error) {
    res.json({ success: false, items: null, message: error.message });
  }
});

// GET /api/chat/:userId/:chatId
router.get("/:userId/:chatId", async (req, res) => {
  const { userId, chatId } = req.params;

  if (!userId || !chatId) {
    return res
      .status(400)
      .json({ success: false, message: "Invalid userId or chatId." });
  }

  try {
    const chatData = await getChatData(userId, chatId);
    res.json({ success: true, data: chatData, message: "Success." });
  } catch (error) {
    res.json({ success: false, data: null, message: error.message });
  }
});

// POST /api/chat/:userId
router.post("/:userId", async (req, res) => {
  const { userId } = req.params;
  const { from, to, chatName } = req.body;

  if (!userId) {
    return res.status(400).json({ success: false, message: "Invalid userId." });
  }

  if (!from || !to || !chatName) {
    return res
      .status(400)
      .json({ success: false, message: "Invalid chat data." });
  }

  try {
    const chatId = await createChat(userId, from, to, chatName);
    res.json({ success: true, chatId, message: "Success." });
  } catch (error) {
    res.json({ success: false, chatId: null, message: error.message });
  }
});

// PUT /api/chat/:userId/:chatId
router.put("/:userId/:chatId", async (req, res) => {
  const { userId, chatId } = req.params;
  const { isIn, text } = req.body;

  if (!userId || !chatId) {
    return res
      .status(400)
      .json({ success: false, message: "Invalid userId or chatId." });
  }

  if (typeof isIn === 'undefined' || !text) {
    return res
      .status(400)
      .json({ success: false, message: "Invalid message data." });
  }

  try {
    await pushMessage(userId, chatId, isIn, text);
    res.json({ success: true, message: "Success." });
  } catch (error) {
    res.json({ success: false, message: error.message });
  }
});

// PUT /api/chat/:userId/:chatId/name
router.put("/:userId/:chatId/name", async (req, res) => {
  const { userId, chatId } = req.params;
  const { chatName } = req.body;

  if (!userId || !chatId) {
    return res
      .status(400)
      .json({ success: false, message: "Invalid userId or chatId." });
  }
  if (!chatName) {
    return res
      .status(400)
      .json({ success: false, message: "Invalid chat name." });
  }

  try {
    await updateChatName(userId, chatId, chatName);
    res.json({ success: true, message: "Success." });
  } catch (error) {
    res.json({ success: false, message: error.message });
  }
});

// DELETE /api/chat/:userId/:chatId
router.delete("/:userId/:chatId", async (req, res) => {
  const { userId, chatId } = req.params;

  if (!userId || !chatId) {
    return res
      .status(400)
      .json({ success: false, message: "Invalid userId or chatId." });
  }

  try {
    await deleteChat(userId, chatId);
    res.json({ success: true, message: "Success." });
  } catch (error) {
    res.json({ success: false, message: error.message });
  }
});

export default router;
