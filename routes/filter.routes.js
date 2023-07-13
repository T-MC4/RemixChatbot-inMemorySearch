import express from "express";
import {
  getCustomFilter,
  createCustomFilter,
  deleteCustomFilter,
} from "../models/filter.model.js";

const router = express.Router();

// GET /api/filter/:userId
router.get("/:userId", async (req, res) => {
  const { userId } = req.params;

  if (!userId) {
    return res.status(400).json({ success: false, message: "Invalid userId." });
  }

  try {
    const customFilter = await getCustomFilter(userId);
    res.json({ success: true, items: customFilter, message: "Success." });
  } catch (error) {
    res.json({ success: false, items: null, message: error.message });
  }
});

// POST /api/filter/:userId
router.post("/:userId", async (req, res) => {
  const { userId } = req.params;
  const { title, options } = req.body;

  if (!userId) {
    return res.status(400).json({ success: false, message: "Invalid userId." });
  }

  if (!title || !options) {
    return res
      .status(400)
      .json({ success: false, message: "Invalid filter data." });
  }

  try {
    const filterId = await createCustomFilter(userId, title, options);
    res.json({ success: true, filterId, message: "Success." });
  } catch (error) {
    res.json({ success: false, filterId: null, message: error.message });
  }
});

// DELETE /api/filter/:userId/:filterId
router.delete("/:userId/:filterId", async (req, res) => {
  const { userId, filterId } = req.params;

  if (!userId || !filterId) {
    return res
      .status(400)
      .json({ success: false, message: "Invalid userId or filterId." });
  }

  try {
    await deleteCustomFilter(userId, filterId);
    res.json({ success: true, message: "Success." });
  } catch (error) {
    res.json({ success: false, message: error.message });
  }
});

export default router;
