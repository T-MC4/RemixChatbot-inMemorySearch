import express from "express";
import {
  getStates,
  getStatsId,
  createStatItem,
  updateStatItemName,
  updateStatItemValue,
  deleteStatItems,
} from "../models/stat.model.js";

const router = express.Router();

// GET /api/stat/
router.get("/", async (req, res) => {
  const { orgId } = req.body;

  if (!orgId) {
    return res.status(400).json({
      success: false,
      message: "Invalid orgId.",
    });
  }

  try {
    const states = [];
    const statIds = await getStatsId(orgId);
    for (const statName of Object.keys(statIds)) {
      states.push({ name: statName, id: statIds[statName] });
    }
    res.json({ success: true, data: states, message: "Success." });
  } catch (error) {
    res.json({ success: false, data: null, message: error.message });
  }
});

// GET /api/stat/:orgId
router.get("/:orgId", async (req, res) => {
  const { orgId } = req.params;
  const { startDate, endDate } = req.body;

  if (!startDate || !endDate || !orgId) {
    return res.status(400).json({
      success: false,
      message: "Invalid startDate, endDate, or orgId.",
    });
  }

  try {
    const states = await getStates(startDate, endDate, orgId);
    res.json({ success: true, data: states, message: "Success." });
  } catch (error) {
    res.json({ success: false, data: null, message: error.message });
  }
});

// POST /api/stat/:orgId
router.post("/:orgId", async (req, res) => {
  const { orgId } = req.params;
  const { title } = req.body;

  if (!orgId || !title) {
    return res
      .status(400)
      .json({ success: false, message: "Invalid orgId or title." });
  }

  try {
    const statId = await createStatItem(orgId, title);
    res.json({ success: true, statId, message: "Success." });
  } catch (error) {
    res.json({ success: false, statId: null, message: error.message });
  }
});

// PUT /api/stat/:statId/name
router.put("/:statId/name", async (req, res) => {
  const { statId } = req.params;
  const { title } = req.body;

  if (!statId || !title) {
    return res
      .status(400)
      .json({ success: false, message: "Invalid  statId, or title." });
  }

  try {
    await updateStatItemName(statId, title);
    res.json({ success: true, message: "Success." });
  } catch (error) {
    res.json({ success: false, message: error.message });
  }
});

// PUT /api/stat/:orgId/:statId/value
router.put("/:orgId/:statId/value", async (req, res) => {
  const { statId, orgId } = req.params;
  const { value, date } = req.body;

  if (!statId || !value || !date || !orgId) {
    return res.status(400).json({
      success: false,
      message: "Invalid statId, orgId, value, or date.",
    });
  }

  try {
    await updateStatItemValue(orgId, statId, value, date);
    res.json({ success: true, message: "Success." });
  } catch (error) {
    res.json({ success: false, message: error.message });
  }
});

// DELETE /api/stat/:orgId/:statId
router.delete("/:orgId/:statId", async (req, res) => {
  const { orgId, statId } = req.params;

  if (!orgId || !statId) {
    return res
      .status(400)
      .json({ success: false, message: "Invalid orgId or statId." });
  }

  try {
    await deleteStatItems(orgId, statId);
    res.json({ success: true, message: "Success." });
  } catch (error) {
    res.json({ success: false, message: error.message });
  }
});

export default router;
