import express from "express";
import {
  getStates,
  getStatsId,
  createStatItem,
  updateStatItemName,
  updateStatItemValue,
  deleteStatItems,
  updateStatItemFormatter,
} from "../models/stat.model.js";

const router = express.Router();

// GET /api/stat/
router.get("/:orgId/list", async (req, res) => {
  const { orgId } = req.params;

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
      states.push({
        name: statName,
        id: statIds[statName]["statId"],
        category: statIds[statName]["category"],
        formatter: statIds[statName]["formatter"],
        isFixed: statIds[statName]["isFixed"],
      });
    }
    res.json({ success: true, data: states, message: "Success." });
  } catch (error) {
    res.json({ success: false, data: null, message: error.message });
  }
});

// GET /api/stat/:orgId
router.get("/:orgId", async (req, res) => {
  const { orgId } = req.params;
  const { startDate, endDate } = req.query;

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
  const { title, category, formatter } = req.body;

  if (!orgId || !title || !category || !formatter) {
    return res.status(400).json({
      success: false,
      message: "Invalid orgId, title, category or formatter.",
    });
  }

  try {
    const statId = await createStatItem(orgId, title, category, formatter);
    res.json({ success: true, statId, message: "Success." });
  } catch (error) {
    res.json({ success: false, statId: null, message: error.message });
  }
});

// PUT /api/stat/name/:orgId/:statId
router.put("/name/:orgId/:statId", async (req, res) => {
  const { statId, orgId } = req.params;
  const { value } = req.body;

  if (!statId || !value) {
    return res
      .status(400)
      .json({ success: false, message: "Invalid  statId, or title." });
  }

  try {
    if (await updateStatItemName(orgId, statId, value)) {
      return res.json({ success: true, message: "Success." });
    }
    return res.json({
      success: false,
      message: "Cannot update fixed stats name.",
    });
  } catch (error) {
    res.json({ success: false, message: error.message });
  }
});

// PUT /api/stat/formatter/:orgId/:statId
router.put("/formatter/:orgId/:statId", async (req, res) => {
  const { statId, orgId } = req.params;
  const { value } = req.body;

  if (!statId || !value) {
    return res
      .status(400)
      .json({ success: false, message: "Invalid  statId, or formatter." });
  }

  try {
    await updateStatItemFormatter(orgId, statId, value);
    res.json({ success: true, message: "Success." });
  } catch (error) {
    res.json({ success: false, message: error.message });
  }
});

// PUT /api/stat/:orgId/:statId/value
router.put("/:orgId/:statId/value", async (req, res) => {
  const { statId, orgId } = req.params;
  const { value, date } = req.body;

  if (!statId || value === undefined || !date || !orgId) {
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

// POST /api/stat/:orgId/delete
router.post("/:orgId/delete", async (req, res) => {
  const { orgId } = req.params;
  const { statIds = [] } = req.body;

  if (!orgId || !statIds.length) {
    return res
      .status(400)
      .json({ success: false, message: "Invalid orgId or statIds." });
  }

  try {
    if (await deleteStatItems(orgId, statIds)) {
      return res.json({ success: true, message: "Success." });
    }
    return res.json({ success: false, message: "Cannot delete fixed stats." });
  } catch (error) {
    res.json({ success: false, message: error.message });
  }
});

export default router;
