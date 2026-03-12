const express = require("express");
const cors = require("cors");
const fs = require("fs");
const path = require("path");

const app = express();
// DEPRECATED: This server is superseded by DPR/dprengine.js which provides
// all these endpoints plus the TCP EA bridge. Do NOT run both — only start
// dprengine.js. Port changed to 3099 to avoid silently stealing 3001 if
// this file is accidentally launched.
const PORT = 3099;
const DB_PATH = path.join(__dirname, "db.json");

app.use(cors());
app.use(express.json());

function readDb() {
  return JSON.parse(fs.readFileSync(DB_PATH, "utf-8"));
}

function normalizeMasterMode(value) {
  return String(value || "").toLowerCase() === "automated" ? "automated" : "manual";
}

function writeDb(data) {
  fs.writeFileSync(DB_PATH, JSON.stringify(data, null, 2), "utf-8");
}

// GET all data
app.get("/api/data", (req, res) => {
  res.json(readDb());
});

// POST add master account
app.post("/api/masters", (req, res) => {
  const db = readDb();
  const newMaster = { ...req.body, mode: normalizeMasterMode(req.body?.mode) };
  db.masterAccounts.push(newMaster);
  writeDb(db);
  res.json(newMaster);
});

// DELETE master account (and its slaves)
app.delete("/api/masters/:id", (req, res) => {
  const db = readDb();
  db.masterAccounts = db.masterAccounts.filter((m) => m.id !== req.params.id);
  db.slaveAccounts = db.slaveAccounts.filter((s) => s.masterId !== req.params.id);
  writeDb(db);
  res.json({ success: true });
});

// PUT update master account
app.put("/api/masters/:id", (req, res) => {
  const db = readDb();
  const idx = db.masterAccounts.findIndex((m) => m.id === req.params.id);
  if (idx === -1) return res.status(404).json({ error: "Not found" });
  const hasMode = Object.prototype.hasOwnProperty.call(req.body || {}, "mode");
  db.masterAccounts[idx] = {
    ...db.masterAccounts[idx],
    ...req.body,
    mode: hasMode ? normalizeMasterMode(req.body?.mode) : normalizeMasterMode(db.masterAccounts[idx]?.mode),
    lastUpdated: new Date().toISOString().split("T")[0],
  };
  writeDb(db);
  res.json(db.masterAccounts[idx]);
});

// POST add slave account
app.post("/api/slaves", (req, res) => {
  const db = readDb();
  const newSlave = req.body;
  db.slaveAccounts.push(newSlave);
  writeDb(db);
  res.json(newSlave);
});

// PUT update slave account
app.put("/api/slaves/:id", (req, res) => {
  const db = readDb();
  const idx = db.slaveAccounts.findIndex((s) => s.id === req.params.id);
  if (idx === -1) return res.status(404).json({ error: "Not found" });
  db.slaveAccounts[idx] = { ...db.slaveAccounts[idx], ...req.body, lastUpdated: new Date().toISOString().split("T")[0] };
  writeDb(db);
  res.json(db.slaveAccounts[idx]);
});

// DELETE slave account
app.delete("/api/slaves/:id", (req, res) => {
  const db = readDb();
  db.slaveAccounts = db.slaveAccounts.filter((s) => s.id !== req.params.id);
  writeDb(db);
  res.json({ success: true });
});

// POST activate master (allows multiple active masters)
app.post("/api/masters/:id/activate", (req, res) => {
  const db = readDb();
  const idx = db.masterAccounts.findIndex((m) => m.id === req.params.id);
  if (idx === -1) return res.status(404).json({ error: "Not found" });
  db.masterAccounts[idx] = {
    ...db.masterAccounts[idx],
    mode: normalizeMasterMode(db.masterAccounts[idx]?.mode),
    status: "active",
    lastUpdated: new Date().toISOString().split("T")[0],
  };
  writeDb(db);
  res.json({ success: true });
});

app.listen(PORT, () => {
  console.log(`DPR API running on port ${PORT}`);
});
