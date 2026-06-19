// ============================================================
//  FeTNA Shuttle Tracker — relay server  (v3)
//  Tiny Socket.io relay. NO external database.
//  Carries: live vehicle positions, custom bus names, on-board
//  counts, per-trip ridership, location occupancy, and a full
//  action log. Kept in memory with a best-effort JSON backup file.
//
//  Local run:  npm install  &&  npm start   -> http://localhost:3000
// ============================================================
const express = require("express");
const http = require("http");
const fs = require("fs");
const { Server } = require("socket.io");

const app = express();
app.use(express.static("public"));
app.get("/healthz", (_req, res) => res.send("ok"));

const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: "*", methods: ["GET", "POST"] },
});

// ---- in-memory state -------------------------------------------------
const vehicles = {};      // id -> { id,label,name,type,route,lat,lng,heading,status,onboard,ts }
const ownerOf = {};       // socket.id -> vehicleId
let names = {};           // id -> custom display name (set by driver or admin)
let trips = [];           // [{ id, vehicleId, label, from, to, passengers, queue, ts }]
let occupancy = {};       // { location: { count, note, ts, queue, queueTs } }
let actions = [];         // [{ id, vehicleId, label, type, from, to, people, ts }]
// roster = the editable fleet list (admin manages it; drivers pick from it)
const DEFAULT_ROSTER = [
  { id:"A1", label:"A1 · Sheraton Express", type:"bus", route:"A" },
  { id:"A2", label:"A2 · Sheraton Express", type:"bus", route:"A" },
  { id:"A3", label:"A3 · Surge / Flex",     type:"bus", route:"A" },
  { id:"B1", label:"B1 · HGI + Courtyard",  type:"bus", route:"B" },
  { id:"B2", label:"B2 · HGI + Courtyard",  type:"bus", route:"B" },
  { id:"SB", label:"SB · Standby / ADA",    type:"bus", route:"C" },
];
let roster = DEFAULT_ROSTER.slice();

// ---- best-effort persistence ----------------------------------------
const DB_FILE = "./fetna-data.json";
function saveData() {
  try { fs.writeFileSync(DB_FILE, JSON.stringify({ trips, occupancy, names, actions, roster })); } catch (e) {}
}
try {
  if (fs.existsSync(DB_FILE)) {
    const d = JSON.parse(fs.readFileSync(DB_FILE, "utf8"));
    trips = Array.isArray(d.trips) ? d.trips : [];
    occupancy = d.occupancy || {};
    names = d.names || {};
    actions = Array.isArray(d.actions) ? d.actions : [];
    if (Array.isArray(d.roster) && d.roster.length) roster = d.roster;
  }
} catch (e) {}

io.on("connection", (socket) => {
  socket.emit("snapshot", vehicles);
  socket.emit("roster", roster);
  socket.emit("names", names);
  socket.emit("tripsSnapshot", trips);
  socket.emit("occupancy", occupancy);
  socket.emit("actionsSnapshot", actions);

  // ---- admin edits the fleet roster (add / edit / remove vehicles) ----
  socket.on("saveRoster", (list, ack) => {
    if (!Array.isArray(list)) { if (typeof ack === "function") ack({ ok: false }); return; }
    // sanitize
    const seen = {};
    roster = list
      .filter(r => r && r.id)
      .map(r => ({
        id: String(r.id).slice(0, 12),
        label: String(r.label || r.id).slice(0, 40),
        type: (r.type === "van" ? "van" : "bus"),
        route: (["A", "B", "C"].includes(r.route) ? r.route : "A"),
      }))
      .filter(r => (seen[r.id] ? false : (seen[r.id] = true)));
    // drop any live vehicle that's no longer in the roster
    Object.keys(vehicles).forEach(id => { if (!seen[id]) delete vehicles[id]; });
    saveData();
    io.emit("roster", roster);
    io.emit("snapshot", vehicles);
    if (typeof ack === "function") ack({ ok: true });
  });
  socket.on("getRoster", () => socket.emit("roster", roster));

  // ---- live position / status / onboard count from a driver ----
  socket.on("update", (rec, ack) => {
    if (!rec || !rec.id) { if (typeof ack === "function") ack({ ok: false }); return; }
    rec.ts = Date.now();
    if (names[rec.id]) rec.name = names[rec.id];           // keep custom name attached
    vehicles[rec.id] = { ...(vehicles[rec.id] || {}), ...rec };
    ownerOf[socket.id] = rec.id;
    io.emit("vehicle", vehicles[rec.id]);
    if (typeof ack === "function") ack({ ok: true });
  });

  // ---- custom bus name (from driver OR admin) ----
  socket.on("setName", (rec, ack) => {
    if (!rec || !rec.id) { if (typeof ack === "function") ack({ ok: false }); return; }
    const nm = (rec.name || "").toString().slice(0, 40);
    names[rec.id] = nm;
    if (vehicles[rec.id]) { vehicles[rec.id].name = nm; io.emit("vehicle", vehicles[rec.id]); }
    saveData();
    io.emit("names", names);
    if (typeof ack === "function") ack({ ok: true });
  });

  // ---- a driver logs a completed trip (from -> to, people, queue) ----
  socket.on("logTrip", (rec, ack) => {
    if (!rec || !rec.from) { if (typeof ack === "function") ack({ ok: false }); return; }
    const trip = {
      id: "t_" + Date.now() + "_" + Math.random().toString(36).slice(2, 7),
      vehicleId: rec.vehicleId || "",
      label: rec.label || "",
      from: rec.from,
      to: rec.to || "",
      passengers: Number(rec.passengers) || 0,
      queue: (rec.queue === "" || rec.queue == null) ? null : Number(rec.queue),
      ts: Date.now(),
    };
    trips.push(trip);
    if (trips.length > 20000) trips = trips.slice(-20000);
    if (trip.queue != null) {
      occupancy[trip.from] = { ...(occupancy[trip.from] || {}), queue: trip.queue, queueTs: trip.ts };
      io.emit("occupancy", occupancy);
    }
    saveData();
    io.emit("trip", trip);
    if (typeof ack === "function") ack({ ok: true, id: trip.id });
  });

  // ---- generic action log entry (in_service, break, trip_start, trip_stop, name) ----
  socket.on("logAction", (rec, ack) => {
    if (!rec || !rec.type) { if (typeof ack === "function") ack({ ok: false }); return; }
    const a = {
      id: "a_" + Date.now() + "_" + Math.random().toString(36).slice(2, 7),
      vehicleId: rec.vehicleId || "",
      label: rec.label || "",
      type: rec.type,
      from: rec.from || "",
      to: rec.to || "",
      people: (rec.people == null || rec.people === "") ? null : Number(rec.people),
      ts: Date.now(),
    };
    actions.push(a);
    if (actions.length > 20000) actions = actions.slice(-20000);
    saveData();
    io.emit("action", a);
    if (typeof ack === "function") ack({ ok: true, id: a.id });
  });

  // ---- admin sets how many people are housed/present at a location ----
  socket.on("setOccupancy", (rec, ack) => {
    if (!rec || !rec.location) { if (typeof ack === "function") ack({ ok: false }); return; }
    occupancy[rec.location] = {
      ...(occupancy[rec.location] || {}),
      count: Number(rec.count) || 0,
      note: rec.note || "",
      ts: Date.now(),
    };
    saveData();
    io.emit("occupancy", occupancy);
    if (typeof ack === "function") ack({ ok: true });
  });

  // ---- snapshots / diagnostics ----
  socket.on("getSnapshot", () => socket.emit("snapshot", vehicles));
  socket.on("getTrips", () => {
    socket.emit("tripsSnapshot", trips);
    socket.emit("occupancy", occupancy);
    socket.emit("actionsSnapshot", actions);
    socket.emit("names", names);
  });
  socket.on("ping", (clientTs, ack) => { if (typeof ack === "function") ack(null, Date.now()); });

  // ---- driver phone dropped -> mark their vehicle offline ----
  socket.on("disconnect", () => {
    const id = ownerOf[socket.id];
    if (id && vehicles[id]) {
      const wasActive = vehicles[id].status && vehicles[id].status !== "offline";
      if (wasActive) {
        // Driver dropped without logging out (lost wifi / app killed). Show "lost signal"
        // for a window, then fall back to offline.
        vehicles[id].status = "lost";
        vehicles[id].ts = Date.now();
        io.emit("vehicle", vehicles[id]);
        const LOST_MS = 4 * 60 * 1000;
        setTimeout(() => {
          if (vehicles[id] && vehicles[id].status === "lost") {
            vehicles[id].status = "offline";
            vehicles[id].ts = Date.now();
            io.emit("vehicle", vehicles[id]);
          }
        }, LOST_MS);
      } else {
        vehicles[id].status = "offline";
        vehicles[id].ts = Date.now();
        io.emit("vehicle", vehicles[id]);
      }
    }
    delete ownerOf[socket.id];
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log("FeTNA shuttle relay (v3) listening on :" + PORT));
