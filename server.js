// ============================================================
//  FeTNA Shuttle Tracker — relay server  (v2)
//  Tiny Socket.io relay. NO external database.
//  Live vehicle positions + trip logs + location occupancy,
//  kept in memory (with a best-effort local JSON backup file).
//
//  Local run:
//    npm install
//    npm start         -> http://localhost:3000
//  Then set SERVER_URL in the apps to your deployed https URL.
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
  cors: { origin: "*", methods: ["GET", "POST"] }, // open for the event; fine for 3 days
});

// ---- in-memory state -------------------------------------------------
const vehicles = {};      // id -> { id,label,type,lat,lng,heading,status,ts }
const ownerOf = {};       // socket.id -> vehicleId
let trips = [];           // [{ id, vehicleId, label, type, from, passengers, queue, ts }]
let occupancy = {};       // { "Sheraton Edison": { count, note, ts, queue, queueTs }, ... }

// ---- best-effort persistence (survives a simple restart; NOT guaranteed
//      on free hosts that wipe disk on redeploy -- the admin app also keeps
//      a localStorage backup). -----------------------------------------
const DB_FILE = "./fetna-data.json";
function saveData() {
  try { fs.writeFileSync(DB_FILE, JSON.stringify({ trips, occupancy })); } catch (e) {}
}
try {
  if (fs.existsSync(DB_FILE)) {
    const d = JSON.parse(fs.readFileSync(DB_FILE, "utf8"));
    trips = Array.isArray(d.trips) ? d.trips : [];
    occupancy = d.occupancy || {};
  }
} catch (e) {}

io.on("connection", (socket) => {
  // bring the newcomer up to speed
  socket.emit("snapshot", vehicles);
  socket.emit("tripsSnapshot", trips);
  socket.emit("occupancy", occupancy);

  // ---- live position / status from a driver ----
  socket.on("update", (rec, ack) => {
    if (!rec || !rec.id) { if (typeof ack === "function") ack({ ok: false }); return; }
    rec.ts = Date.now();
    vehicles[rec.id] = { ...(vehicles[rec.id] || {}), ...rec };
    ownerOf[socket.id] = rec.id;
    io.emit("vehicle", vehicles[rec.id]);
    if (typeof ack === "function") ack({ ok: true });
  });

  // ---- a driver logs a completed trip (passengers + queue left) ----
  socket.on("logTrip", (rec, ack) => {
    if (!rec || !rec.from) { if (typeof ack === "function") ack({ ok: false }); return; }
    const trip = {
      id: "t_" + Date.now() + "_" + Math.random().toString(36).slice(2, 7),
      vehicleId: rec.vehicleId || "",
      label: rec.label || "",
      type: rec.type || "",
      from: rec.from,
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
  socket.on("getTrips", () => { socket.emit("tripsSnapshot", trips); socket.emit("occupancy", occupancy); });
  socket.on("ping", (clientTs, ack) => { if (typeof ack === "function") ack(null, Date.now()); });

  // ---- driver phone dropped -> mark their vehicle offline ----
  socket.on("disconnect", () => {
    const id = ownerOf[socket.id];
    if (id && vehicles[id]) {
      vehicles[id].status = "offline";
      vehicles[id].ts = Date.now();
      io.emit("vehicle", vehicles[id]);
    }
    delete ownerOf[socket.id];
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log("FeTNA shuttle relay (v2) listening on :" + PORT));
