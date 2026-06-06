// ============================================================
//  FeTNA Shuttle Tracker — relay server
//  Tiny Socket.io relay. NO DATABASE. Positions live in memory.
//  Run free on Render / Railway / Fly.io / Glitch, or locally.
//
//  Local run:
//    npm install
//    npm start         → http://localhost:3000
//  Then set SERVER_URL in the HTML to your deployed https URL.
// ============================================================
const express = require("express");
const http = require("http");
const { Server } = require("socket.io");

const app = express();

// (optional) serve the tracker page too: drop the HTML in ./public/index.html
app.use(express.static("public"));
app.get("/healthz", (_req, res) => res.send("ok"));

const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: "*", methods: ["GET", "POST"] }, // open for the event; fine for 3 days
});

// In-memory state only — wiped if the server restarts (drivers re-populate it in seconds).
const vehicles = {};      // id -> { id,label,type,route,lat,lng,heading,status,ts }
const ownerOf = {};       // socket.id -> vehicleId (so we can mark offline on disconnect)

io.on("connection", (socket) => {
  // send the newcomer the current picture
  socket.emit("snapshot", vehicles);

  // a driver pushed a position/status
  socket.on("update", (rec, ack) => {
    if (!rec || !rec.id) { if (typeof ack === "function") ack({ ok: false }); return; }
    rec.ts = Date.now();
    vehicles[rec.id] = { ...(vehicles[rec.id] || {}), ...rec };
    ownerOf[socket.id] = rec.id;
    io.emit("vehicle", vehicles[rec.id]);     // broadcast to everyone (incl. sender)
    if (typeof ack === "function") ack({ ok: true });
  });

  // a viewer (re)requests the full snapshot
  socket.on("getSnapshot", () => socket.emit("snapshot", vehicles));

  // connection test from the app's diagnostics screen
  socket.on("ping", (clientTs, ack) => { if (typeof ack === "function") ack(null, Date.now()); });

  // when a driver's phone drops, mark their vehicle offline immediately
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
server.listen(PORT, () => console.log("FeTNA shuttle relay listening on :" + PORT));
