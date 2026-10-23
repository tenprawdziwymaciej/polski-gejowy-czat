const express = require("express");
const http = require("http");
const { Server } = require("socket.io");

const app = express();
const server = http.createServer(app);
const io = new Server(server);

const PORT = process.env.PORT || 3000;
const waiting = [];
const partners = new Map();

app.use(express.static("public"));

function removeFromWaiting(id) {
  const index = waiting.indexOf(id);
  if (index !== -1) waiting.splice(index, 1);
}

function disconnectPartner(id, notify = true) {
  const partnerId = partners.get(id);
  partners.delete(id);

  if (partnerId) {
    partners.delete(partnerId);
    const partnerSocket = io.sockets.sockets.get(partnerId);
    if (partnerSocket && notify) partnerSocket.emit("partner-left");
  }
}

function findPartner(socket) {
  removeFromWaiting(socket.id);
  disconnectPartner(socket.id);

  while (waiting.length) {
    const candidateId = waiting.shift();
    const candidate = io.sockets.sockets.get(candidateId);
    if (!candidate || candidate.id === socket.id || partners.has(candidateId)) continue;

    partners.set(socket.id, candidateId);
    partners.set(candidateId, socket.id);

    socket.emit("matched", { initiator: true });
    candidate.emit("matched", { initiator: false });
    return;
  }

  waiting.push(socket.id);
  socket.emit("waiting");
}

io.on("connection", (socket) => {
  socket.on("find-partner", () => findPartner(socket));

  socket.on("signal", (payload) => {
    const partnerId = partners.get(socket.id);
    if (!partnerId || !payload || !["offer", "answer", "ice-candidate"].includes(payload.type)) return;
    const partnerSocket = io.sockets.sockets.get(partnerId);
    if (partnerSocket) partnerSocket.emit("signal", payload);
  });

  socket.on("chat-message", (message) => {
    const partnerId = partners.get(socket.id);
    const partnerSocket = partnerId && io.sockets.sockets.get(partnerId);
    if (!partnerSocket || typeof message !== "string") return;
    const clean = message.trim().slice(0, 1000);
    if (clean) partnerSocket.emit("chat-message", clean);
  });

  socket.on("next-partner", () => {
    removeFromWaiting(socket.id);
    disconnectPartner(socket.id);
    findPartner(socket);
  });

  socket.on("report-partner", () => {
    // Starter placeholder: reports are logged only on the server.
    // Before public launch, store reports safely and add moderation workflows.
    console.warn(`Report received from socket ${socket.id} at ${new Date().toISOString()}`);
    socket.emit("report-received");
  });

  socket.on("disconnect", () => {
    removeFromWaiting(socket.id);
    disconnectPartner(socket.id);
  });
});

server.listen(PORT, () => {
  console.log(`Random Video Chat running at http://localhost:${PORT}`);
});
