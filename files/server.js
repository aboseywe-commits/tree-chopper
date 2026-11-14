// قاطع الأشجار — سيرفر الأونلاين (يخدم اللعبة + يمرّر حالة اللاعبين بينهم)
// التشغيل:   npm install   ثم   node server.js
const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const MAX_PLAYERS = 4;
const MAX_PRESENCE_BYTES = 8192;

const server = http.createServer((req, res) => {
  const url = req.url.split('?')[0];
  if (url === '/' || url === '/index.html') {
    res.writeHead(200, { 'content-type': 'text/html; charset=utf-8', 'cache-control': 'no-cache' });
    fs.createReadStream(path.join(__dirname, 'index.html')).pipe(res);
  } else {
    res.writeHead(404);
    res.end('not found');
  }
});

const wss = new WebSocketServer({ server, path: '/ws' });
const rooms = new Map(); // code -> Map(peerId -> { ws, presence })
let counter = 0;

function send(ws, msg) {
  if (ws.readyState === 1) ws.send(JSON.stringify(msg));
}
function broadcast(room, exceptId, msg) {
  const data = JSON.stringify(msg);
  for (const [id, p] of room) if (id !== exceptId && p.ws.readyState === 1) p.ws.send(data);
}

wss.on('connection', (ws) => {
  const id = 'p' + (++counter).toString(36) + Math.random().toString(36).slice(2, 6);
  let code = null;
  let room = null;

  ws.on('message', (raw) => {
    if (raw.length > 16384) return;
    let m;
    try { m = JSON.parse(raw); } catch { return; }

    if (m.t === 'join' && !room) {
      const c = String(m.room || '').toLowerCase();
      if (!/^[a-z0-9]{3,12}$/.test(c)) return ws.close();
      let r = rooms.get(c);
      if (!r) { r = new Map(); rooms.set(c, r); }
      if (r.size >= MAX_PLAYERS) { send(ws, { t: 'full' }); return ws.close(); }
      code = c; room = r;
      r.set(id, { ws, presence: {} });
      send(ws, { t: 's', you: id, peers: [...r].map(([pid, p]) => ({ peer: pid, presence: p.presence })) });
      broadcast(r, id, { t: 'u', peer: id, presence: {} });
    } else if (m.t === 'p' && room) {
      const me = room.get(id);
      if (!me || !m.patch || typeof m.patch !== 'object') return;
      const next = { ...me.presence };
      for (const k of Object.keys(m.patch)) {
        if (m.patch[k] === null) delete next[k];
        else next[k] = m.patch[k];
      }
      if (JSON.stringify(next).length > MAX_PRESENCE_BYTES) return;
      me.presence = next;
      broadcast(room, id, { t: 'u', peer: id, presence: next });
    }
  });

  ws.on('close', () => {
    if (!room) return;
    room.delete(id);
    broadcast(room, id, { t: 'l', peer: id });
    if (room.size === 0) rooms.delete(code);
  });
});

server.listen(PORT, () => {
  console.log('اللعبة شغّالة على:  http://localhost:' + PORT);
  console.log('لصديقك على نفس الشبكة افتح:  http://<عنوان-جهازك>:' + PORT);
});
