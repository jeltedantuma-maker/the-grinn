'use strict';
const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const PORT = Number(process.env.PORT) || 8080;
const GAME = fs.existsSync(path.join(__dirname, 'public', 'index.html')) ? path.join(__dirname, 'public', 'index.html') : path.join(__dirname, 'index.html');
const MAX_CLIENTS = Number(process.env.MAX_CLIENTS) || 2000;
const MAX_MSG = 96 * 1024;
const RATE = 400;
const ID_RE = /^[A-Za-z0-9_-]{3,64}$/;
const ROOM_PREFIX = 'grin-coop-v1-';

const INJECT = '<script>window.GRINN_SERVER=(location.protocol==="https:"?"wss://":"ws://")+location.host+"/ws";</script>';
let gameHtml = null, gameMtime = 0;
function loadGame() {
  try {
    const st = fs.statSync(GAME);
    if (!gameHtml || st.mtimeMs !== gameMtime) {
      const t = fs.readFileSync(GAME, 'utf8');
      gameHtml = t.includes('<head>') ? t.replace('<head>', '<head>' + INJECT) : INJECT + t;
      gameMtime = st.mtimeMs;
    }
  } catch (e) { gameHtml = null; }
  return gameHtml;
}

const peers = new Map();
const links = new Map();

function send(ws, o) { if (ws && ws.readyState === 1) { try { ws.send(JSON.stringify(o)); } catch (e) { } } }

function publicRooms() {
  const out = [];
  for (const [id, ws] of peers) {
    const m = ws.meta;
    if (!id.startsWith(ROOM_PREFIX) || !m || !m.public || m.inGame) continue;
    out.push({ code: id.slice(ROOM_PREFIX.length), name: m.name, players: m.players, max: m.max, where: m.where });
  }
  return out.slice(0, 100);
}

const server = http.createServer((req, res) => {
  const url = (req.url || '/').split('?')[0];
  res.setHeader('Access-Control-Allow-Origin', '*');
  if (url === '/health') { res.writeHead(200, { 'Content-Type': 'text/plain' }); res.end('ok ' + peers.size); return; }
  if (url === '/rooms') { res.writeHead(200, { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' }); res.end(JSON.stringify(publicRooms())); return; }
  if (url === '/' || url === '/index.html') {
    const html = loadGame();
    if (!html) { res.writeHead(500, { 'Content-Type': 'text/plain' }); res.end('Put the game file next to server.js as index.html'); return; }
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-cache' });
    res.end(html);
    return;
  }
  res.writeHead(404, { 'Content-Type': 'text/plain' }); res.end('not found');
});

const wss = new WebSocketServer({ server, path: '/ws', maxPayload: MAX_MSG });

wss.on('connection', (ws) => {
  if (wss.clients.size > MAX_CLIENTS) { send(ws, { t: 'err', type: 'server-full', message: 'The server is full. Try again later.' }); ws.close(); return; }
  ws.id = null; ws.alive = true; ws.count = 0; ws.windowStart = Date.now(); ws.meta = null; ws.cids = new Set();
  ws.on('pong', () => { ws.alive = true; });
  ws.on('message', (raw) => {
    const now = Date.now();
    if (now - ws.windowStart > 1000) { ws.windowStart = now; ws.count = 0; }
    if (++ws.count > RATE) return;
    let o; try { o = JSON.parse(raw); } catch (e) { return; }
    if (!o || typeof o.t !== 'string') return;
    if (o.t === 'reg') {
      if (ws.id) return;
      if (typeof o.id !== 'string' || !ID_RE.test(o.id)) { send(ws, { t: 'err', type: 'invalid-id' }); return; }
      if (peers.has(o.id)) { send(ws, { t: 'err', type: 'unavailable-id' }); return; }
      ws.id = o.id; peers.set(o.id, ws);
      send(ws, { t: 'open', id: o.id });
      return;
    }
    if (o.t === 'list') { send(ws, { t: 'rooms', rooms: publicRooms() }); return; }
    if (!ws.id) return;
    const cid = typeof o.cid === 'string' && o.cid.length <= 100 ? o.cid : null;
    switch (o.t) {
      case 'meta': {
        const m = o.meta || {};
        ws.meta = { public: !!m.public, name: String(m.name || 'Room').slice(0, 24), players: Math.max(1, Math.min(8, m.players | 0)), max: Math.max(1, Math.min(8, m.max | 0 || 4)), where: String(m.where || '').slice(0, 40), inGame: !!m.inGame };
        return;
      }
      case 'conn': {
        const target = peers.get(o.to);
        if (!cid) return;
        if (!target) { send(ws, { t: 'err', type: 'peer-unavailable', cid }); return; }
        links.set(cid, { a: ws.id, b: o.to }); ws.cids.add(cid); target.cids.add(cid);
        send(target, { t: 'incoming', from: ws.id, cid });
        return;
      }
      case 'accept': case 'data': case 'close': {
        const L = cid && links.get(cid);
        if (!L || (L.a !== ws.id && L.b !== ws.id)) return;
        const other = L.a === ws.id ? L.b : L.a;
        const target = peers.get(other);
        if (o.t === 'accept') send(target, { t: 'opened', cid });
        else if (o.t === 'data') send(target, { t: 'data', from: ws.id, cid, m: o.m });
        else { send(target, { t: 'close', cid }); links.delete(cid); ws.cids.delete(cid); if (target) target.cids.delete(cid); }
        return;
      }
    }
  });
  ws.on('close', () => {
    if (ws.id && peers.get(ws.id) === ws) peers.delete(ws.id);
    for (const cid of ws.cids) {
      const L = links.get(cid); if (!L) continue;
      const other = peers.get(L.a === ws.id ? L.b : L.a);
      send(other, { t: 'close', cid }); if (other) other.cids.delete(cid);
      links.delete(cid);
    }
  });
});

setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.alive) { ws.terminate(); continue; }
    ws.alive = false; try { ws.ping(); } catch (e) { }
  }
}, 25000);

server.listen(PORT, () => {
  console.log('THE GRINN server running on port ' + PORT);
  console.log(loadGame() ? 'Game file found' : 'WARNING: index.html is missing');
});
