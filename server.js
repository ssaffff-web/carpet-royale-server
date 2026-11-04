'use strict';
// Carpet Royale: сервер интернет-лобби и релей.
// Клиенты подключаются по WebSocket. Сервер хранит список комнат (имя, игроки, пинг хозяина)
// и пересылает пакеты Godot-мультиплеера внутри комнаты по звезде: все клиенты <-> хозяин (peer 1).
//
// Текстовые сообщения (JSON):
//   клиент -> сервер: list | ping{ts,rtt} | create{room,name,max} | join{room,name} | state{playing} | kick{id} | leave
//   сервер -> клиент: hello | rooms{rooms:[...]} | pong{ts} | created{id,room} | joined{id,room}
//                      | peer{id} | gone{id} | closed{reason} | error{msg}
// Бинарные пакеты: клиент шлёт [int32 LE кому][данные]; получатель видит [int32 LE от кого][данные].
//   хозяин (id 1): кому > 0 - одному, 0 - всем клиентам, < 0 - всем, кроме -кому.  клиент: всегда хозяину.

const http = require('http');
const { WebSocketServer } = require('ws');

const PORT = parseInt(process.env.PORT || '8080', 10);
const MAX_ROOMS = 200;
const MAX_CLIENTS = 1000;
const MAX_PLAYERS_LIMIT = 8;
const LIST_PUSH_MS = 2000;
const MAX_PAYLOAD = 64 * 1024;
const BACKPRESSURE = 256 * 1024;   // не пересылаем «ненадёжные» пакеты, если у получателя забит буфер

const rooms = new Map();   // roomId -> {id,name,max,host,members:Map<peerId,client>,playing,created,nextId}
let clientSeq = 1;
let roomSeq = 1;

const server = http.createServer((req, res) => {
  if (req.url === '/health') {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end('ok');
    return;
  }
  res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
  res.end(JSON.stringify({ service: 'carpet-royale-server', rooms: rooms.size, clients: wss.clients.size }));
});
const wss = new WebSocketServer({ server, maxPayload: MAX_PAYLOAD });

function sendJson(ws, obj) {
  if (ws.readyState === 1) ws.send(JSON.stringify(obj));
}

function cleanText(s, max, fallback) {
  s = String(s == null ? '' : s).replace(/[\u0000-\u001f<>]/g, ' ').trim();
  if (s.length > max) s = s.slice(0, max);
  return s || fallback;
}

function roomList() {
  const out = [];
  for (const r of rooms.values()) {
    out.push({
      id: r.id,
      name: r.name,
      players: r.members.size,
      max: r.max,
      playing: r.playing,
      hping: r.host ? Math.round(r.host.rtt || 0) : 0,
      age: Math.round((Date.now() - r.created) / 1000),
    });
  }
  return out;
}

function broadcastRooms() {
  const msg = JSON.stringify({ t: 'rooms', rooms: roomList() });
  for (const ws of wss.clients) {
    if (ws.readyState === 1 && ws.browsing) ws.send(msg);
  }
}

function leaveRoom(ws, reason) {
  const r = ws.room;
  if (!r) return;
  ws.room = null;
  if (r.host === ws) {
    // хозяин ушёл: комната закрывается
    rooms.delete(r.id);
    for (const [, m] of r.members) {
      if (m !== ws) {
        m.room = null;
        sendJson(m, { t: 'closed', reason: reason || 'Хост закрыл комнату' });
      }
    }
  } else {
    r.members.delete(ws.peerId);
    if (r.host) {
      sendJson(r.host, { t: 'gone', id: ws.peerId });
    }
  }
  ws.peerId = 0;
}

wss.on('connection', (ws) => {
  if (wss.clients.size > MAX_CLIENTS) {
    ws.close(1013, 'busy');
    return;
  }
  ws.cid = clientSeq++;
  ws.room = null;
  ws.peerId = 0;
  ws.rtt = 0;
  ws.browsing = true;       // получает периодический список комнат, пока не вошёл в комнату
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });
  sendJson(ws, { t: 'hello', rooms: rooms.size });
  sendJson(ws, { t: 'rooms', rooms: roomList() });

  ws.on('message', (data, isBinary) => {
    if (isBinary) return relay(ws, data);
    let m;
    try { m = JSON.parse(data.toString('utf8')); } catch (e) { return; }
    if (!m || typeof m.t !== 'string') return;
    switch (m.t) {
      case 'list':
        sendJson(ws, { t: 'rooms', rooms: roomList() });
        break;
      case 'ping':
        if (typeof m.rtt === 'number' && m.rtt >= 0 && m.rtt < 10000) ws.rtt = m.rtt;
        sendJson(ws, { t: 'pong', ts: m.ts });
        break;
      case 'create': {
        if (ws.room) return sendJson(ws, { t: 'error', msg: 'Вы уже в комнате' });
        if (rooms.size >= MAX_ROOMS) return sendJson(ws, { t: 'error', msg: 'Сервер переполнен, попробуйте позже' });
        const room = {
          id: String(roomSeq++),
          name: cleanText(m.room, 28, 'Игра'),
          max: Math.max(2, Math.min(MAX_PLAYERS_LIMIT, parseInt(m.max, 10) || MAX_PLAYERS_LIMIT)),
          host: ws,
          members: new Map(),
          playing: false,
          created: Date.now(),
          nextId: 2,
        };
        ws.room = room;
        ws.peerId = 1;
        ws.browsing = false;
        room.members.set(1, ws);
        rooms.set(room.id, room);
        sendJson(ws, { t: 'created', id: 1, room: room.id });
        broadcastRooms();
        break;
      }
      case 'join': {
        if (ws.room) return sendJson(ws, { t: 'error', msg: 'Вы уже в комнате' });
        const r = rooms.get(String(m.room));
        if (!r) return sendJson(ws, { t: 'error', msg: 'Комнаты больше нет' });
        if (r.playing) return sendJson(ws, { t: 'error', msg: 'Матч уже идёт' });
        if (r.members.size >= r.max) return sendJson(ws, { t: 'error', msg: 'В комнате нет мест' });
        const id = r.nextId++;
        ws.room = r;
        ws.peerId = id;
        ws.browsing = false;
        r.members.set(id, ws);
        sendJson(ws, { t: 'joined', id, room: r.id });
        sendJson(r.host, { t: 'peer', id });
        broadcastRooms();
        break;
      }
      case 'state':
        if (ws.room && ws.room.host === ws) {
          ws.room.playing = !!m.playing;
          broadcastRooms();
        }
        break;
      case 'kick': {
        // хозяин выгоняет участника
        const r = ws.room;
        if (r && r.host === ws) {
          const t = r.members.get(parseInt(m.id, 10));
          if (t && t !== ws) {
            r.members.delete(t.peerId);
            t.room = null; t.peerId = 0; t.browsing = true;
            sendJson(t, { t: 'closed', reason: cleanText(m.reason, 60, 'Вас исключили') });
            sendJson(ws, { t: 'gone', id: parseInt(m.id, 10) });
            broadcastRooms();
          }
        }
        break;
      }
      case 'leave':
        leaveRoom(ws);
        ws.browsing = true;
        sendJson(ws, { t: 'rooms', rooms: roomList() });
        broadcastRooms();
        break;
    }
  });

  ws.on('close', () => {
    const had = !!ws.room;
    leaveRoom(ws);
    if (had) broadcastRooms();
  });
  ws.on('error', () => {});
});

function relay(ws, buf) {
  const r = ws.room;
  if (!r || buf.length < 6) return;
  const target = buf.readInt32LE(0);
  const body = buf.subarray(4);
  const mode = body[0];     // 0 - без гарантий, 1 - упорядоченный, 2 - надёжный
  const out = Buffer.allocUnsafe(body.length + 4);
  out.writeInt32LE(ws.peerId, 0);
  body.copy(out, 4);
  const sendTo = (c) => {
    if (!c || c.readyState !== 1) return;
    if (mode === 0 && c.bufferedAmount > BACKPRESSURE) return;
    c.send(out, { binary: true });
  };
  if (ws !== r.host) {
    sendTo(r.host);       // клиенты говорят только с хозяином (как в ENet-звезде)
    return;
  }
  if (target > 0) {
    sendTo(r.members.get(target));
  } else {
    const skip = target < 0 ? -target : 0;
    for (const [id, c] of r.members) {
      if (id !== 1 && id !== skip) sendTo(c);
    }
  }
}

// Живость соединений и периодическая рассылка списка
setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) { ws.terminate(); continue; }
    ws.isAlive = false;
    try { ws.ping(); } catch (e) {}
  }
}, 25000);
setInterval(broadcastRooms, LIST_PUSH_MS);

if (require.main === module) {
  server.listen(PORT, () => console.log('carpet-royale-server listening on :' + PORT));
}
module.exports = { server, wss, rooms };
