'use strict';
// Проверка протокола: два клиента, комната, список, релей пакетов в обе стороны.
const WebSocket = require('ws');
const { server } = require('./server.js');
const assert = require('assert');

function client(port) {
  const ws = new WebSocket('ws://127.0.0.1:' + port);
  const c = { ws, msgs: [], bins: [], waiters: [] };
  ws.on('message', (d, bin) => {
    if (bin) c.bins.push(Buffer.from(d));
    else c.msgs.push(JSON.parse(d.toString()));
    c.waiters.forEach((w) => w());
  });
  c.open = new Promise((r) => ws.on('open', r));
  c.wait = async (pred, ms = 3000) => {
    const t0 = Date.now();
    for (;;) {
      const f = pred();
      if (f) return f;
      if (Date.now() - t0 > ms) throw new Error('timeout');
      await new Promise((r) => { c.waiters.push(r); setTimeout(r, 50); });
    }
  };
  c.send = (o) => ws.send(JSON.stringify(o));
  return c;
}
function pkt(target, mode, payload) {
  const p = Buffer.from(payload);
  const b = Buffer.alloc(4 + 2 + p.length);
  b.writeInt32LE(target, 0);
  b[4] = mode; b[5] = 0;
  p.copy(b, 6);
  return b;
}

(async () => {
  await new Promise((r) => server.listen(0, r));
  const port = server.address().port;
  const host = client(port), a = client(port), b = client(port);
  await Promise.all([host.open, a.open, b.open]);

  host.send({ t: 'create', room: 'Тест', max: 3 });
  const created = await host.wait(() => host.msgs.find((m) => m.t === 'created'));
  assert.strictEqual(created.id, 1);

  a.send({ t: 'list' });
  const rl = await a.wait(() => a.msgs.filter((m) => m.t === 'rooms' && m.rooms.some((r) => r.name === 'Тест')).pop());
  const room = rl.rooms.find((r) => r.name === 'Тест');
  assert(room && room.players === 1 && room.max === 3);

  a.send({ t: 'join', room: room.id });
  const ja = await a.wait(() => a.msgs.find((m) => m.t === 'joined'));
  assert.strictEqual(ja.id, 2);
  await host.wait(() => host.msgs.find((m) => m.t === 'peer' && m.id === 2));
  b.send({ t: 'join', room: room.id });
  const jb = await b.wait(() => b.msgs.find((m) => m.t === 'joined'));
  assert.strictEqual(jb.id, 3);

  // клиент -> хозяин
  a.ws.send(pkt(1, 2, 'привет'), { binary: true });
  const toHost = await host.wait(() => host.bins[0]);
  assert.strictEqual(toHost.readInt32LE(0), 2);
  assert.strictEqual(toHost.subarray(6).toString(), 'привет');

  // хозяин -> один / все / все кроме
  host.ws.send(pkt(3, 2, 'one'), { binary: true });
  await b.wait(() => b.bins[0]);
  assert.strictEqual(b.bins[0].subarray(6).toString(), 'one');
  assert.strictEqual(a.bins.length, 0);
  host.ws.send(pkt(0, 2, 'all'), { binary: true });
  await a.wait(() => a.bins[0]);
  await b.wait(() => b.bins[1]);
  host.ws.send(pkt(-2, 2, 'except2'), { binary: true });
  await b.wait(() => b.bins[2]);
  await new Promise((r) => setTimeout(r, 150));
  assert.strictEqual(a.bins.length, 1);

  // комната полна / матч идёт
  const c4 = client(port); await c4.open;
  c4.send({ t: 'join', room: room.id });
  await c4.wait(() => c4.msgs.find((m) => m.t === 'error'));
  host.send({ t: 'state', playing: true });
  await new Promise((r) => setTimeout(r, 100));
  a.ws.close();
  await host.wait(() => host.msgs.find((m) => m.t === 'gone' && m.id === 2));

  // хозяин уходит - комната закрывается
  host.ws.close();
  await b.wait(() => b.msgs.find((m) => m.t === 'closed'));
  console.log('OK: протокол лобби и релея работает');
  process.exit(0);
})().catch((e) => { console.error('FAIL', e); process.exit(1); });
