// Servidor multiplayer do RPG do Mago.
// Repassa presença (posição dos jogadores), chat e eventos do mundo (criaturas, mortes)
// entre todos os jogadores conectados, por WebSocket.
// + Salvamento de contas/progresso na nuvem (accounts.js).
const http = require('http');
const { WebSocketServer } = require('ws');
const accounts = require('./accounts');

const PORT = process.env.PORT || 8080;
const MAX_PLAYERS = 60;
const PID_RE = /^[a-z0-9]{4,24}$/;

const clients = new Map(); // pid -> { ws, pres, tokens, last }

const server = http.createServer((req, res) => {
  if (accounts.handle(req, res)) return; // rotas /acc/get e /acc/put
  res.writeHead(200, { 'content-type': 'text/plain; charset=utf-8', 'cache-control': 'no-store' });
  res.end('RPG do Mago server OK\nonline: ' + clients.size + '\n');
});

const wss = new WebSocketServer({ server, perMessageDeflate: false, maxPayload: 70000 });

function sendRaw(ws, str) {
  if (ws.readyState === 1) { try { ws.send(str); } catch (e) {} }
}

// Envia para todos (menos "except"). "droppable" = pode pular quem está com a fila cheia.
function broadcast(str, except, droppable) {
  for (const [pid, c] of clients) {
    if (pid === except) continue;
    if (c.ws.readyState !== 1) continue;
    if (c.ws.bufferedAmount > 1e6) {
      if (droppable) continue;
      if (c.ws.bufferedAmount > 8e6) { try { c.ws.terminate(); } catch (e) {} continue; }
    }
    try { c.ws.send(str); } catch (e) {}
  }
}

wss.on('connection', (ws, req) => {
  if (req.socket && req.socket.setNoDelay) req.socket.setNoDelay(true);
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });
  let pid = null;
  let tokens = 300, last = Date.now();

  ws.on('message', (data, isBinary) => {
    if (isBinary) return;
    // limite de mensagens por segundo (anti-flood)
    const now = Date.now();
    tokens = Math.min(300, tokens + (now - last) * 0.12); last = now; // ~120 msg/s
    if (tokens < 1) return;
    tokens -= 1;

    const str = data.toString();
    let o;
    try { o = JSON.parse(str); } catch (e) { return; }
    if (!o || typeof o !== 'object') return;

    if (o.t === 'hi') {
      if (pid) return;
      if (typeof o.p !== 'string' || !PID_RE.test(o.p)) return ws.close(1008, 'pid');
      if (clients.size >= MAX_PLAYERS && !clients.has(o.p)) return ws.close(1013, 'cheio');
      const old = clients.get(o.p);
      if (old && old.ws !== ws) { try { old.ws.terminate(); } catch (e) {} }
      pid = o.p;
      clients.set(pid, { ws, pres: null });
      const roster = [];
      for (const [id, c] of clients) if (id !== pid && c.pres) roster.push([id, c.pres]);
      sendRaw(ws, JSON.stringify({ t: 'roster', a: roster }));
      return;
    }
    if (!pid) return;

    if (o.t === 'p') {
      if (str.length > 700 || !o.d || typeof o.d !== 'object') return;
      const c = clients.get(pid); if (c) c.pres = o.d;
      broadcast(JSON.stringify({ t: 'p', p: pid, d: o.d }), pid, true);
    } else if (o.t === 'chat') {
      if (str.length > 700) return;
      broadcast(JSON.stringify({ t: 'chat', p: pid, d: o.d }), null, false); // inclui quem enviou
    } else if (o.t === 'w') {
      if (str.length > 60000 || !o.d || typeof o.d !== 'object') return;
      broadcast(JSON.stringify({ t: 'w', p: pid, d: o.d }), pid, o.d.k === 'st' || o.d.k === 'pg' || o.d.k === 'pn');
    }
  });

  ws.on('close', () => {
    if (!pid) return;
    const c = clients.get(pid);
    if (c && c.ws === ws) {
      clients.delete(pid);
      broadcast(JSON.stringify({ t: 'gone', p: pid }), pid, false);
    }
  });
  ws.on('error', () => {});
});

// mantém as conexões vivas e derruba as mortas
setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) { try { ws.terminate(); } catch (e) {} continue; }
    ws.isAlive = false;
    try { ws.ping(); } catch (e) {}
  }
}, 25000);

server.listen(PORT, () => console.log('RPG do Mago server na porta ' + PORT));
