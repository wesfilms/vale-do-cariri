/* Servidor de salas — Vale do Cariri.
 * Não executa o jogo: só organiza salas e dá uma ORDEM ÚNICA às ações (log).
 * Cada navegador roda uma réplica determinística do jogo e aplica o log na mesma ordem. */
'use strict';
const http = require('http'), fs = require('fs'), path = require('path'), crypto = require('crypto');
const { WebSocketServer } = require('ws');

const PORT = Number(process.env.PORT) || 3000;
const PUBLIC = path.join(__dirname, 'public');
const DATA = process.env.DATA_DIR || path.join(__dirname, 'data');
const MAX_JOG = 10, MAX_SALAS = 200, TTL_MS = 1000 * 60 * 60 * 24 * 3;
const METODOS = new Set(['relogio', 'iniciarEm', 'ir', 'irAteCasa', 'irAteTrabalho', 'visitarLocal', 'checarDocumento', 'esperar', 'interrogar', 'conversarTestemunhas', 'consultar', 'irOndeEsta', 'encerrarDia', 'convocarJuri', 'retirarVotoJuri', 'fecharCaso', 'responderAbordagem', 'acusar', '__reu', '__forcar', '__acu0', 'acusarGrupo', 'resolver']);
const SO_ANFITRIAO = new Set(['acusarGrupo', 'resolver', '__forcar']);
const salas = new Map();

const MIME = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript; charset=utf-8', '.jpg': 'image/jpeg', '.png': 'image/png', '.svg': 'image/svg+xml', '.json': 'application/json', '.ico': 'image/x-icon' };
const http_ = http.createServer((req, res) => {
  let p = decodeURIComponent((req.url || '/').split('?')[0]);
  if (p === '/healthz') { res.writeHead(200); return res.end('ok'); }
  if (p === '/') p = '/index.html';
  const f = path.normalize(path.join(PUBLIC, p));
  if (!f.startsWith(PUBLIC)) { res.writeHead(403); return res.end(); }
  fs.readFile(f, (err, buf) => {
    if (err) { res.writeHead(404); return res.end('não encontrado'); }
    res.writeHead(200, { 'Content-Type': MIME[path.extname(f)] || 'application/octet-stream', 'Cache-Control': 'no-cache' });
    res.end(buf);
  });
});
const wss = new WebSocketServer({ server: http_, maxPayload: 64 * 1024 });

const novoCodigo = () => { const A = 'ABCDEFGHJKLMNPQRSTUVWXYZ'; for (;;) { let c = ''; for (let i = 0; i < 4; i++) c += A[crypto.randomInt(A.length)]; if (!salas.has(c)) return c; } };
const novoToken = () => crypto.randomBytes(12).toString('hex');
const limpaNome = (n) => String(n || '').replace(/[\u0000-\u001f<>]/g, '').trim().slice(0, 24) || 'Investigador';
const pub = (s) => ({ codigo: s.codigo, anfitriao: s.jogadores[0] ? s.jogadores[0].id : null, iniciada: s.iniciada, cfg: s.cfg, jogadores: s.jogadores.map((j) => ({ id: j.id, nome: j.nome, online: !!j.ws })) });
const envia = (ws, o) => { if (ws && ws.readyState === 1) ws.send(JSON.stringify({ ...o, now: Date.now() })); };
const todos = (s, o) => { for (const j of s.jogadores) envia(j.ws, o); };

let salvaT = null;
function persiste() {
  if (salvaT) return; salvaT = setTimeout(() => {
    salvaT = null;
    try {
      fs.mkdirSync(DATA, { recursive: true });
      const out = [...salas.values()].map((s) => ({ codigo: s.codigo, inicioTs: s.inicioTs, cfg: s.cfg, iniciada: s.iniciada, criada: s.criada, ult: s.ult, log: s.log, jogadores: s.jogadores.map((j) => ({ id: j.id, nome: j.nome, token: j.token })) }));
      fs.writeFileSync(path.join(DATA, 'salas.json'), JSON.stringify(out));
    } catch (e) { console.error('persistência falhou:', e.message); }
  }, 1500);
}
function carrega() {
  try {
    const arr = JSON.parse(fs.readFileSync(path.join(DATA, 'salas.json'), 'utf8'));
    for (const s of arr) if (Date.now() - (s.ult || 0) < TTL_MS) salas.set(s.codigo, { ...s, jogadores: s.jogadores.map((j) => ({ ...j, ws: null })) });
    console.log(`${salas.size} sala(s) restaurada(s).`);
  } catch (e) { /* sem dados */ }
}
setInterval(() => { for (const [c, s] of salas) if (Date.now() - s.ult > TTL_MS) salas.delete(c); }, 60 * 60 * 1000).unref();

function cfgValida(c) {
  c = c || {};
  const n = (v, a, b, d) => { v = Number(v); return Number.isFinite(v) ? Math.max(a, Math.min(b, Math.round(v))) : d; };
  return { seed: String(c.seed || 'cidade-001').slice(0, 40), dur: n(c.dur, 60, 180, 180), rodadas: n(c.rodadas, 5, 60, 15), nivel: [1, 2, 3].includes(Number(c.nivel)) ? Number(c.nivel) : '', tipo: String(c.tipo || '').slice(0, 20), eleit: !!c.eleit };
}

wss.on('connection', (ws) => {
  let sala = null, jog = null;
  ws.isAlive = true; ws.on('pong', () => { ws.isAlive = true; });
  const erro = (msg) => envia(ws, { t: 'erro', msg });
  ws.on('message', (raw) => {
    let m; try { m = JSON.parse(raw); } catch (e) { return; }
    if (!m || typeof m !== 'object') return;
    if (m.t === 'ping') return envia(ws, { t: 'pong', c: m.c });
    if (m.t === 'criar') {
      if (salas.size >= MAX_SALAS) return erro('Servidor cheio. Tente mais tarde.');
      const s = { codigo: novoCodigo(), cfg: cfgValida(m.cfg), iniciada: false, criada: Date.now(), ult: Date.now(), log: [], jogadores: [] };
      salas.set(s.codigo, s);
      jog = { id: 'P1', nome: limpaNome(m.nome), token: novoToken(), ws }; s.jogadores.push(jog); sala = s;
      envia(ws, { t: 'entrou', eu: jog.id, token: jog.token, sala: pub(s) }); persiste(); return;
    }
    if (m.t === 'entrar') {
      const s = salas.get(String(m.codigo || '').toUpperCase().trim());
      if (!s) return erro('Sala não encontrada. Confira o código.');
      let j = m.token ? s.jogadores.find((x) => x.token === m.token) : null;
      if (j) { if (j.ws && j.ws !== ws) { try { j.ws.close(); } catch (e) {} } j.ws = ws; }
      else {
        if (s.iniciada) return erro('A partida já começou e você não faz parte dela.');
        if (s.jogadores.length >= MAX_JOG) return erro('A sala está cheia (10 jogadores).');
        j = { id: 'P' + (s.jogadores.length + 1), nome: limpaNome(m.nome), token: novoToken(), ws }; s.jogadores.push(j);
      }
      jog = j; sala = s; s.ult = Date.now();
      envia(ws, { t: 'entrou', eu: j.id, token: j.token, sala: pub(s) });
      if (s.iniciada) envia(ws, { t: 'inicio', cfg: s.cfg, ids: s.jogadores.map((x) => x.id), nomes: s.jogadores.map((x) => x.nome), log: s.log, ts: s.inicioTs });
      todos(s, { t: 'sala', sala: pub(s) }); persiste(); return;
    }
    if (!sala || !jog) return erro('Entre numa sala primeiro.');
    sala.ult = Date.now();
    if (m.t === 'cfg') { if (jog.id !== 'P1' || sala.iniciada) return; sala.cfg = cfgValida(m.cfg); todos(sala, { t: 'sala', sala: pub(sala) }); persiste(); return; }
    if (m.t === 'nome') { if (sala.iniciada) return; jog.nome = limpaNome(m.nome); todos(sala, { t: 'sala', sala: pub(sala) }); persiste(); return; }
    if (m.t === 'iniciar') {
      if (jog.id !== 'P1') return erro('Só quem criou a sala pode iniciar.');
      if (sala.iniciada) return;
      sala.iniciada = true; sala.log = []; sala.inicioTs = Date.now();
      todos(sala, { t: 'inicio', cfg: sala.cfg, ids: sala.jogadores.map((x) => x.id), nomes: sala.jogadores.map((x) => x.nome), log: [], ts: sala.inicioTs }); persiste(); return;
    }
    if (m.t === 'sync') { if (sala.iniciada) envia(ws, { t: 'lote', es: sala.log.slice(Math.max(0, Number(m.desde) || 0)) }); return; }
    if (m.t === 'cmd') {
      if (!sala.iniciada) return;
      if (!METODOS.has(m.m)) return erro('Comando inválido.');
      if (SO_ANFITRIAO.has(m.m) && jog.id !== 'P1') return erro('Só o anfitrião pode fazer isso.');
      if (!Array.isArray(m.a) || JSON.stringify(m.a).length > 8000) return erro('Parâmetros inválidos.');
      const e = { seq: sala.log.length, p: jog.id, m: m.m, a: m.a, tm: Number.isFinite(m.tm) ? m.tm : null, ts: Date.now(), cid: m.cid };
      sala.log.push(e); todos(sala, { t: 'e', e }); persiste(); return;
    }
  });
  ws.on('close', () => { if (sala && jog && jog.ws === ws) { jog.ws = null; todos(sala, { t: 'sala', sala: pub(sala) }); } });
});
setInterval(() => { for (const ws of wss.clients) { if (!ws.isAlive) { ws.terminate(); continue; } ws.isAlive = false; try { ws.ping(); } catch (e) {} } }, 25000).unref();

if (require.main === module) { carrega(); http_.listen(PORT, () => console.log(`Vale do Cariri online em http://localhost:${PORT}`)); }
module.exports = { http_, wss, salas };
