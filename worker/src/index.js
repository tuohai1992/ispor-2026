// ISPOR Europe 2026 team planner – votes and Ask backend.
import { DurableObject } from 'cloudflare:workers';

const DATA_URL = 'https://tuohai1992.github.io/ispor-2026/data/sessions.json';
const MODEL = 'claude-haiku-5-5';
const MAX_Q = 600;
const MAX_VOTES = 80;
const PEOPLE = ['Kun', 'Fabian', 'Jieyi'];
const ITEM_URL = 'https://ispor2026.abundanceds.com/api/items/';
const details = new Map();

async function detail(env, id) {
  if (details.has(id)) return details.get(id);
  const r = await fetch((env.ITEM_URL || ITEM_URL) + id, { cf: { cacheTtl: 86400, cacheEverything: true } });
  if (!r.ok) throw new Error('upstream ' + r.status);
  const j = await r.json();
  const it = j.item || j;
  const out = {
    description: String(it.description || '').replace(/^Separate registration required\.\s*/, '').trim(),
    speakers: (Array.isArray(it.speakers) ? it.speakers : []).map(s => ({ name: String(s.name || ''), affiliation: String(s.affiliation || ''), role: String(s.role || '') })).filter(s => s.name)
  };
  details.set(id, out);
  return out;
}

let cache = { at: 0, data: null, ids: null, ctx: null };

async function data(env) {
  if (cache.data && Date.now() - cache.at < 3600e3) return cache;
  const d = await (await fetch((env && env.DATA_URL) || DATA_URL, { cf: { cacheTtl: 600 } })).json();
  const day = s => new Date(s.date + 'T12:00:00Z').toLocaleDateString('en-GB', { weekday: 'short', day: 'numeric', month: 'short', timeZone: 'UTC' });
  const lines = d.sessions
    .filter(s => s.format !== 'General Meeting')
    .map(s => `${s.id} | ${day(s)} ${s.start}-${s.end} | ${s.format} | ${s.tracks.join(', ')} | ${s.title} | ${s.speakers} | ${s.summary}`)
    .join('\n');
  const ctx = `You help a pharmaceutical international Pricing & Market Access (iPMA) team, working on ophthalmology medicines across Europe and Asia, choose sessions at ISPOR Europe 2026 (Vienna, 9-11 November 2026, all times CET).
Rules:
- Use ONLY the catalogue below. Never invent sessions, rooms, speakers or ids.
- Prefer sessions that fit the question. If two strong picks run at the same time, say so in their reasons.
- If the question is not about choosing or understanding sessions, say so in one sentence and return no sessions.
- "answer": 2-5 sentences, in the same language as the question, no headings.
- "sessions": at most 8, most relevant first, each with one short reason.
- Never write session ids in "answer" or "why"; refer to other sessions by short title or day and time.
- Keep every string on one line and escape double quotes inside strings.
Reply with only JSON: {"answer": string, "sessions": [{"id": "3699043", "why": string}]}

CATALOGUE (id | day time | format | tracks | title | speakers | summary)
${lines}`;
  cache = { at: Date.now(), data: d, ids: new Set(d.sessions.map(s => s.id)), ctx };
  return cache;
}

export class VoteStore extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    this.sql = ctx.storage.sql;
    ctx.setWebSocketAutoResponse(new WebSocketRequestResponsePair('ping', 'pong'));
    this.sql.exec('CREATE TABLE IF NOT EXISTS votes (voter TEXT NOT NULL, session TEXT NOT NULL, at INTEGER NOT NULL, PRIMARY KEY (voter, session))');
    this.sql.exec('CREATE TABLE IF NOT EXISTS plan (session TEXT NOT NULL, person TEXT NOT NULL, at INTEGER NOT NULL, PRIMARY KEY (session, person))');
  }
  state(voter) {
    const counts = {};
    for (const r of this.sql.exec('SELECT session, COUNT(*) AS n FROM votes GROUP BY session').toArray()) counts[r.session] = r.n;
    const mine = voter ? this.sql.exec('SELECT session FROM votes WHERE voter = ?', voter).toArray().map(r => r.session) : [];
    return { counts, mine };
  }
  // live updates: browsers hold a hibernatable WebSocket on the plan object
  async fetch(req) {
    const pair = new WebSocketPair();
    this.ctx.acceptWebSocket(pair[1]);
    return new Response(null, { status: 101, webSocket: pair[0] });
  }
  webSocketMessage(ws, msg) { if (msg === 'ping') ws.send('pong'); }
  webSocketClose(ws, code) { try { ws.close(code, 'bye'); } catch (e) {} }
  broadcast(msg) {
    const s = JSON.stringify(msg);
    for (const ws of this.ctx.getWebSockets()) { try { ws.send(s); } catch (e) {} }
  }
  planState() {
    const plan = {};
    for (const r of this.sql.exec('SELECT session, person FROM plan ORDER BY at').toArray()) (plan[r.session] = plan[r.session] || []).push(r.person);
    return { plan };
  }
  vote(voter, session, on) {
    if (on) {
      const n = this.sql.exec('SELECT COUNT(*) AS n FROM votes WHERE voter = ?', voter).one().n;
      if (n < MAX_VOTES) this.sql.exec('INSERT OR IGNORE INTO votes (voter, session, at) VALUES (?, ?, ?)', voter, session, Date.now());
    } else {
      this.sql.exec('DELETE FROM votes WHERE voter = ? AND session = ?', voter, session);
    }
    return this.state(voter);
  }
  clearVoter(voter) {
    this.sql.exec('DELETE FROM votes WHERE voter = ?', voter);
    return this.state(voter);
  }
  clearSession(session) {
    this.sql.exec('DELETE FROM votes WHERE session = ?', session);
    return this.state(null);
  }
  planChanges(changes, wipe) {
    if (wipe) this.sql.exec('DELETE FROM plan');
    const now = Date.now();
    for (const c of changes) {
      if (c.on) this.sql.exec('INSERT OR IGNORE INTO plan (session, person, at) VALUES (?, ?, ?)', c.session, c.person, now);
      else this.sql.exec('DELETE FROM plan WHERE session = ? AND person = ?', c.session, c.person);
    }
    return this.planState();
  }
  reset() {
    this.sql.exec('DELETE FROM votes');
    return this.state(null);
  }
}

function cors(env, origin) {
  const allowed = (env.ALLOWED_ORIGINS || 'https://tuohai1992.github.io').split(',').map(s => s.trim());
  return {
    'Access-Control-Allow-Origin': allowed.includes(origin) ? origin : allowed[0],
    'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type',
    'Vary': 'Origin'
  };
}

const json = (body, status, h) => new Response(JSON.stringify(body), { status, headers: { 'Content-Type': 'application/json', 'Cache-Control': 'no-store', ...h } });

function parseJSON(text) {
  text = text.replace(/^\s*```(?:json)?\s*|\s*```\s*$/g, '');
  try { return JSON.parse(text); } catch (e) {}
  const a = text.indexOf('{'), b = text.lastIndexOf('}');
  if (a >= 0 && b > a) { try { return JSON.parse(text.slice(a, b + 1)); } catch (e) {} }
  // last resort: pull fields out of malformed JSON
  const unq = s => { try { return JSON.parse('"' + s.replace(/\n/g, '\\n') + '"'); } catch (e) { return s; } };
  const ans = text.match(/"answer"\s*:\s*"((?:[^"\\]|\\.)*)"/);
  const sessions = [...text.matchAll(/"id"\s*:\s*"?(\d{6,8})"?\s*,\s*"why"\s*:\s*"((?:[^"\\]|\\.)*)"/g)].map(m => ({ id: m[1], why: unq(m[2]) }));
  return { answer: ans ? unq(ans[1]) : (sessions.length ? '' : text.replace(/```(json)?/g, '').trim()), sessions };
}

function sameKey(a, b) {
  if (typeof a !== 'string' || typeof b !== 'string' || a.length !== b.length) return false;
  let d = 0;
  for (let i = 0; i < a.length; i++) d |= a.charCodeAt(i) ^ b.charCodeAt(i);
  return d === 0;
}

const validVoter = v => typeof v === 'string' && /^[A-Za-z0-9-]{8,64}$/.test(v);

async function ask(req, env, h, ip) {
  if (env.ASK_LIMITER && !(await env.ASK_LIMITER.limit({ key: ip })).success) return json({ error: 'rate_limited' }, 429, h);
  if (!env.ANTHROPIC_API_KEY) return json({ error: 'ask_not_configured' }, 503, h);
  let body;
  try { body = await req.json(); } catch (e) { return json({ error: 'Bad request' }, 400, h); }
  const q = String(body.question || '').trim();
  if (!q || q.length > MAX_Q) return json({ error: 'Question must be 1–600 characters' }, 400, h);
  const { ctx, ids } = await data(env);

  const r = await fetch('https://api.anthropic.com/v1/messages', {
    method: 'POST',
    headers: { 'x-api-key': env.ANTHROPIC_API_KEY, 'anthropic-version': '2023-06-01', 'content-type': 'application/json' },
    body: JSON.stringify({
      model: env.MODEL || MODEL,
      max_tokens: 2000,
      system: [{ type: 'text', text: ctx, cache_control: { type: 'ephemeral' } }],
      messages: [{ role: 'user', content: q }]
    })
  });
  if (!r.ok) return json({ error: 'upstream_error', status: r.status }, 502, h);
  const out = await r.json();
  const text = (out.content || []).filter(c => c.type === 'text').map(c => c.text).join('');
  const p = parseJSON(text);
  const sessions = (Array.isArray(p.sessions) ? p.sessions : [])
    .filter(s => s && ids.has(String(s.id)))
    .slice(0, 8)
    .map(s => ({ id: String(s.id), why: String(s.why || '') }));
  return json({ answer: String(p.answer || ''), sessions }, 200, h);
}

export default {
  async fetch(req, env, ctx) {
    const origin = req.headers.get('Origin') || '';
    const h = cors(env, origin);
    if (req.method === 'OPTIONS') return new Response(null, { headers: h });
    const url = new URL(req.url);

    // full session text; public programme data, so no origin check
    if (url.pathname === '/details' && req.method === 'GET') {
      const id = url.searchParams.get('id') || '';
      const { ids } = await data(env);
      if (!ids.has(id)) return json({ error: 'Not found' }, 404, h);
      try { return new Response(JSON.stringify(await detail(env, id)), { headers: { 'Content-Type': 'application/json', 'Cache-Control': 'public, max-age=3600', ...h } }); }
      catch (e) { return json({ error: 'upstream_error' }, 502, h); }
    }

    if (origin !== h['Access-Control-Allow-Origin']) return json({ error: 'Origin not allowed' }, 403, h);
    const ip = req.headers.get('CF-Connecting-IP') || 'unknown';
    const store = env.VOTES.get(env.VOTES.idFromName('ispor-europe-2026'));
    // the plan lives in its own object so it never touches the votes
    const planStore = env.VOTES.get(env.VOTES.idFromName('ispor-europe-2026-plan'));
    const withPlan = async o => ({ ...o, ...(await planStore.planState()) });
    const push = msg => ctx.waitUntil(planStore.broadcast(msg).catch(() => {}));
    const pushCounts = r => { push({ counts: r.counts }); return r; };

    if (url.pathname === '/live') {
      if ((req.headers.get('Upgrade') || '').toLowerCase() !== 'websocket') return json({ error: 'Expected WebSocket' }, 426, h);
      return planStore.fetch(req);
    }

    if (url.pathname === '/votes' && req.method === 'GET') {
      const v = url.searchParams.get('voter');
      return json(await withPlan(await store.state(validVoter(v) ? v : null)), 200, h);
    }

    if (url.pathname === '/vote' && req.method === 'POST') {
      if (env.VOTE_LIMITER && !(await env.VOTE_LIMITER.limit({ key: ip })).success) return json({ error: 'rate_limited' }, 429, h);
      let b;
      try { b = await req.json(); } catch (e) { return json({ error: 'Bad request' }, 400, h); }
      const { ids } = await data(env);
      if (!validVoter(b.voter) || !ids.has(String(b.session))) return json({ error: 'Bad request' }, 400, h);
      return json(await withPlan(pushCounts(await store.vote(b.voter, String(b.session), !!b.on))), 200, h);
    }

    // team plan: who attends which session; PLAN_KEY or ADMIN_KEY to edit
    if (url.pathname === '/plan' && req.method === 'POST') {
      if (env.VOTE_LIMITER && !(await env.VOTE_LIMITER.limit({ key: 'plan:' + ip })).success) return json({ error: 'rate_limited' }, 429, h);
      let b;
      try { b = await req.json(); } catch (e) { return json({ error: 'Bad request' }, 400, h); }
      const keys = [env.PLAN_KEY, env.ADMIN_KEY].filter(Boolean);
      if (!keys.length) return json({ error: 'admin_not_configured' }, 503, h);
      if (!keys.some(k => sameKey(String(b.key || ''), k))) return json({ error: 'wrong_key' }, 403, h);
      const { ids } = await data(env);
      const changes = (Array.isArray(b.changes) ? b.changes : []).slice(0, 300)
        .map(c => ({ session: String(c && c.session), person: String(c && c.person), on: !!(c && c.on) }))
        .filter(c => ids.has(c.session) && PEOPLE.includes(c.person));
      const r = await planStore.planChanges(changes, b.wipe === true);
      if (changes.length || b.wipe === true) push(r);
      return json(r, 200, h);
    }

    if (url.pathname === '/vote/clear' && req.method === 'POST') {
      let b;
      try { b = await req.json(); } catch (e) { return json({ error: 'Bad request' }, 400, h); }
      if (!validVoter(b.voter)) return json({ error: 'Bad request' }, 400, h);
      return json(await withPlan(pushCounts(await store.clearVoter(b.voter))), 200, h);
    }

    // admin: clear one session or reset all votes, guarded by the ADMIN_KEY secret
    if (url.pathname === '/admin' && req.method === 'POST') {
      if (env.ASK_LIMITER && !(await env.ASK_LIMITER.limit({ key: 'admin:' + ip })).success) return json({ error: 'rate_limited' }, 429, h);
      if (!env.ADMIN_KEY) return json({ error: 'admin_not_configured' }, 503, h);
      let b;
      try { b = await req.json(); } catch (e) { return json({ error: 'Bad request' }, 400, h); }
      if (!sameKey(String(b.key || ''), env.ADMIN_KEY)) return json({ error: 'wrong_key' }, 403, h);
      if (b.action === 'check') return json({ ok: true }, 200, h);
      if (b.action === 'reset') return json(pushCounts(await store.reset()), 200, h);
      if (b.action === 'clear' && /^\d{6,8}$/.test(String(b.session))) return json(pushCounts(await store.clearSession(String(b.session))), 200, h);
      return json({ error: 'Bad request' }, 400, h);
    }

    if (url.pathname === '/ask' && req.method === 'POST') return ask(req, env, h, ip);

    return json({ error: 'Not found' }, 404, h);
  }
};
