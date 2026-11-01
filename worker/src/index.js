// ISPOR Europe 2026 team planner – votes and Ask backend.
import { DurableObject } from 'cloudflare:workers';

const DATA_URL = 'https://tuohai1992.github.io/ispor-2026/data/sessions.json';
const MODEL = 'claude-haiku-5-5';
const MAX_Q = 600;
const MAX_VOTES = 80;

let cache = { at: 0, data: null, ids: null, ctx: null };

async function data(env) {
  if (cache.data && Date.now() - cache.at < 3600e3) return cache;
  const d = await (await fetch((env && env.DATA_URL) || DATA_URL, { cf: { cacheTtl: 600 } })).json();
  const day = s => new Date(s.date + 'T12:00:00Z').toLocaleDateString('en-GB', { weekday: 'short', day: 'numeric', month: 'short', timeZone: 'UTC' });
  const lines = d.sessions
    .filter(s => s.format !== 'General Meeting')
    .map(s => `${s.id} | ${day(s)} ${s.start}-${s.end} | ${s.format} | ${s.tracks.join(', ')} | ${s.title} | ${s.speakers} | ${s.summary}`)
    .join('\n');
  const ctx = `You help a pharmaceutical Pricing & Market Access (PMA) team, working on ophthalmology medicines across Europe and Asia, choose sessions at ISPOR Europe 2026 (Vienna, 9-11 November 2026, all times CET).
Rules:
- Use ONLY the catalogue below. Never invent sessions, rooms, speakers or ids.
- Prefer sessions that fit the question. If two strong picks run at the same time, say so in their reasons.
- If the question is not about choosing or understanding sessions, say so in one sentence and return no sessions.
- "answer": 2-5 sentences, in the same language as the question, no headings.
- "sessions": at most 8, most relevant first, each with one short reason.
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
    this.sql.exec('CREATE TABLE IF NOT EXISTS votes (voter TEXT NOT NULL, session TEXT NOT NULL, at INTEGER NOT NULL, PRIMARY KEY (voter, session))');
  }
  state(voter) {
    const counts = {};
    for (const r of this.sql.exec('SELECT session, COUNT(*) AS n FROM votes GROUP BY session').toArray()) counts[r.session] = r.n;
    const mine = voter ? this.sql.exec('SELECT session FROM votes WHERE voter = ?', voter).toArray().map(r => r.session) : [];
    return { counts, mine };
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
  try { return JSON.parse(text); } catch (e) {}
  const a = text.indexOf('{'), b = text.lastIndexOf('}');
  if (a >= 0 && b > a) { try { return JSON.parse(text.slice(a, b + 1)); } catch (e) {} }
  return { answer: text, sessions: [] };
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
  async fetch(req, env) {
    const origin = req.headers.get('Origin') || '';
    const h = cors(env, origin);
    if (req.method === 'OPTIONS') return new Response(null, { headers: h });
    if (origin !== h['Access-Control-Allow-Origin']) return json({ error: 'Origin not allowed' }, 403, h);

    const url = new URL(req.url);
    const ip = req.headers.get('CF-Connecting-IP') || 'unknown';
    const store = env.VOTES.get(env.VOTES.idFromName('ispor-europe-2026'));

    if (url.pathname === '/votes' && req.method === 'GET') {
      const v = url.searchParams.get('voter');
      return json(await store.state(validVoter(v) ? v : null), 200, h);
    }

    if (url.pathname === '/vote' && req.method === 'POST') {
      if (env.VOTE_LIMITER && !(await env.VOTE_LIMITER.limit({ key: ip })).success) return json({ error: 'rate_limited' }, 429, h);
      let b;
      try { b = await req.json(); } catch (e) { return json({ error: 'Bad request' }, 400, h); }
      const { ids } = await data(env);
      if (!validVoter(b.voter) || !ids.has(String(b.session))) return json({ error: 'Bad request' }, 400, h);
      return json(await store.vote(b.voter, String(b.session), !!b.on), 200, h);
    }

    if (url.pathname === '/ask' && req.method === 'POST') return ask(req, env, h, ip);

    return json({ error: 'Not found' }, 404, h);
  }
};
