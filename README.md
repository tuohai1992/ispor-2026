# ISPOR Europe 2026 · PMA team planner

Session list for ISPOR Europe 2026 (Vienna, 9–11 November 2026) where the PMA team can ask for recommendations and vote for the sessions to cover. Votes are shared and update live for everyone.

- `index.html` – the site (GitHub Pages).
- `data/sessions.json` – 169 sessions, Monday to Wednesday. Snapshot of 30 October 2026 from the official conference app, via ispor2026.abundanceds.com.
- `worker/` – Cloudflare Worker that stores votes (one SQLite Durable Object) and answers Ask with Claude.

## Backend

Routes, all limited to `ALLOWED_ORIGINS`:

| Route | Purpose |
|---|---|
| `GET /votes?voter=<id>` | Vote counts per session, plus this browser's own votes |
| `POST /vote` `{voter, session, on}` | Add or remove one vote |
| `POST /ask` `{question}` | Recommendations from the session list (needs `ANTHROPIC_API_KEY`) |

Each browser gets a random voter id, so one person counts once per browser. Limits: 80 votes per voter, 120 vote clicks and 6 questions per minute per IP.

Deploy: Cloudflare Workers & Pages → Create → Import a repository → this repo, root directory `worker`. Add a Secret `ANTHROPIC_API_KEY`. Then set `API` in `index.html` to the worker URL.
