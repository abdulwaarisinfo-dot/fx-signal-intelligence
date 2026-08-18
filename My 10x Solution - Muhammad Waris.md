# My 10x Solution - Muhammad Waris

## 1. The problem

Sales and growth teams at fintech and FX companies lose deals not because their product is weak, but because they find out too late that a prospect is looking. A potential buyer complains about a competitor on Reddit, asks a question on X, or mentions a pain point in a Telegram group — and by the time a human on the sales team stumbles across it (if they ever do), the moment has passed. Manually watching social platforms for these signals does not scale past a handful of keywords and a few hours a day.

Flintel is a signal intelligence platform that solves this: it watches Twitter, Reddit, and Telegram continuously for buyer-intent signals in fintech and FX conversations, scores how likely each one is to represent a real opportunity, and delivers a ready-to-act alert — with a drafted outreach message — to Slack and HubSpot in under 90 seconds. It turns "we might have missed a warm lead somewhere on the internet" into "here is the lead, scored, with the first message already written."

Flintel is currently deployed with a real client — a Canadian fintech company expanding to 25 markets — so this isn't a hypothetical use case; it's already running against live traffic.

**10x claim:** manually monitoring social platforms for buyer intent takes hours of scattered attention a day and still misses most signals; Flintel turns that into a continuous, automated pipeline that surfaces a scored, actionable signal in under 90 seconds.

## 2. How it works, and the concepts implemented

Flintel is a two-service system: a background discovery engine that never stops watching, and a user-facing web app that surfaces what it finds. Five program concepts are implemented, mapped below to where they live in the codebase.

| # | Concept | Where it lives | What it does |
|---|---|---|---|
| 1 | **API endpoints** | `index.py` (FastAPI routes) | REST endpoints serving signals, scores, and account data to the web app, with request validation and proper status codes |
| 2 | **Database** | MongoDB Atlas, connection + queries in `index.py` | Persists discovered signals, scores, and per-user/per-tenant state; survives restarts by design — the whole pipeline is built to resume, not replay |
| 3 | **Background jobs** | Worker/polling functions in `index.py` | Continuously polls Twitter, Reddit, and Telegram every 60 seconds, runs keyword discovery, and hands candidates to the scoring layer — entirely off the request path |
| 4 | **Caching** | Caching logic in `index.py` | Expensive steps (repeated prompt context, semantic similarity lookups) are cached rather than recomputed, which is also the main lever used to control LLM cost at scale |
| 5 | **LLM integration** | Scoring functions in `index.py` | Each candidate signal goes through Claude/GPT-based scoring, returning a 0–10 intent score plus a drafted outreach message — a narrow, validated AI job behind an endpoint, with usage logged for cost tracking |

No swaps were needed — the five core concepts fit the project's actual shape without substitution.

### Architecture, in plain words

```
Twitter / Reddit / Telegram
        |
        v
  Background workers (poll every 60s)
        |
        v
  Two-layer filter (removes ~95% of noise before it reaches the LLM)
        |
        v
  LLM scoring (0-10 intent score + drafted outreach message)
        |
        v
  MongoDB (persisted, cached)
        |
        v
  API layer -> Slack / HubSpot delivery, web app
```

### Steps to run

```bash
git clone <flintel-repo-url>
cd flintel
pip install -r requirements.txt --break-system-packages
cp .env.example .env   # fill in MongoDB URI, LLM API key, platform API keys
python index.py        # or: uvicorn index:app --reload, depending on the entry point
```

The repository also includes `render.yaml` for one-command deployment to Render's free tier.

### One real limitation

Signal scoring quality depends heavily on how well the keyword and filter layer is tuned per client vertical — a fintech-tuned filter doesn't transfer cleanly to a different industry without re-tuning, so onboarding a new client currently involves manual keyword configuration rather than being fully self-serve yet.

### What I built with AI and how

I used Claude as a thinking and pair-programming partner throughout — for architecture decisions (the two-layer filter design, the caching strategy for cost control), for scaffolding the FastAPI routers and worker structure, and for debugging. I made the actual product and scope decisions myself (what counts as a signal, what the scoring rubric weighs, which platforms to prioritize), and I validated the scoring pipeline against real signals before trusting it in front of a live client.
