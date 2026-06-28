# MktOS Python Proxy Server v2.0

FastAPI backend that resolves browser CORS/CSP issues.
**Yahoo Finance (yfinance) primary → EODHD fallback → Finnhub news.**

## Quick start

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Keys are pre-configured in .env — edit if needed

# 3. Start server
python server.py

# Auto-reload during development:
uvicorn server:app --reload --port 3001

# 4. Open dashboard.html in browser (just double-click it)
```

## API Endpoints

| Endpoint | Source | Cache |
|---|---|---|
| `GET /health` | — | — |
| `GET /api/prices?symbols=NVDA,META,QQQ` | Yahoo Finance → EODHD | 60s |
| `GET /api/quote?symbol=NVDA` | Yahoo Finance → EODHD | 30s |
| `GET /api/news?category=general` | Finnhub | 5 min |
| `GET /api/company-news?symbol=NVDA` | Finnhub | 5 min |
| `GET /api/ticker-info?symbol=NVDA` | Yahoo Finance | 1 hr |
| `GET /api/history?symbol=NVDA&period=3mo` | Yahoo Finance | 5 min |

## Price source logic

```
Request → Try Yahoo Finance (yfinance)
            ✓ Got data → return with source="yahoo"
            ✗ Failed   → Try EODHD
                           ✓ Got data → return with source="eodhd (fallback)"
                           ✗ Failed   → HTTP 502 with error details
```

The dashboard shows a **green "Y" badge** for Yahoo-sourced prices
and an **amber "E" badge** for EODHD-sourced prices.

## Data freshness

- **Yahoo Finance** during market hours: real-time (1–5 min delay)
- **Yahoo Finance** after hours: last trade price
- **EODHD fallback**: ~15 min delay during market hours

## Deploy to production

### Railway (recommended, free tier)
```bash
# Install Railway CLI
npm i -g @railway/cli
railway login
railway init
railway up
```

### Render
- Connect GitHub repo
- Build command: `pip install -r requirements.txt`
- Start command: `python server.py`

### VPS with PM2
```bash
pip install gunicorn
gunicorn server:app -w 4 -k uvicorn.workers.UvicornWorker -b 0.0.0.0:3001
```

Then update `API` in `dashboard.html`:
```javascript
const API = 'https://your-server.railway.app';
```
