"""
MktOS Python Proxy Server v3.0
================================
NO pandas. NO scipy. NO yfinance at startup.
All data from EODHD (prices, history, S/R, indicators, SMC, ORB)
News: Finnhub → RSS fallback
Options/BigMoney: yfinance imported lazily (only when endpoint called)
AI: Anthropic proxy
Run: python server.py
"""

import os, time, logging, calendar, math
from datetime import datetime, timedelta
from typing import Optional

import numpy as np
import requests
import feedparser
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from dotenv import load_dotenv

load_dotenv(override=True)

EODHD_KEY     = os.getenv("EODHD_KEY",        "6a39c434eccce5.23158388")
FINNHUB_KEY   = os.getenv("FINNHUB_KEY",       "d8su0nhr01qh5revj690")
ANTHROPIC_KEY = os.getenv("ANTHROPIC_API_KEY", "")
PORT          = int(os.getenv("PORT", 3001))

EODHD_BASE    = "https://eodhd.com/api"
FINNHUB_BASE  = "https://finnhub.io/api/v1"
ANTHROPIC_URL = "https://api.anthropic.com/v1/messages"

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)s  %(message)s")
log = logging.getLogger("mktos")

# ── App ────────────────────────────────────────────────────────────────────
app = FastAPI(title="MktOS Proxy Server", version="3.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# ── Cache ──────────────────────────────────────────────────────────────────
_cache: dict = {}

def cache_get(key):
    e = _cache.get(key)
    if not e: return None
    if time.time() - e["ts"] > e["ttl"]:
        del _cache[key]; return None
    return e["data"]

def cache_set(key, data, ttl):
    _cache[key] = {"data": data, "ts": time.time(), "ttl": ttl}

# ── Startup ────────────────────────────────────────────────────────────────
@app.on_event("startup")
async def startup():
    log.info("=" * 55)
    log.info("  MktOS Proxy Server v3.0  — zero pandas/scipy")
    log.info(f"  EODHD key     : {EODHD_KEY[:8]}…")
    log.info(f"  Finnhub key   : {FINNHUB_KEY[:8]}… (len={len(FINNHUB_KEY)})")
    log.info(f"  Anthropic key : {'SET ✓' if ANTHROPIC_KEY else 'NOT SET — add to .env'}")
    log.info(f"  Port          : {PORT}")
    log.info("  Data sources  : EODHD (prices/history/indicators) + Finnhub/RSS (news)")
    log.info("=" * 55)

@app.get("/health")
def health():
    today = datetime.utcnow().strftime("%Y-%m-%d")
    count = _eodhd_call_count["count"] if _eodhd_call_count["date"] == today else 0
    return {
        "status":            "ok",
        "version":           "3.0",
        "port":              PORT,
        "eodhd_key":         EODHD_KEY[:6] + "…",
        "anthropic_key_set": bool(ANTHROPIC_KEY),
        "eodhd_calls_today": count,
        "eodhd_limit_hit":   _eodhd_call_count.get("limit_hit", False),
        "price_source":      "yahoo (fallback)" if _eodhd_call_count.get("limit_hit") else "eodhd",
    }

# ─────────────────────────────────────────────────────────────────────────
#  EODHD HELPERS
# ─────────────────────────────────────────────────────────────────────────
# EODHD API call counter — track daily usage (resets at midnight UTC)
_eodhd_call_count = {"date": "", "count": 0, "limit_hit": False}

def _eodhd_get(path: str, params: dict, timeout=15) -> dict | list:
    """
    EODHD API call with daily counter tracking.
    Free plan: 20 calls/day. Paid plans: much higher limits.
    Check http://localhost:3001/api/usage to see current count.
    """
    global _eodhd_call_count
    today = datetime.utcnow().strftime("%Y-%m-%d")

    # Reset counter daily
    if _eodhd_call_count["date"] != today:
        _eodhd_call_count = {"date": today, "count": 0, "limit_hit": False}

    _eodhd_call_count["count"] += 1
    log.debug(f"[eodhd] call #{_eodhd_call_count['count']} today → {path}")

    params["api_token"] = EODHD_KEY
    params.setdefault("fmt", "json")
    r = requests.get(f"{EODHD_BASE}/{path}", params=params, timeout=timeout)

    if r.status_code == 429:
        _eodhd_call_count["limit_hit"] = True
        raise ValueError(f"EODHD daily limit hit (HTTP 429) after {_eodhd_call_count['count']} calls today")

    if not r.ok:
        body = r.text[:200]
        # Detect quota messages in body
        if any(x in body.lower() for x in ["limit", "quota", "credits", "exceeded"]):
            _eodhd_call_count["limit_hit"] = True
            raise ValueError(f"EODHD quota exceeded: {body}")
        raise ValueError(f"EODHD {path} HTTP {r.status_code}: {body}")

    return r.json()

def _yahoo_eod_history(symbol: str, days: int = 90) -> dict:
    """
    Daily OHLCV from Yahoo Finance — pure HTTP requests, zero extra packages.
    Uses Yahoo Finance v8 chart API directly (same data as yfinance, no pandas).
    """
    import time as _time

    period2 = "3mo" if days <= 90 else "6mo" if days <= 180 else "1y" if days <= 365 else "2y"
    range_map = {"3mo": "3mo", "6mo": "6mo", "1y": "1y", "2y": "2y"}
    yf_range  = range_map.get(period2, "3mo")

    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
    params = {
        "range":    yf_range,
        "interval": "1d",
        "events":   "history",
    }
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept":     "application/json",
    }

    # Try query1 first, then query2 as fallback
    for host in ["query1.finance.yahoo.com", "query2.finance.yahoo.com"]:
        try:
            url = f"https://{host}/v8/finance/chart/{symbol}"
            r   = requests.get(url, params=params, headers=headers, timeout=15)
            if not r.ok:
                log.warning(f"[yahoo_eod] {host} HTTP {r.status_code}")
                continue

            data    = r.json()
            result  = data.get("chart", {}).get("result", [])
            if not result:
                log.warning(f"[yahoo_eod] {host} empty result")
                continue

            chart     = result[0]
            timestamps= chart.get("timestamp", [])
            quote     = chart.get("indicators", {}).get("quote", [{}])[0]
            adjclose  = chart.get("indicators", {}).get("adjclose", [{}])
            closes_raw= (adjclose[0].get("adjclose") if adjclose else None) or quote.get("close", [])

            if not timestamps or not closes_raw:
                continue

            # Convert timestamps to dates
            dates   = [datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d") for ts in timestamps]
            opens   = [round(float(v), 4) if v else 0.0 for v in quote.get("open",   closes_raw)]
            highs   = [round(float(v), 4) if v else 0.0 for v in quote.get("high",   closes_raw)]
            lows    = [round(float(v), 4) if v else 0.0 for v in quote.get("low",    closes_raw)]
            closes  = [round(float(v), 4) if v else 0.0 for v in closes_raw]
            volumes = [int(v) if v else 0               for v in quote.get("volume", [0]*len(closes))]

            # Filter out null/zero closes
            valid = [(d,o,h,l,c,v) for d,o,h,l,c,v in zip(dates,opens,highs,lows,closes,volumes) if c > 0]
            if not valid:
                continue

            dates, opens, highs, lows, closes, volumes = zip(*valid)
            log.info(f"[yahoo_eod] {symbol}: {len(closes)} bars from {host}")
            return {
                "dates":   list(dates),
                "opens":   list(opens),
                "highs":   list(highs),
                "lows":    list(lows),
                "closes":  list(closes),
                "volumes": list(volumes),
            }
        except Exception as e:
            log.warning(f"[yahoo_eod] {host} error: {e}")
            continue

    raise ValueError(f"Yahoo Finance HTTP API failed for {symbol} — both query1 and query2 unreachable")


def _fetch_eod_history(symbol: str, days: int = 90) -> dict:
    """
    Daily OHLCV — EODHD primary, Yahoo Finance fallback.
    Used for: S/R calculation, technical indicators, SMC concepts.
    When EODHD daily limit is hit, Yahoo Finance provides identical data.
    """
    errors = []

    # ── Primary: EODHD ────────────────────────────────────────────────────
    try:
        date_from = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")
        date_to   = datetime.utcnow().strftime("%Y-%m-%d")
        data = _eodhd_get(f"eod/{symbol}.US", {"period":"d","from":date_from,"to":date_to})
        if data and isinstance(data, list) and len(data) > 0:
            log.debug(f"[eod] {symbol}: {len(data)} bars from EODHD")
            return {
                "dates":   [d["date"]                          for d in data],
                "opens":   [float(d.get("open",  d["close"])) for d in data],
                "highs":   [float(d.get("high",  d["close"])) for d in data],
                "lows":    [float(d.get("low",   d["close"])) for d in data],
                "closes":  [float(d["close"])                  for d in data],
                "volumes": [int(d.get("volume", 0) or 0)       for d in data],
            }
        errors.append("EODHD returned empty data")
    except Exception as e:
        err_str = str(e)
        errors.append(f"EODHD: {err_str}")
        if any(x in err_str.lower() for x in ["429","limit","quota","403","credits"]):
            log.warning(f"[eod] {symbol} EODHD limit hit — switching to Yahoo Finance")
        else:
            log.warning(f"[eod] {symbol} EODHD failed: {err_str}")

    # ── Fallback: Yahoo Finance ────────────────────────────────────────────
    try:
        log.info(f"[eod] {symbol} trying Yahoo Finance fallback")
        result = _yahoo_eod_history(symbol, days)
        log.info(f"[eod] {symbol}: {len(result['closes'])} bars from Yahoo Finance (EODHD errors: {errors})")
        return result
    except Exception as e:
        errors.append(f"Yahoo: {e}")
        log.error(f"[eod] {symbol} Yahoo fallback failed: {e}")

    raise ValueError(f"All EOD history sources failed for {symbol}. Errors: {errors}")

def _yahoo_realtime(symbols: list[str]) -> dict:
    """
    Real-time prices from Yahoo Finance — pure HTTP, no yfinance/pandas.
    Uses Yahoo Finance v8 chart API with 2d range to get change %.
    """
    result = {}
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept":     "application/json",
    }
    for symbol in symbols:
        for host in ["query1.finance.yahoo.com", "query2.finance.yahoo.com"]:
            try:
                url = f"https://{host}/v8/finance/chart/{symbol}"
                r   = requests.get(url,
                                   params={"range":"5d","interval":"1d","events":"history"},
                                   headers=headers, timeout=10)
                if not r.ok: continue

                data   = r.json()
                res    = data.get("chart",{}).get("result",[])
                if not res: continue

                chart  = res[0]
                meta   = chart.get("meta", {})
                ts     = chart.get("timestamp", [])
                quote  = chart.get("indicators",{}).get("quote",[{}])[0]
                adj    = chart.get("indicators",{}).get("adjclose",[{}])
                closes = (adj[0].get("adjclose") if adj else None) or quote.get("close",[])

                # Filter valid closes
                valid_closes = [float(c) for c in closes if c is not None and float(c)>0]
                if len(valid_closes) < 1: continue

                close   = valid_closes[-1]
                prev_c  = valid_closes[-2] if len(valid_closes)>=2 else close
                chg     = close - prev_c
                chgp    = (chg/prev_c*100) if prev_c else 0

                opens   = [v for v in quote.get("open",[]) if v]
                highs   = [v for v in quote.get("high",[]) if v]
                lows    = [v for v in quote.get("low", []) if v]
                vols    = [v for v in quote.get("volume",[]) if v]

                result[symbol] = {
                    "close":         round(close, 4),
                    "change":        round(chg,   4),
                    "change_p":      round(chgp,  4),
                    "open":          round(float(opens[-1]),  4) if opens  else round(close,4),
                    "high":          round(float(highs[-1]),  4) if highs  else round(close,4),
                    "low":           round(float(lows[-1]),   4) if lows   else round(close,4),
                    "volume":        int(vols[-1])                if vols   else None,
                    "previousClose": round(prev_c, 4),
                    "source":        "yahoo",
                }
                log.debug(f"[yahoo_rt] {symbol}: ${close} via {host}")
                break  # success — move to next symbol
            except Exception as e:
                log.debug(f"[yahoo_rt] {symbol} {host}: {e}")
                continue
    return result


def _eodhd_realtime(symbols: list[str]) -> dict:
    """
    Real-time prices — EODHD primary, Yahoo Finance fallback.
    EODHD free plan: 20 calls/day. When limit hit (HTTP 429/403 with quota msg),
    automatically falls back to Yahoo Finance at no cost.
    """
    errors = []

    # ── Primary: EODHD ────────────────────────────────────────────────────
    try:
        base   = symbols[0]
        rest   = ",".join(f"{s}.US" for s in symbols[1:]) if len(symbols) > 1 else ""
        params = {}
        if rest: params["s"] = rest
        data  = _eodhd_get(f"real-time/{base}.US", params)
        items = data if isinstance(data, list) else [data]
        result = {}
        for q in items:
            sym = (q.get("code") or "").replace(".US","")
            if not sym: continue
            close = float(q.get("close") or 0)
            if close <= 0: continue
            prev  = float(q.get("previousClose") or q.get("open") or close)
            chg   = float(q.get("change") or (close - prev))
            chgp  = float(q.get("change_p") or ((chg/prev*100) if prev else 0))
            result[sym] = {
                "close":         round(close, 4),
                "change":        round(chg,   4),
                "change_p":      round(chgp,  4),
                "open":          round(float(q.get("open",  close)), 4),
                "high":          round(float(q.get("high",  close)), 4),
                "low":           round(float(q.get("low",   close)), 4),
                "volume":        int(q.get("volume", 0)) or None,
                "previousClose": round(prev, 4),
                "source":        "eodhd",
            }
        if result:
            return result
        errors.append("EODHD returned empty data")
    except Exception as e:
        err_str = str(e)
        errors.append(f"EODHD: {err_str}")
        # Detect quota/limit errors
        if any(x in err_str.lower() for x in ["429","limit","quota","403","credits"]):
            log.warning(f"[prices] EODHD limit hit — switching to Yahoo Finance fallback")
        else:
            log.warning(f"[prices] EODHD error: {err_str}")

    # ── Fallback: Yahoo Finance ────────────────────────────────────────────
    try:
        log.info(f"[prices] Trying Yahoo Finance fallback for {symbols}")
        result = _yahoo_realtime(symbols)
        if result:
            log.info(f"[prices] Yahoo Finance → {len(result)} tickers (EODHD errors: {errors})")
            return result
        errors.append("Yahoo returned empty data")
    except Exception as e:
        errors.append(f"Yahoo: {e}")
        log.error(f"[prices] Yahoo fallback also failed: {e}")

    raise ValueError(f"All price sources failed. Errors: {errors}")

# ─────────────────────────────────────────────────────────────────────────
#  PRICES
# ─────────────────────────────────────────────────────────────────────────
@app.get("/api/prices")
def get_prices(symbols: str = Query(default="NVDA,META,PLTR,SOFI,AVGO,CEG,NFLX,QQQ,SPY,NOW,VST,IONQ,AAPL,TSLA,MELI")):
    sym_list  = [s.strip().upper() for s in symbols.split(",") if s.strip()]
    cache_key = "prices:" + ",".join(sorted(sym_list))
    cached    = cache_get(cache_key)
    if cached:
        return {"source": "eodhd (cached)", "data": cached}
    try:
        data = _eodhd_realtime(sym_list)
        if not data: raise ValueError("No data returned")
        log.info(f"[prices] EODHD → {len(data)} tickers")
        cache_set(cache_key, data, 60)
        return {"source": "eodhd", "data": data}
    except Exception as e:
        log.error(f"[prices] {e}")
        raise HTTPException(502, str(e))

@app.get("/api/quote")
def get_quote(symbol: str = Query(default="NVDA")):
    symbol = symbol.strip().upper()
    ck = f"quote:{symbol}"; cached = cache_get(ck)
    if cached: return {"source": "eodhd (cached)", "quote": cached}
    try:
        data = _eodhd_realtime([symbol])
        q    = data.get(symbol)
        if not q: raise ValueError(f"No quote for {symbol}")
        q["symbol"] = symbol
        cache_set(ck, q, 30)
        return {"source": "eodhd", "quote": q}
    except Exception as e:
        raise HTTPException(502, str(e))

@app.get("/api/history")
def get_history(symbol:str=Query(default="NVDA"), period:str=Query(default="3mo"), interval:str=Query(default="1d")):
    symbol = symbol.strip().upper()
    ck = f"history:{symbol}:{period}"; cached = cache_get(ck)
    if cached: return {"source":"eodhd (cached)","symbol":symbol,"history":cached}
    days = {"1mo":30,"3mo":90,"6mo":180,"1y":365,"2y":730}.get(period, 90)
    try:
        ohlcv = _fetch_eod_history(symbol, days)
        rows  = [{"date":ohlcv["dates"][i],"open":ohlcv["opens"][i],"high":ohlcv["highs"][i],
                  "low":ohlcv["lows"][i],"close":ohlcv["closes"][i],"volume":ohlcv["volumes"][i]}
                 for i in range(len(ohlcv["dates"]))]
        cache_set(ck, rows, 300)
        return {"source":"eodhd","symbol":symbol,"period":period,"history":rows}
    except Exception as e:
        raise HTTPException(502, str(e))

# ─────────────────────────────────────────────────────────────────────────
#  TECHNICAL INDICATORS — pure numpy from EODHD history
# ─────────────────────────────────────────────────────────────────────────
def _ema(values: np.ndarray, period: int) -> np.ndarray:
    k = 2.0 / (period + 1)
    out = np.empty(len(values)); out[0] = values[0]
    for i in range(1, len(values)):
        out[i] = values[i] * k + out[i-1] * (1 - k)
    return out

def _rsi(closes: np.ndarray, period: int = 14) -> float:
    d = np.diff(closes)
    gains  = np.where(d > 0, d, 0.0)
    losses = np.where(d < 0, -d, 0.0)
    if len(gains) < period: return 50.0
    ag = np.mean(gains[:period]); al = np.mean(losses[:period])
    for i in range(period, len(gains)):
        ag = (ag*(period-1)+gains[i])/period
        al = (al*(period-1)+losses[i])/period
    return round(float(100 - 100/(1 + ag/max(al,1e-10))), 2)

def _macd(closes: np.ndarray):
    if len(closes) < 26: return 0.0, 0.0, 0.0
    ml = _ema(closes,12) - _ema(closes,26)
    sig = _ema(ml, 9)
    return round(float(ml[-1]),4), round(float(sig[-1]),4), round(float(ml[-1]-sig[-1]),4)

def _atr(h, l, c, period=14) -> float:
    tr = [max(h[i]-l[i], abs(h[i]-c[i-1]), abs(l[i]-c[i-1])) for i in range(1,len(c))]
    return round(float(np.mean(tr[-period:])), 4) if tr else 0.0

def _ema_signal(c: np.ndarray) -> str:
    if len(c) < 50: return "Insufficient data"
    e9=_ema(c,9); e21=_ema(c,21); e50=_ema(c,50); p=c[-1]
    if e9[-1]>e21[-1] and e9[-4]<=e21[-4]: return "Bullish X"
    if e9[-1]<e21[-1] and e9[-4]>=e21[-4]: return "Bearish X"
    if e9[-1]>e21[-1]>e50[-1] and p>e9[-1]: return "Strong uptrend"
    if e9[-1]>e21[-1] and p>e21[-1]: return "Above 21"
    if p<e50[-1]: return "Below 50"
    if e9[-1]<e21[-1]: return "Sideways"
    return "Neutral"

def _trend(c: np.ndarray) -> str:
    if len(c)<20: return "flat"
    y=c[-20:]; x=np.arange(len(y))
    slope=np.polyfit(x,y,1)[0]/y[0]*100
    return "up" if slope>0.2 else "down" if slope<-0.2 else "flat"

def _vol_signal(vols: list) -> str:
    if len(vols)<20: return "med"
    r=np.mean(vols[-5:])/max(np.mean(vols[-20:]),1)
    return "high" if r>=1.3 else "low" if r<0.8 else "med"

def _calc_indicators(symbol: str) -> dict:
    ohlcv = _fetch_eod_history(symbol, days=120)
    c=np.array(ohlcv["closes"]); h=np.array(ohlcv["highs"])
    l=np.array(ohlcv["lows"]);   v=ohlcv["volumes"]
    price=float(c[-1]); prev=float(c[-2]) if len(c)>=2 else price
    macd_v,macd_s,macd_h = _macd(c)
    e9=_ema(c,9); e21=_ema(c,21); e50=_ema(c,50) if len(c)>=50 else None
    return {
        "symbol":symbol,"price":round(price,2),
        "change_pct":round((price-prev)/prev*100,2) if prev else 0,
        "rsi":_rsi(c),"macd":macd_v,"macd_signal":macd_s,"macd_hist":macd_h,
        "atr":_atr(h.tolist(),l.tolist(),c.tolist()),
        "ema_signal":_ema_signal(c),
        "ema9":round(float(e9[-1]),2),"ema21":round(float(e21[-1]),2),
        "ema50":round(float(e50[-1]),2) if e50 is not None else None,
        "trend":_trend(c),"volume_signal":_vol_signal(v),
        "bars_used":len(c),"source":"eodhd",
        "calculated_at":datetime.utcnow().isoformat(),
    }

@app.get("/api/indicators")
def get_indicators(symbol: str = Query(default="NVDA")):
    symbol=symbol.strip().upper(); ck=f"indicators:{symbol}"
    cached=cache_get(ck)
    if cached: return {"source":"eodhd (cached)","indicators":cached}
    try:
        ind=_calc_indicators(symbol); cache_set(ck,ind,900)
        log.info(f"[indicators] {symbol}: RSI={ind['rsi']} MACD={ind['macd']:+.3f} EMA={ind['ema_signal']}")
        return {"source":"eodhd","indicators":ind}
    except Exception as e:
        log.error(f"[indicators] {symbol}: {e}"); raise HTTPException(502,str(e))

@app.get("/api/indicators/batch")
def get_indicators_batch(symbols: str = Query(default="NVDA,META,PLTR,SOFI,AVGO,CEG,NOW,NFLX,VST,IONQ")):
    sym_list=[s.strip().upper() for s in symbols.split(",") if s.strip()]
    ck="indicators:batch:"+",".join(sorted(sym_list)); cached=cache_get(ck)
    if cached: return {"source":"cache","data":cached}
    results,errors={},{}
    for sym in sym_list:
        sk=f"indicators:{sym}"; c=cache_get(sk)
        if c: results[sym]=c; continue
        try:
            ind=_calc_indicators(sym); cache_set(sk,ind,900); results[sym]=ind
            log.info(f"[indicators/batch] {sym}: RSI={ind['rsi']} EMA={ind['ema_signal']}")
        except Exception as e:
            errors[sym]=str(e); log.warning(f"[indicators/batch] {sym}: {e}")
    cache_set(ck,results,900)
    return {"source":"eodhd","data":results,"errors":errors}

# ─────────────────────────────────────────────────────────────────────────
#  SUPPORT & RESISTANCE
# ─────────────────────────────────────────────────────────────────────────
def _compute_sr(h,l,c,symbol) -> dict:
    h=np.array(h); l=np.array(l); c=np.array(c)
    price=float(c[-1])
    tr=[max(h[i]-l[i],abs(h[i]-c[i-1]),abs(l[i]-c[i-1])) for i in range(1,len(c))]
    atr=float(np.mean(tr[-14:])) if len(tr)>=14 else float(np.mean(tr)) if tr else price*0.02
    w=5; ph,pl=[],[]
    for i in range(w,len(h)-w):
        if h[i]==max(h[i-w:i+w+1]): ph.append(float(h[i]))
        if l[i]==min(l[i-w:i+w+1]): pl.append(float(l[i]))
    def cluster(vals):
        if not vals: return []
        vals=sorted(vals); out,grp=[],[vals[0]]
        for v in vals[1:]:
            if (v-grp[0])/max(grp[0],1)<0.005: grp.append(v)
            else: out.append(round(float(np.mean(grp)),2)); grp=[v]
        out.append(round(float(np.mean(grp)),2)); return out
    res=sorted([v for v in cluster(ph) if v>price])
    sup=sorted([v for v in cluster(pl) if v<price],reverse=True)
    rh=float(np.max(h[-20:])); rl=float(np.min(l[-20:]))
    if rh>price and (not res or abs(rh-res[0])/price>0.005): res=sorted(res+[round(rh,2)])
    if rl<price and (not sup or abs(rl-sup[0])/price>0.005): sup=sorted(sup+[round(rl,2)],reverse=True)
    r1=res[0] if res else round(price+1.2*atr,2)
    r2=res[1] if len(res)>=2 else round(r1+atr,2)
    s1=sup[0] if sup else round(price-1.5*atr,2)
    s2=sup[1] if len(sup)>=2 else round(s1-atr,2)
    if s2>=s1: s2=round(s1-atr,2)
    if r1<=price: r1=round(price+atr,2)
    if r2<=r1:    r2=round(r1+atr,2)
    return {"symbol":symbol,"price":round(price,2),"atr":round(atr,2),
            "s2":round(s2,2),"s1":round(s1,2),"r1":round(r1,2),"r2":round(r2,2),
            "pivot_res":res[:4],"pivot_sup":sup[:4],
            "method":"swing pivots + 20d range","calculated_at":datetime.utcnow().isoformat()}

def _calc_sr(symbol:str, price:float=None):
    # Primary: EODHD historical OHLC → swing pivot S/R
    try:
        ohlcv = _fetch_eod_history(symbol, days=90)
        sr    = _compute_sr(ohlcv["highs"], ohlcv["lows"], ohlcv["closes"], symbol)
        return sr, "eodhd"
    except Exception as e:
        log.warning(f"[sr] {symbol} EODHD history failed: {e}")

    # Fallback: ATR-only using live price (from hint or EODHD real-time)
    live_price = price
    if not live_price:
        try:
            rt = _eodhd_realtime([symbol])
            live_price = rt.get(symbol, {}).get("close")
        except Exception:
            pass

    if live_price and float(live_price) > 0:
        p   = float(live_price)
        atr = p * 0.02
        log.info(f"[sr] {symbol} using ATR fallback @ ${p:.2f}")
        return {
            "symbol": symbol, "price": round(p, 2), "atr": round(atr, 2),
            "s2": round(p - 2.8*atr, 2), "s1": round(p - 1.5*atr, 2),
            "r1": round(p + 1.2*atr, 2), "r2": round(p + 2.5*atr, 2),
            "method": "atr_only_fallback",
            "calculated_at": datetime.utcnow().isoformat(),
        }, "atr_only"

    raise ValueError(f"All S/R sources failed for {symbol} — no price available")

@app.get("/api/sr")
def get_sr(symbol:str=Query(default="NVDA"), price:float=Query(default=None)):
    symbol=symbol.strip().upper(); ck=f"sr:{symbol}"
    cached=cache_get(ck)
    if cached: return {"source":"eodhd (cached)","sr":cached}
    try:
        sr,src=_calc_sr(symbol,price)
        log.info(f"[sr] {symbol}: S1={sr['s1']} R1={sr['r1']}")
        cache_set(ck,sr,900); return {"source":src,"sr":sr}
    except Exception as e:
        raise HTTPException(502,str(e))

@app.get("/api/sr/batch")
def get_sr_batch(symbols:str=Query(default="NVDA,META,PLTR,SOFI,AVGO,CEG,NOW,NFLX,VST,IONQ,QQQ"),prices:str=Query(default="")):
    sym_list=[s.strip().upper() for s in symbols.split(",") if s.strip()]
    px_list=[float(p) if p and p.strip() else None for p in prices.split(",")] if prices.strip() else []
    while len(px_list)<len(sym_list): px_list.append(None)
    ck="sr:batch:"+",".join(sorted(sym_list)); cached=cache_get(ck)
    if cached: return {"source":"cache","data":cached}
    results, errors = {}, {}
    for sym,px in zip(sym_list,px_list):
        sk=f"sr:{sym}"; c=cache_get(sk)
        if c: results[sym]=c; continue
        try:
            sr,src=_calc_sr(sym,px); cache_set(sk,sr,900); results[sym]=sr
            log.info(f"[sr/batch] {sym}: S1={sr['s1']} R1={sr['r1']}")
        except Exception as e:
            errors[sym]=str(e); log.warning(f"[sr/batch] {sym}: {e}")
    cache_set(ck,results,900)
    return {"source":"eodhd","data":results,"errors":errors}

# ─────────────────────────────────────────────────────────────────────────
#  SMC — all 5 concepts
# ─────────────────────────────────────────────────────────────────────────
def _detect_fvg(o,h,l,c,lookback=60):
    n=min(len(c),lookback); price=float(c[-1]); bull,bear=[],[]
    for i in range(2,n):
        if l[i]>h[i-2]:
            bull.append({"top":round(float(l[i]),4),"bottom":round(float(h[i-2]),4),
                         "mid":round((float(l[i])+float(h[i-2]))/2,4),
                         "gap_pct":round((float(l[i])-float(h[i-2]))/float(h[i-2])*100,3),
                         "idx":i,"filled":price<float(h[i-2])})
        if h[i]<l[i-2]:
            bear.append({"top":round(float(l[i-2]),4),"bottom":round(float(h[i]),4),
                         "mid":round((float(l[i-2])+float(h[i]))/2,4),
                         "gap_pct":round((float(l[i-2])-float(h[i]))/float(l[i-2])*100,3),
                         "idx":i,"filled":price>float(l[i-2])})
    ba=[f for f in bull if not f["filled"]][-4:]
    be=[f for f in bear if not f["filled"]][-4:]
    return {"bullish":ba,"bearish":be,"nearest_bull":ba[-1] if ba else None,"nearest_bear":be[-1] if be else None}

def _detect_order_blocks(o,h,l,c,lookback=60):
    n=min(len(c),lookback); price=float(c[-1]); bull,bear=[],[]
    for i in range(1,n-1):
        if l[i]==0: continue
        if c[i]<o[i]:
            nc=(c[i+1]-o[i+1])/max(abs(o[i+1]),0.01)*100
            if nc>0.4 and c[i+1]>float(h[i]):
                ob={"top":round(float(o[i]),4),"bottom":round(float(c[i]),4),
                    "mid":round((float(o[i])+float(c[i]))/2,4),"type":"bullish",
                    "impulse":round(nc,2),"idx":i,
                    "tested":float(c[i])<=price<=float(o[i]),"broken":price<float(c[i]),
                    "desc":f"Bullish OB ${c[i]:.2f}–${o[i]:.2f}"}
                bull.append(ob)
        if c[i]>o[i]:
            nc=(c[i+1]-o[i+1])/max(abs(o[i+1]),0.01)*100
            if nc<-0.4 and c[i+1]<float(l[i]):
                ob={"top":round(float(c[i]),4),"bottom":round(float(o[i]),4),
                    "mid":round((float(c[i])+float(o[i]))/2,4),"type":"bearish",
                    "impulse":round(nc,2),"idx":i,
                    "tested":float(o[i])<=price<=float(c[i]),"broken":price>float(c[i]),
                    "desc":f"Bearish OB ${o[i]:.2f}–${c[i]:.2f}"}
                bear.append(ob)
    bv=[x for x in bull if not x["broken"]][-3:]
    bv2=[x for x in bear if not x["broken"]][-3:]
    return {"bullish":bv,"bearish":bv2,"nearest_bull":bv[-1] if bv else None,"nearest_bear":bv2[-1] if bv2 else None}

def _detect_choch_bos(h,l,c,lookback=60):
    n=min(len(c),lookback); hh=h[:n]; ll=l[:n]; cc=c[:n]; w=3; events=[]
    sh,sl=[],[]
    for i in range(w,n-w):
        if hh[i]==max(hh[i-w:i+w+1]): sh.append((i,float(hh[i])))
        if ll[i]==min(ll[i-w:i+w+1]): sl.append((i,float(ll[i])))
    for idx in range(1,len(sh)):
        if sh[idx][1]>sh[idx-1][1]:
            events.append({"type":"BOS_BULL","level":round(sh[idx-1][1],4),"broken_at":round(sh[idx][1],4),"idx":sh[idx][0],"desc":f"Bullish BoS — broke ${sh[idx-1][1]:.2f}","bias":"bullish"})
    for idx in range(1,len(sl)):
        if sl[idx][1]<sl[idx-1][1]:
            events.append({"type":"BOS_BEAR","level":round(sl[idx-1][1],4),"broken_at":round(sl[idx][1],4),"idx":sl[idx][0],"desc":f"Bearish BoS — broke ${sl[idx-1][1]:.2f}","bias":"bearish"})
    price=float(cc[-1])
    if len(sh)>=2 and len(sl)>=2:
        if sh[-1][1]<sh[-2][1] and price>sh[-1][1]:
            events.append({"type":"CHOCH_BULL","level":round(sh[-1][1],4),"idx":n-1,"desc":f"Bullish CHoCH — broke ${sh[-1][1]:.2f}","bias":"bullish"})
        if sl[-1][1]>sl[-2][1] and price<sl[-1][1]:
            events.append({"type":"CHOCH_BEAR","level":round(sl[-1][1],4),"idx":n-1,"desc":f"Bearish CHoCH — broke ${sl[-1][1]:.2f}","bias":"bearish"})
    recent=sorted(events,key=lambda x:x["idx"],reverse=True)[:8]
    latest=recent[0] if recent else None
    bias=latest["bias"] if latest else "neutral"
    return {"events":recent,"bias":bias,"latest":latest,"bull_count":len([e for e in recent if e["bias"]=="bullish"]),"bear_count":len([e for e in recent if e["bias"]=="bearish"]),"choch":[e for e in recent if "CHOCH" in e["type"]]}

def _detect_liquidity_sweeps(h,l,c,v,lookback=60):
    n=min(len(c),lookback); sweeps=[]; w=5
    for i in range(w,n):
        rh=max(h[i-w:i]); rl=min(l[i-w:i])
        pc=float(c[i]); wh=float(h[i]); wl=float(l[i])
        if wl<rl and pc>rl:
            d=round((float(rl)-wl)/float(rl)*100,3)
            if d>0.05:
                sweeps.append({"type":"BULL_SWEEP","swept_level":round(float(rl),4),"wick_extreme":round(wl,4),"close":round(pc,4),"depth_pct":d,"volume":int(v[i]) if i<len(v) else 0,"idx":i,"desc":f"Bullish sweep — grabbed ${wl:.2f}, swept ${rl:.2f}","signal":"Long after sweep"})
        if wh>rh and pc<rh:
            d=round((wh-float(rh))/float(rh)*100,3)
            if d>0.05:
                sweeps.append({"type":"BEAR_SWEEP","swept_level":round(float(rh),4),"wick_extreme":round(wh,4),"close":round(pc,4),"depth_pct":d,"volume":int(v[i]) if i<len(v) else 0,"idx":i,"desc":f"Bearish sweep — grabbed ${wh:.2f}, swept ${rh:.2f}","signal":"Short after sweep"})
    recent=sorted(sweeps,key=lambda x:x["idx"],reverse=True)[:6]
    return {"sweeps":recent,"latest":recent[0] if recent else None,"bull_sweeps":[s for s in recent if s["type"]=="BULL_SWEEP"],"bear_sweeps":[s for s in recent if s["type"]=="BEAR_SWEEP"]}

def _detect_volume_base(o,h,l,c,v,lookback=80):
    if not v or max(v)==0: return {"bases":[],"nearest":None}
    n=min(len(c),lookback); avg20=float(np.mean(v[-20:])) if len(v)>=20 else float(np.mean(v))
    price=float(c[-1]); bases=[]; i=2
    while i<n-1:
        hlr=(h[i]-l[i])/max(l[i],0.01)*100; vr=v[i]/max(avg20,1)
        if hlr<2.0 and vr>=1.15:
            run=[i]; j=i+1
            while j<n and (h[j]-l[j])/max(l[j],0.01)*100<2.5:
                run.append(j); j+=1
            if len(run)>=1:
                rh=[h[k] for k in run]; rl=[l[k] for k in run]; rv=[v[k] for k in run]
                zt=max(rh); zb=min(rl); vrat=round(float(np.mean(rv))/max(avg20,1),2)
                bt="accumulation" if c[run[-1]]>=o[run[0]] else "distribution"
                inz=zb<=price<=zt
                bases.append({"type":bt,"top":round(float(zt),4),"bottom":round(float(zb),4),"mid":round((float(zt)+float(zb))/2,4),"vol_ratio":vrat,"candles":len(run),"idx_start":run[0],"idx_end":run[-1],"price_in_zone":inz,"desc":f"{bt.title()} base — {len(run)} candles {vrat}x avg vol","signal":"Price IN zone" if inz else f"Watch retest ${round((float(zt)+float(zb))/2,2):.2f}"})
            i=j
        else: i+=1
    bases.sort(key=lambda x:x["vol_ratio"],reverse=True)
    nearest=min(bases,key=lambda x:abs(x["mid"]-price)) if bases else None
    return {"bases":bases[:5],"nearest":nearest,"accumulation":[b for b in bases if b["type"]=="accumulation"][:3],"distribution":[b for b in bases if b["type"]=="distribution"][:3]}

def _run_all_smc(symbol:str) -> dict:
    ohlcv=_fetch_eod_history(symbol,days=120)
    o=ohlcv["opens"]; h=ohlcv["highs"]; l=ohlcv["lows"]; c=ohlcv["closes"]; v=ohlcv["volumes"]
    fvg=_detect_fvg(o,h,l,c); ob=_detect_order_blocks(o,h,l,c)
    choch=_detect_choch_bos(h,l,c); liq=_detect_liquidity_sweeps(h,l,c,v)
    vbase=_detect_volume_base(o,h,l,c,v); price=float(c[-1])
    score=20; signals=[]
    if choch["bias"]=="bullish": score+=25; signals.append(f"Bullish structure")
    elif choch["bias"]=="bearish": score-=20
    if fvg["bullish"]:
        nf=fvg["bullish"][-1]
        if nf["bottom"]<=price<=nf["top"]: score+=20; signals.append(f"Inside bullish FVG ${nf['bottom']}–${nf['top']}")
        elif price>nf["top"]: score+=8; signals.append(f"Bullish FVG below at ${nf['mid']}")
    if ob["bullish"]:
        nob=ob["bullish"][-1]
        if nob["bottom"]<=price<=nob["top"] or nob["tested"]: score+=20; signals.append(f"At bullish OB ${nob['bottom']}–${nob['top']}")
    if liq["bull_sweeps"] and liq["bull_sweeps"][0]["idx"]>=len(c)-5:
        score+=15; signals.append("Recent bullish liquidity sweep")
    if vbase["nearest"] and vbase["nearest"]["type"]=="accumulation" and vbase["nearest"]["price_in_zone"]:
        score+=15; signals.append(f"Inside accumulation base ({vbase['nearest']['vol_ratio']}x vol)")
    return {"symbol":symbol,"price":round(price,2),"bars":len(c),"dates":{"first":ohlcv["dates"][0],"last":ohlcv["dates"][-1]},"fvg":fvg,"order_block":ob,"choch":choch,"liquidity":liq,"volume_base":vbase,"smc_score":max(0,min(100,score)),"smc_signals":signals,"source":"eodhd","calculated_at":datetime.utcnow().isoformat()}


@app.get("/api/debug/sr")
def debug_sr(symbol: str = Query(default="NVDA")):
    """
    Diagnostic endpoint — shows exactly what's happening with S/R for a symbol.
    Open in browser: http://localhost:3001/api/debug/sr?symbol=NVDA
    """
    symbol = symbol.strip().upper()
    result = {"symbol": symbol, "steps": []}

    # Step 1: EODHD real-time price
    try:
        rt    = _eodhd_realtime([symbol])
        price = rt.get(symbol, {}).get("close")
        result["steps"].append({"step": "realtime_price", "status": "ok", "price": price})
    except Exception as e:
        price = None
        result["steps"].append({"step": "realtime_price", "status": "error", "error": str(e)})

    # Step 2: EODHD EOD history
    from datetime import datetime, timedelta
    date_from = (datetime.utcnow() - timedelta(days=90)).strftime("%Y-%m-%d")
    date_to   = datetime.utcnow().strftime("%Y-%m-%d")
    eod_url   = f"{EODHD_BASE}/eod/{symbol}.US"
    try:
        import requests as req
        r = req.get(eod_url, params={"api_token": EODHD_KEY, "fmt": "json",
                                      "period": "d", "from": date_from, "to": date_to}, timeout=15)
        result["steps"].append({
            "step":       "eodhd_eod_history",
            "status":     "ok" if r.ok else "error",
            "http_code":  r.status_code,
            "url":        eod_url,
            "bars":       len(r.json()) if r.ok else 0,
            "error":      r.text[:200] if not r.ok else None,
        })
        if r.ok:
            data = r.json()
            result["steps"].append({
                "step":   "latest_bar",
                "status": "ok",
                "bar":    data[-1] if data else None,
            })
    except Exception as e:
        result["steps"].append({"step": "eodhd_eod_history", "status": "error", "error": str(e)})

    # Step 3: S/R calculation
    try:
        sr, src = _calc_sr(symbol, price)
        result["steps"].append({"step": "sr_calculation", "status": "ok", "source": src, "sr": sr})
        result["sr"] = sr
        result["source"] = src
    except Exception as e:
        result["steps"].append({"step": "sr_calculation", "status": "error", "error": str(e)})

    result["diagnosis"] = (
        "✓ S/R working" if any(s["step"] == "sr_calculation" and s["status"] == "ok" for s in result["steps"])
        else "✗ S/R failed — check eodhd_eod_history step for the root cause"
    )
    return result

@app.get("/api/smc")
def get_smc(symbol:str=Query(default="NVDA")):
    symbol=symbol.strip().upper(); ck=f"smc:{symbol}"; cached=cache_get(ck)
    if cached: return {"source":"cache","data":cached}
    try:
        data=_run_all_smc(symbol); cache_set(ck,data,900)
        log.info(f"[smc] {symbol}: score={data['smc_score']} bias={data['choch']['bias']}")
        return {"source":"eodhd","data":data}
    except Exception as e:
        log.error(f"[smc] {symbol}: {e}"); raise HTTPException(502,str(e))

@app.get("/api/smc/batch")
def get_smc_batch(symbols:str=Query(default="NVDA,META,PLTR,SOFI,AVGO,CEG,NOW,NFLX,VST,IONQ")):
    sym_list=[s.strip().upper() for s in symbols.split(",") if s.strip()]
    ck="smc:batch:"+",".join(sorted(sym_list)); cached=cache_get(ck)
    if cached: return {"source":"cache","data":cached}
    results,errors={},{}
    for sym in sym_list:
        sk=f"smc:{sym}"; c=cache_get(sk)
        if c: results[sym]=c; continue
        try:
            data=_run_all_smc(sym); cache_set(sk,data,900); results[sym]=data
            log.info(f"[smc/batch] {sym}: score={data['smc_score']} bias={data['choch']['bias']}")
        except Exception as e:
            errors[sym]=str(e); log.warning(f"[smc/batch] {sym}: {e}")
    cache_set(ck,results,900)
    return {"source":"eodhd","data":results,"errors":errors}

# ─────────────────────────────────────────────────────────────────────────
#  ORB + EMA9 + VWAP + Delta — intraday 5-min via EODHD
# ─────────────────────────────────────────────────────────────────────────
def _norm_cdf(x:float) -> float:
    return 0.5 * math.erfc(-x / math.sqrt(2))

def _bs_call_delta(S,K,T,r,sigma) -> float:
    if T<=0 or sigma<=0: return 1.0 if S>K else 0.0
    d1=(math.log(S/K)+(r+0.5*sigma**2)*T)/(sigma*math.sqrt(T))
    return float(_norm_cdf(d1))

def _fetch_intraday_eodhd(symbol:str, interval:str="5m") -> list[dict]:
    """5-min bars from EODHD intraday endpoint."""
    data=_eodhd_get(f"intraday/{symbol}.US", {"interval":interval})
    if not data or not isinstance(data,list):
        raise ValueError(f"No intraday data for {symbol}")
    bars=[]
    for d in data:
        try:
            ts=datetime.utcfromtimestamp(d.get("timestamp",0))
            bars.append({"ts":ts,"open":float(d.get("open",d["close"])),"high":float(d.get("high",d["close"])),"low":float(d.get("low",d["close"])),"close":float(d["close"]),"volume":int(d.get("volume",0) or 0)})
        except Exception: pass
    return bars

def _filter_today_rth(bars:list) -> list:
    """Keep today's 9:30–16:00 ET bars."""
    import pytz
    et=pytz.timezone("America/New_York")
    now_et=datetime.now(et); today=now_et.date()
    result=[]
    for b in bars:
        try:
            ts=b["ts"]
            if hasattr(ts,"replace"):
                ts_et=pytz.utc.localize(ts).astimezone(et) if ts.tzinfo is None else ts.astimezone(et)
            else: continue
            bd=ts_et.date(); bh=ts_et.hour; bm=ts_et.minute
            bmin=bh*60+bm
            if bd==today and 570<=bmin<960:   # 9:30–16:00
                b["ts_et"]=ts_et; result.append(b)
        except Exception: pass
    # Fallback: use last available day
    if not result and bars:
        last_ts=bars[-1]["ts"]
        for b in bars:
            if b["ts"].date()==last_ts.date(): result.append(b)
    return result

def _find_best_strike(price,direction,expiry,iv,delta_low=0.45,delta_high=0.55,r=0.05):
    inc=5.0 if price>300 else 1.0
    atm=round(price/inc)*inc
    try:
        exp_dt=datetime.strptime(expiry,"%Y-%m-%d")
        T=max((exp_dt-datetime.utcnow()).total_seconds()/(365*24*3600),1/365)
    except Exception:
        T=1/365
    candidates=[]
    for offset in range(-10,11):
        strike=round(atm+offset*inc,2)
        if strike<=0: continue
        delta=_bs_call_delta(price,strike,T,r,iv) if direction=="CALL" else _bs_call_delta(price,strike,T,r,iv)-1.0
        delta=abs(delta)
        moneyness=("ATM" if abs(strike-price)/price<0.005 else "ITM" if (direction=="CALL" and strike<price) or (direction=="PUT" and strike>price) else "OTM")
        if delta_low<=delta<=delta_high:
            candidates.append({"strike":round(strike,2),"delta":round(delta,3),"direction":direction,"expiry":expiry,"moneyness":moneyness,"contract":f"{direction[0]}{int(strike)}{expiry[5:]}"})
    candidates.sort(key=lambda x:abs(x["delta"]-0.50))
    return candidates

def _calc_orb_signal(symbol:str="QQQ") -> dict:
    ORB_BARS=6  # 30 min = 6 × 5min
    try:
        bars=_fetch_intraday_eodhd(symbol,"5m")
        today_bars=_filter_today_rth(bars)
    except Exception as e:
        raise ValueError(f"Intraday data failed: {e}")

    if len(today_bars)<ORB_BARS+1:
        return {"symbol":symbol,"status":"premarket" if not today_bars else "insufficient_bars",
                "message":f"Only {len(today_bars)} bars available — need {ORB_BARS+1}+ (market opens 9:30 ET)",
                "orb_bars_needed":ORB_BARS,"bars_available":len(today_bars),
                "orb":{},"current":{},"signal":{"direction":"NONE"},"options":{},"chart_bars":[]}

    o=np.array([b["open"]   for b in today_bars])
    h=np.array([b["high"]   for b in today_bars])
    l=np.array([b["low"]    for b in today_bars])
    c=np.array([b["close"]  for b in today_bars])
    v=np.array([b["volume"] for b in today_bars])

    orb_high=float(np.max(h[:ORB_BARS])); orb_low=float(np.min(l[:ORB_BARS]))
    orb_mid=round((orb_high+orb_low)/2,2); orb_range=round(orb_high-orb_low,2)

    # VWAP
    tp=(h+l+c)/3; vwap=np.cumsum(tp*v)/np.maximum(np.cumsum(v),1)
    # EMA9
    k=2.0/(9+1); ema9=np.empty(len(c)); ema9[0]=c[0]
    for i in range(1,len(c)): ema9[i]=c[i]*k+ema9[i-1]*(1-k)

    # Find breakout
    orb_sig="NONE"; brk_idx=None
    for i in range(ORB_BARS,len(c)):
        av=c[i]>vwap[i]; ae=c[i]>ema9[i]
        bv=c[i]<vwap[i]; be=c[i]<ema9[i]
        if c[i]>orb_high and av and ae and orb_sig=="NONE": orb_sig="BULL"; brk_idx=i; break
        if c[i]<orb_low  and bv and be and orb_sig=="NONE": orb_sig="BEAR"; brk_idx=i; break

    cur_p=float(c[-1]); cur_v=float(vwap[-1]); cur_e=float(ema9[-1])

    # Trade levels
    entry=stop=target=rr=None; iv=0.18; expiry=datetime.utcnow().strftime("%Y-%m-%d")
    candidates=[]; best_strike=None
    if orb_sig!="NONE":
        entry=round(float(c[brk_idx]),2); stop=orb_mid
        target=round(orb_high+1.5*orb_range,2) if orb_sig=="BULL" else round(orb_low-1.5*orb_range,2)
        risk=abs(entry-stop); reward=abs(target-entry)
        rr=round(reward/risk,2) if risk>0 else None
        # Get current IV from EODHD live price as proxy
        try:
            rt=_eodhd_realtime([symbol])
            px=rt.get(symbol,{}).get("close",cur_p)
            atr_daily=px*0.01  # rough 1% daily ATR
            iv=atr_daily/px*math.sqrt(252)  # annualized
        except Exception: iv=0.18
        direction="CALL" if orb_sig=="BULL" else "PUT"
        candidates=_find_best_strike(entry,direction,expiry,iv)
        best_strike=candidates[0] if candidates else None

    # Chart bars
    chart=[]
    for i,b in enumerate(today_bars[-40:]):
        abs_i=max(0,len(today_bars)-40)+i
        ts_str=b.get("ts_et") or b["ts"]
        try: tstr=ts_str.strftime("%H:%M") if hasattr(ts_str,"strftime") else str(ts_str)[-8:-3]
        except: tstr=str(i)
        chart.append({"time":tstr,"open":round(float(o[abs_i]),2),"high":round(float(h[abs_i]),2),"low":round(float(l[abs_i]),2),"close":round(float(c[abs_i]),2),"volume":int(v[abs_i]),"vwap":round(float(vwap[abs_i]),2),"ema9":round(float(ema9[abs_i]),2),"is_orb":abs_i<ORB_BARS,"is_breakout":abs_i==brk_idx})

    return {"symbol":symbol,"status":"signal" if orb_sig!="NONE" else "watching",
            "orb":{"high":round(orb_high,2),"low":round(orb_low,2),"mid":orb_mid,"range":orb_range,"range_pct":round(orb_range/max(orb_low,1)*100,3),"bars":ORB_BARS},
            "current":{"price":round(cur_p,2),"vwap":round(cur_v,2),"ema9":round(cur_e,2),"above_vwap":cur_p>cur_v,"above_ema9":cur_p>cur_e,"price_vs_orb":"above_high" if cur_p>orb_high else "below_low" if cur_p<orb_low else "inside_range"},
            "signal":{"direction":orb_sig,"bar_idx":brk_idx,"bar_time":str(today_bars[brk_idx].get("ts_et",today_bars[brk_idx]["ts"])) if brk_idx is not None else None,"entry_price":entry,"stop_price":stop,"target_price":target,"risk_reward":rr,"vwap_at_signal":round(float(vwap[brk_idx]),2) if brk_idx is not None else None,"ema9_at_signal":round(float(ema9[brk_idx]),2) if brk_idx is not None else None},
            "options":{"direction":"CALL" if orb_sig=="BULL" else "PUT" if orb_sig=="BEAR" else None,"expiry":expiry,"iv":round(iv*100,1),"delta_target":"0.45–0.55","best_strike":best_strike,"candidates":candidates[:5]},
            "chart_bars":chart,"total_bars":len(today_bars),"source":"eodhd 5m","calculated_at":datetime.utcnow().isoformat()}

@app.get("/api/orb")
def get_orb(symbol:str=Query(default="QQQ")):
    symbol=symbol.strip().upper(); ck=f"orb:{symbol}"; cached=cache_get(ck)
    if cached: return {"source":"cache (2min)","data":cached}
    try:
        data=_calc_orb_signal(symbol); cache_set(ck,data,120)
        log.info(f"[orb] {symbol}: {data['signal']['direction']} | ORB H={data['orb'].get('high')} L={data['orb'].get('low')}")
        return {"source":"eodhd 5m","data":data}
    except Exception as e:
        log.error(f"[orb] {symbol}: {e}"); raise HTTPException(502,str(e))

# ─────────────────────────────────────────────────────────────────────────
#  BIG MONEY — options via yfinance (lazy import, won't crash startup)
# ─────────────────────────────────────────────────────────────────────────
def _safe_float(v,d=0.0):
    try: return float(v) if v is not None and str(v) not in ("nan","None","") else d
    except: return d
def _safe_int(v,d=0):
    try: return int(float(v)) if v is not None and str(v) not in ("nan","None","") else d
    except: return d

def _get_options_chain(symbol:str) -> dict:
    """Lazy-import yfinance for options only."""
    try:
        import yfinance as yf
    except ImportError:
        raise ValueError("yfinance not installed — run: pip install yfinance")
    ticker=yf.Ticker(symbol)
    exps=ticker.options
    if not exps: raise ValueError(f"No options expiries for {symbol}")
    target=list(exps[:3]); calls=[]; puts=[]
    for exp in target:
        try:
            chain=ticker.option_chain(exp)
            for _,row in chain.calls.iterrows():
                d=row.to_dict(); d["expiry"]=exp; d["side"]="CALL"; calls.append(d)
            for _,row in chain.puts.iterrows():
                d=row.to_dict(); d["expiry"]=exp; d["side"]="PUT"; puts.append(d)
        except Exception as e:
            log.warning(f"[bigmoney] {symbol} chain {exp}: {e}")
    if not calls: raise ValueError(f"No options data for {symbol}")
    return {"calls":calls,"puts":puts,"expiries":target}

def _calc_pcr(cv,pv,co,po):
    tv=cv+pv
    return {"volume_pcr":round(pv/max(cv,1),3),"oi_pcr":round(po/max(co,1),3),"total_call_vol":int(cv),"total_put_vol":int(pv),"total_call_oi":int(co),"total_put_oi":int(po),"call_pct":round(cv/max(tv,1)*100,1),"put_pct":round(pv/max(tv,1)*100,1)}

def _calc_max_pain(calls,puts,price):
    coi={}; poi={}
    for r in calls:
        s=_safe_float(r.get("strike"))
        if s>0: coi[s]=coi.get(s,0)+_safe_int(r.get("openInterest"))
    for r in puts:
        s=_safe_float(r.get("strike"))
        if s>0: poi[s]=poi.get(s,0)+_safe_int(r.get("openInterest"))
    strikes=sorted(set(list(coi)+list(poi)))
    if not strikes: return {"max_pain":round(price,2),"price":round(price,2),"dist_pct":0,"top_strikes":[]}
    mp=float("inf"); ms=strikes[0]
    for s in strikes:
        cp=sum(v for k,v in coi.items() if k<=s)
        pp=sum(v for k,v in poi.items() if k>=s)
        if cp+pp<mp: mp=cp+pp; ms=s
    sd=[{"strike":s,"call_oi":coi.get(s,0),"put_oi":poi.get(s,0),"total_oi":coi.get(s,0)+poi.get(s,0)} for s in strikes]
    sd.sort(key=lambda x:x["total_oi"],reverse=True); top=sorted(sd[:14],key=lambda x:x["strike"])
    return {"max_pain":round(float(ms),2),"price":round(price,2),"dist_pct":round((price-float(ms))/float(ms)*100,2),"top_strikes":[{**r,"is_max_pain":float(r["strike"])==float(ms)} for r in top]}

def _calc_iv_rank(calls,symbol):
    ivs=[_safe_float(r.get("impliedVolatility")) for r in calls if _safe_float(r.get("impliedVolatility"))>0]
    if not ivs: return {"iv_current":None,"iv_rank":None,"iv_label":"unknown","hv_20d":None,"best_strategy":"unknown"}
    ivs.sort(); mid=len(ivs)//2
    iv_c=(ivs[mid-1]+ivs[mid])/2 if len(ivs)%2==0 else ivs[mid]
    iv_cur=round(iv_c*100,1)
    try:
        ohlcv=_fetch_eod_history(symbol,days=252)
        c=np.array(ohlcv["closes"]); lr=np.diff(np.log(c))
        hv20=round(float(np.std(lr[-20:])*np.sqrt(252)*100),1)
        iv_low=round(hv20*0.7,1); iv_high=round(hv20*1.8,1)
        iv_rank=round(max(0,min(100,(iv_cur-iv_low)/max(iv_high-iv_low,1)*100)),1)
        label="High IV (sell premium)" if iv_rank>70 else "Low IV (buy premium)" if iv_rank<30 else "Normal IV"
        strategy="Sell premium (strangles/iron condors)" if iv_rank>70 else "Buy options (debit spreads)" if iv_rank<30 else "Directional spreads"
    except Exception:
        iv_rank=None; hv20=None; label="unknown"; strategy="unknown"
    return {"iv_current":iv_cur,"iv_rank":iv_rank,"hv_20d":hv20,"iv_label":label,"best_strategy":strategy}

def _unusual_options(calls,puts,price):
    enriched=[]
    for r in calls+puts:
        side=r.get("side","CALL"); strike=_safe_float(r.get("strike")); vol=_safe_int(r.get("volume"))
        oi=_safe_int(r.get("openInterest")); last=_safe_float(r.get("lastPrice")); iv=_safe_float(r.get("impliedVolatility"))
        expiry=str(r.get("expiry","")); ratio=vol/max(oi,1); notional=vol*last*100
        mono=("ATM" if abs(strike-price)/max(price,1)<0.02 else "ITM" if (side=="CALL" and strike<price) or (side=="PUT" and strike>price) else "OTM")
        enriched.append({"side":side,"strike":round(strike,2),"expiry":expiry,"volume":vol,"open_interest":oi,"vol_oi_ratio":round(ratio,1),"last_price":round(last,2),"notional":round(notional),"iv":round(iv*100,1),"moneyness":mono,"bias":"BULL" if side=="CALL" else "BEAR","contract":f"{side[0]}{int(strike)}{expiry[-5:]}"})
    unusual=[r for r in enriched if r["vol_oi_ratio"]>3 and r["volume"]>500 and r["notional"]>50000]
    if not unusual: unusual=sorted(enriched,key=lambda x:x["notional"],reverse=True)[:8]
    return sorted(unusual,key=lambda x:x["notional"],reverse=True)[:12]

def _calc_bigmoney(symbol:str) -> dict:
    price=float(_eodhd_realtime([symbol]).get(symbol,{}).get("close",0))
    if price<=0: raise ValueError(f"No price for {symbol}")
    opts=_get_options_chain(symbol)
    calls=opts["calls"]; puts=opts["puts"]
    cv=sum(_safe_int(r.get("volume")) for r in calls); pv=sum(_safe_int(r.get("volume")) for r in puts)
    co=sum(_safe_int(r.get("openInterest")) for r in calls); po=sum(_safe_int(r.get("openInterest")) for r in puts)
    pcr=_calc_pcr(cv,pv,co,po); mp=_calc_max_pain(calls,puts,price)
    iv=_calc_iv_rank(calls,symbol); unusual=_unusual_options(calls,puts,price)
    vpcr=pcr["volume_pcr"]
    psig="Bullish — heavy call buying" if vpcr<0.6 else "Bearish — heavy put buying" if vpcr>1.2 else "Slightly bearish / hedging" if vpcr>0.9 else "Neutral"
    pbias="bullish" if vpcr<0.6 else "bearish" if vpcr>1.2 else "neutral"
    bf=sum(u["notional"] for u in unusual if u["bias"]=="BULL"); bear_f=sum(u["notional"] for u in unusual if u["bias"]=="BEAR"); tf=bf+bear_f
    fb="bullish" if bf>bear_f*1.2 else "bearish" if bear_f>bf*1.2 else "neutral"
    return {"symbol":symbol,"price":round(price,2),"expiries":opts["expiries"],"pcr":pcr,"pcr_signal":psig,"pcr_bias":pbias,"max_pain":mp,"iv":iv,"unusual":unusual,"flow_summary":{"bull_notional":round(bf),"bear_notional":round(bear_f),"total_notional":round(tf),"bias":fb,"bull_pct":round(bf/max(tf,1)*100,1),"bear_pct":round(bear_f/max(tf,1)*100,1)},"source":"yfinance (options)","calculated_at":datetime.utcnow().isoformat()}

@app.get("/api/bigmoney")
def get_bigmoney(symbol:str=Query(default="QQQ")):
    symbol=symbol.strip().upper(); ck=f"bigmoney:{symbol}"; cached=cache_get(ck)
    if cached: return {"source":"cache","data":cached}
    try:
        data=_calc_bigmoney(symbol); cache_set(ck,data,600)
        log.info(f"[bigmoney] {symbol}: PCR={data['pcr']['volume_pcr']} MaxPain=${data['max_pain']['max_pain']}")
        return {"source":"yfinance","data":data}
    except Exception as e:
        log.error(f"[bigmoney] {symbol}: {e}"); raise HTTPException(502,str(e))

@app.get("/api/bigmoney/batch")
def get_bigmoney_batch(symbols:str=Query(default="QQQ,SPY,NVDA,META,PLTR")):
    sym_list=[s.strip().upper() for s in symbols.split(",") if s.strip()]
    results,errors={},{}
    for sym in sym_list:
        ck=f"bigmoney:{sym}"; c=cache_get(ck)
        if c: results[sym]=c; continue
        try:
            data=_calc_bigmoney(sym); cache_set(ck,data,600); results[sym]=data
        except Exception as e:
            errors[sym]=str(e); log.warning(f"[bigmoney/batch] {sym}: {e}")
    return {"source":"yfinance","data":results,"errors":errors}



@app.get("/api/usage")
def get_usage():
    """
    Monitor EODHD API call usage today.
    Free plan = 20 calls/day. Check this to know when fallback kicks in.
    Open: http://localhost:3001/api/usage
    """
    today  = datetime.utcnow().strftime("%Y-%m-%d")
    count  = _eodhd_call_count["count"] if _eodhd_call_count["date"] == today else 0
    limit  = _eodhd_call_count.get("limit_hit", False)
    budget = 20  # free plan default — update if you have paid plan

    return {
        "date":              today,
        "eodhd_calls_today": count,
        "free_plan_limit":   budget,
        "calls_remaining":   max(0, budget - count),
        "limit_hit":         limit,
        "status":            "⚠ LIMIT HIT — using Yahoo Finance fallback" if limit
                             else f"✓ {count}/{budget} calls used today",
        "note": (
            "EODHD free plan = 20 calls/day. Paid from $19.99/mo for higher limits. "
            "When limit is hit, server automatically switches to Yahoo Finance for prices and history."
        ),
        "sources_active": {
            "prices":     "yahoo (fallback)" if limit else "eodhd (primary)",
            "history":    "yahoo (fallback)" if limit else "eodhd (primary)",
            "indicators": "yahoo (fallback)" if limit else "eodhd (primary)",
            "sr":         "yahoo (fallback)" if limit else "eodhd (primary)",
            "news":       "finnhub / rss (not affected by EODHD limit)",
            "options":    "yfinance (not affected by EODHD limit)",
        }
    }

# ─────────────────────────────────────────────────────────────────────────
#  NEWS
# ─────────────────────────────────────────────────────────────────────────
def _norm_article(a,extra={}):
    return {"headline":a.get("headline") or a.get("title",""),"summary":a.get("summary",""),"source":a.get("source",""),"url":a.get("url",""),"image":a.get("image",""),"datetime":a.get("datetime",0),"related":a.get("related",""),"category":a.get("category","general"),**extra}

def _fh_news(cat):
    r=requests.get(f"{FINNHUB_BASE}/news",params={"category":cat,"token":FINNHUB_KEY},headers={"X-Finnhub-Token":FINNHUB_KEY},timeout=10)
    if r.status_code==401: raise PermissionError("Finnhub 401")
    r.raise_for_status()
    raw=r.json()
    if not isinstance(raw,list): raise ValueError("Bad response")
    return [_norm_article(a) for a in raw[:25]]

def _fh_company(symbol):
    today=datetime.utcnow().strftime("%Y-%m-%d"); wago=(datetime.utcnow()-timedelta(days=7)).strftime("%Y-%m-%d")
    r=requests.get(f"{FINNHUB_BASE}/company-news",params={"symbol":symbol,"from":wago,"to":today,"token":FINNHUB_KEY},headers={"X-Finnhub-Token":FINNHUB_KEY},timeout=10)
    if r.status_code==401: raise PermissionError("Finnhub 401")
    r.raise_for_status(); raw=r.json()
    if not isinstance(raw,list): raise ValueError("Bad response")
    return [_norm_article(a,{"related":symbol}) for a in raw[:25]]

def _rss_general():
    feeds=["https://feeds.finance.yahoo.com/rss/2.0/headline?s=^GSPC,^IXIC&region=US&lang=en-US","https://www.cnbc.com/id/100003114/device/rss/rss.html","https://feeds.marketwatch.com/marketwatch/topstories/"]
    arts=[]; seen=set()
    for url in feeds:
        try:
            feed=feedparser.parse(url)
            for e in feed.entries[:10]:
                t=e.get("title","").strip()
                if not t or t in seen: continue
                seen.add(t)
                ts=int(calendar.timegm(e.published_parsed)) if getattr(e,"published_parsed",None) else 0
                arts.append(_norm_article({"headline":t,"summary":e.get("summary","")[:200],"source":feed.feed.get("title","News"),"url":e.get("link",""),"datetime":ts}))
        except Exception as ex: log.debug(f"RSS {url}: {ex}")
    arts.sort(key=lambda x:x["datetime"],reverse=True); return arts[:25]

def _rss_company(symbol):
    url=f"https://feeds.finance.yahoo.com/rss/2.0/headline?s={symbol}&region=US&lang=en-US"; arts=[]
    try:
        feed=feedparser.parse(url)
        for e in feed.entries[:20]:
            t=e.get("title","").strip()
            if not t: continue
            ts=int(calendar.timegm(e.published_parsed)) if getattr(e,"published_parsed",None) else 0
            arts.append(_norm_article({"headline":t,"summary":e.get("summary","")[:200],"source":feed.feed.get("title","Yahoo Finance"),"url":e.get("link",""),"datetime":ts,"related":symbol}))
    except Exception as e: log.debug(f"RSS company {symbol}: {e}")
    return arts

@app.get("/api/news")
def get_news(category:str=Query(default="general")):
    ck=f"news:{category}"; cached=cache_get(ck)
    if cached: return {"source":cached["_src"]+" (cached)","articles":cached["articles"]}
    errors=[]
    try:
        arts=_fh_news(category)
        if arts: cache_set(ck,{"articles":arts,"_src":"finnhub"},300); return {"source":"finnhub","articles":arts}
        errors.append("Finnhub empty")
    except Exception as e: errors.append(str(e)); log.warning(f"[news] {e}")
    try:
        arts=_rss_general()
        if arts: cache_set(ck,{"articles":arts,"_src":"rss"},300); return {"source":"rss","articles":arts,"finnhub_errors":errors}
    except Exception as e: errors.append(str(e))
    raise HTTPException(502,{"errors":errors})

@app.get("/api/company-news")
def get_company_news(symbol:str=Query(default="NVDA")):
    symbol=symbol.strip().upper(); today=datetime.utcnow().strftime("%Y-%m-%d")
    ck=f"news:company:{symbol}:{today}"; cached=cache_get(ck)
    if cached: return {"source":cached["_src"]+" (cached)","articles":cached["articles"]}
    errors=[]
    try:
        arts=_fh_company(symbol)
        if arts: cache_set(ck,{"articles":arts,"_src":"finnhub"},300); return {"source":"finnhub","articles":arts}
        errors.append(f"Finnhub empty for {symbol}")
    except Exception as e: errors.append(str(e)); log.warning(f"[company-news] {e}")
    try:
        arts=_rss_company(symbol)
        if arts: cache_set(ck,{"articles":arts,"_src":"rss"},300); return {"source":"rss","articles":arts,"finnhub_errors":errors}
    except Exception as e: errors.append(str(e))
    raise HTTPException(502,{"errors":errors})

# ─────────────────────────────────────────────────────────────────────────
#  AI PROXY
# ─────────────────────────────────────────────────────────────────────────
class AIRequest(BaseModel):
    prompt: str
    system: Optional[str] = None
    max_tokens: int = 1000

@app.post("/api/ai")
def ai_proxy(req:AIRequest):
    if not ANTHROPIC_KEY:
        raise HTTPException(400,{"error":"ANTHROPIC_API_KEY not set in .env","fix":"Add ANTHROPIC_API_KEY=sk-ant-... to .env then restart"})
    payload={"model":"claude-sonnet-4-6","max_tokens":req.max_tokens,"messages":[{"role":"user","content":req.prompt}]}
    if req.system: payload["system"]=req.system
    try:
        r=requests.post(ANTHROPIC_URL,json=payload,headers={"x-api-key":ANTHROPIC_KEY,"anthropic-version":"2023-06-01","content-type":"application/json"},timeout=30)
        if not r.ok: raise HTTPException(r.status_code,r.json())
        d=r.json()
        return {"text":"".join(b.get("text","") for b in d.get("content",[])),"model":d.get("model"),"usage":d.get("usage")}
    except HTTPException: raise
    except Exception as e: raise HTTPException(502,str(e))

if __name__=="__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=PORT, reload=False, log_level="info")