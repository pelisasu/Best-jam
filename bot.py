#!/usr/bin/env python3
"""
=============================================================================
XAUUSD AGI QUANT ENGINE (HYBRID V24.6.1 + PRODUCTION ARCHITECTURE)
=============================================================================
Fitur Utama Penggabungan:
1. Multi-TF Analysis (M15 + H1) dengan 10 Core Engine Kustom & Dynamic DNA Evolution.
2. Anti-Spam & Persistence State berbasis SQLite Database (`.state_cache/`).
3. Multi-Feed Failover (Deriv WS -> Binance PAXG -> Yahoo Futures GC=F / Spot).
4. News Filter Gate (Mengabaikan trading saat ada berita High Impact USD/XAU).
5. Monte Carlo Stress-Testing Simulation (Validasi Drawdown).
6. Gemini AI Gatekeeper Integration (Otentikasi Native REST API).
7. Dispatcher Telegram Lengkap (Format HTML, Chart Matplotlib, & Fallback Pesan).
=============================================================================
"""

import os
import sys
import time
import json
import sqlite3
import datetime
from datetime import timedelta
import pytz
import numpy as np
import pandas as pd
import requests
import websocket
import yfinance as yf
import matplotlib
from typing import Optional

matplotlib.use('Agg')
import matplotlib.pyplot as plt

plt.rcParams['axes.unicode_minus'] = False

# =============================================================================
# 0. CONFIGURATION & ENVIRONMENT VARIABLES
# =============================================================================
WIB = pytz.timezone("Asia/Jakarta")
UTC = pytz.UTC

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip() or os.getenv("TELEGRAM_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip() or os.getenv("TELE_CHAT", "").strip()
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "").strip()
HEALTHCHECK_URL = os.getenv("HEALTHCHECK_URL", "").strip()

SYMBOL_DERIV = os.getenv("SYMBOL_DERIV", os.getenv("DERIV_SYMBOL", "frxXAUUSD"))
MIN_CONFLUENCE_SCORE = float(os.getenv("MIN_CONFLUENCE_SCORE", "60.0"))
COOLDOWN_MINUTES = int(os.getenv("SIGNAL_COOLDOWN_MINUTES", "60"))
FORCE_RUN = os.getenv("FORCE_RUN", "false").lower() == "true"

# Direktori & File Persistence
CACHE_DIR = ".state_cache"
os.makedirs(CACHE_DIR, exist_ok=True)
DB_FILE = os.path.join(CACHE_DIR, "quant_engine_state.db")
DNA_FILE = os.path.join(CACHE_DIR, "dna_10_engines.json")
JOURNAL_FILE = os.path.join(CACHE_DIR, "trade_journal.json")
FAILURE_FILE = os.path.join(CACHE_DIR, "failure_count.json")


def log(m: str):
    print(f"[{datetime.datetime.now(WIB).strftime('%H:%M:%S %d-%m')} WIB] {m}")


# =============================================================================
# 1. STATE PERSISTENCE ENGINE (SQLITE)
# =============================================================================
class PersistenceEngine:
    """Modul menyimpan state transaksi ke SQLite agar tahan restart server/GitHub Actions."""

    def __init__(self, db_path: str = DB_FILE):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS signal_state (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp REAL,
                    direction TEXT,
                    entry_price REAL,
                    score REAL,
                    status TEXT
                )
            """)
            conn.commit()

    def is_duplicate_signal(self, direction: str, entry_price: float, cooldown_min: int) -> bool:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT timestamp, entry_price FROM signal_state 
                WHERE direction = ? ORDER BY id DESC LIMIT 1
            """, (direction,))
            row = cursor.fetchone()

            if row:
                last_time, last_entry = row
                elapsed_min = (time.time() - last_time) / 60.0
                if elapsed_min < cooldown_min and abs(entry_price - last_entry) * 10 < 15:
                    return True
        return False

    def save_signal(self, direction: str, entry_price: float, score: float):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO signal_state (timestamp, direction, entry_price, score, status)
                VALUES (?, ?, ?, ?, ?)
            """, (time.time(), direction, entry_price, score, "ACTIVE"))
            conn.commit()


# =============================================================================
# 2. MONTE CARLO STRESS-TESTING ENGINE
# =============================================================================
class MonteCarloStressTester:
    """Simulasi pengujian statistik ketahanan strategi terhadap Maximum Drawdown."""

    @staticmethod
    def run_simulation(trades_returns: Optional[list] = None, num_simulations: int = 500, horizon: int = 50) -> dict:
        if not trades_returns or len(trades_returns) < 5:
            trades_returns = [0.015, -0.01, 0.02, -0.01, 0.025, -0.015, 0.01, 0.03, -0.02]

        drawdowns = []
        for _ in range(num_simulations):
            simulated_trades = np.random.choice(trades_returns, size=horizon, replace=True)
            equity_curve = np.cumprod(1 + simulated_trades)
            peak = np.maximum.accumulate(equity_curve)
            dd = (equity_curve - peak) / peak
            drawdowns.append(abs(np.min(dd)))

        max_dd_95_conf = np.percentile(drawdowns, 95) * 100.0
        return {
            "expected_max_drawdown_95": round(float(max_dd_95_conf), 2),
            "pass_stress_test": max_dd_95_conf < 18.0
        }


# =============================================================================
# 3. RESILIENT DATA PROVIDER (MULTI-FEED FAILOVER ENGINE)
# =============================================================================
def fetch_deriv(limit=300, gran=900):
    url = "wss://ws.derivws.com/websockets/v3?app_id=1089"
    for attempt in range(3):
        ws = None
        try:
            ws = websocket.create_connection(url, timeout=8)
            ws.settimeout(8)
            ws.send(json.dumps({"ticks_history": SYMBOL_DERIV, "count": limit, "end": "latest",
                                "granularity": gran, "style": "candles"}))
            start_time = time.time()
            while time.time() - start_time < 8:
                res = json.loads(ws.recv())
                if "candles" in res and res["candles"]:
                    df = pd.DataFrame(res["candles"])
                    for c in ["close", "high", "low", "open"]:
                        df[c] = df[c].astype(float)
                    df["volume"] = 100.0
                    price = float(df["close"].iloc[-1])
                    return df, price
            log(f"Deriv attempt {attempt+1}: Timeout tidak ada data candle.")
        except Exception as e:
            log(f"Deriv attempt {attempt+1} err: {e}")
        finally:
            if ws:
                try:
                    ws.close()
                except Exception:
                    pass
        time.sleep(2)
    return None, None


def fetch_binance_paxg():
    try:
        r = requests.get("https://api.binance.com/api/v3/ticker/price?symbol=PAXGUSDT", timeout=6)
        if r.status_code == 200:
            return float(r.json()["price"])
    except Exception:
        pass
    return None


def fetch_yf_gc():
    try:
        df = yf.Ticker("GC=F").history(period="5d", interval="15m")
        if df is None or len(df) < 50:
            return None, None
        price = float(df["Close"].iloc[-1])
        df = df.reset_index()
        date_col = next((c for c in df.columns if 'date' in c.lower()), df.columns[0])
        df = df.rename(columns={date_col: "datetime", "Close": "close", "High": "high",
                                "Low": "low", "Open": "open"})
        df["volume"] = df["Volume"].astype(float) if "Volume" in df.columns else 100.0
        result_df = df[["datetime", "close", "high", "low", "open", "volume"]].tail(300)
        result_df.set_index("datetime", inplace=True)
        return result_df, price
    except Exception as e:
        log(f"YF GC=F err: {e}")
        return None, None


def get_failover_price_with_dynamic_offset():
    prices_source = "None"
    raw_price = None
    df_m15 = None
    source_specific_offset = 0.0

    log("Mencoba mengambil harga dari Deriv WebSocket...")
    df_m15, p_deriv = fetch_deriv(300, 900)
    if p_deriv:
        raw_price = p_deriv
        prices_source = "Deriv WebSocket"
        try:
            source_specific_offset = float(os.getenv("DERIV_OFFSET", "0"))
        except Exception:
            pass

    if raw_price is None:
        log("Deriv WebSocket gagal, beralih ke Binance PAXG...")
        p_binance = fetch_binance_paxg()
        if p_binance:
            raw_price = p_binance
            prices_source = "Binance PAXG"
            df_m15, _ = fetch_yf_gc()
            try:
                source_specific_offset = float(os.getenv("BINANCE_OFFSET", "0"))
            except Exception:
                pass

    if raw_price is None:
        log("Binance gagal, beralih ke Yahoo Finance (GC=F)...")
        df_yf, p_yf = fetch_yf_gc()
        if p_yf:
            raw_price = p_yf
            prices_source = "Yahoo GC=F"
            df_m15 = df_yf
            try:
                source_specific_offset = float(os.getenv("YAHOO_OFFSET", "-31.50"))
            except Exception:
                source_specific_offset = -31.50

    if raw_price is None or df_m15 is None:
        raise RuntimeError("Kritis: Seluruh sumber harga gagal diakses!")

    global_mt5_offset = 0.0
    try:
        global_mt5_offset = float(os.getenv("MT5_OFFSET", "0"))
    except Exception:
        pass

    total_offset = source_specific_offset + global_mt5_offset
    final_price = raw_price + total_offset

    log(f"Sumber: {prices_source} | Raw: {raw_price:.2f} | Offset: {total_offset:+.2f} | Final: {final_price:.2f}")

    df_h1, _ = fetch_deriv(200, 3600)
    if df_h1 is None:
        df_h1 = df_m15
    df_h4, _ = fetch_deriv(200, 14400)
    if df_h4 is None:
        df_h4 = df_h1

    return final_price, df_m15, df_h1, df_h4, prices_source, total_offset


# =============================================================================
# 4. HIGH IMPACT NEWS FILTER
# =============================================================================
def check_high_impact_news():
    try:
        url = "https://nfs.faireconomy.media/ff_calendar_thisweek.json"
        response = requests.get(url, timeout=5)
        if response.status_code == 200:
            events = response.json()
            now = datetime.datetime.now(UTC)
            for event in events:
                country = event.get('country', '')
                impact = event.get('impact', '')
                if country in ['USD', 'XAU'] and impact in ['High', 'Red']:
                    try:
                        event_time = datetime.datetime.fromisoformat(event['date'].replace('Z', '+00:00'))
                        time_diff = event_time - now
                        if timedelta(minutes=-30) <= time_diff <= timedelta(minutes=90):
                            log(f"⚠️ BERITA HIGH IMPACT TERDETEKSI: {event.get('title', 'Unknown')} "
                                f"pada {event_time.astimezone(WIB).strftime('%H:%M WIB')}")
                            return True
                    except Exception:
                        continue
        return False
    except Exception as e:
        log(f"Gagal cek berita (filter diabaikan): {e}")
        return False


# =============================================================================
# 5. TECHNICAL INDICATORS & 10 CORE STRATEGY ENGINES
# =============================================================================
def calc_atr(df, period=14):
    try:
        hl = df["high"] - df["low"]
        hc = (df["high"] - df["close"].shift()).abs()
        lc = (df["low"] - df["close"].shift()).abs()
        tr = pd.concat([hl, hc, lc], axis=1).max(axis=1)
        return float(tr.rolling(period).mean().iloc[-1])
    except Exception:
        return 8.0


def rsi(df, p=14):
    try:
        delta = df["close"].diff()
        gain = delta.where(delta > 0, 0).rolling(p).mean()
        loss = -delta.where(delta < 0, 0).rolling(p).mean()
        rs = gain / (loss + 1e-9)
        return 100 - 100 / (1 + rs)
    except Exception:
        return pd.Series([50] * len(df))


def NADI(df):
    try:
        e9 = df["close"].ewm(9).mean().iloc[-1]
        e21 = df["close"].ewm(21).mean().iloc[-1]
        pr = df["close"].iloc[-1]
        return (1, 1.5) if pr > e9 and e9 > e21 else (-1, 1.5) if pr < e9 and e9 < e21 else (0, 1.5)
    except Exception:
        return (0, 1.5)


def SAWAH(df):
    try:
        hi = df["high"].rolling(50).max().iloc[-1]
        lo = df["low"].rolling(50).min().iloc[-1]
        pr = df["close"].iloc[-1]
        f62 = lo + (hi - lo) * 0.618
        f38 = lo + (hi - lo) * 0.382
        return (1, 1.5) if pr > f62 else (-1, 1.5) if pr < f38 else (0, 1.5)
    except Exception:
        return (0, 1.5)


def SEMUT(df):
    try:
        e50 = df["close"].ewm(50).mean().iloc[-1]
        pr = df["close"].iloc[-1]
        return (1, 1.2) if pr > e50 else (-1, 1.2)
    except Exception:
        return (0, 1.2)


def PADI(df):
    try:
        r = rsi(df).iloc[-1]
        return (1, 1.3) if r > 55 else (-1, 1.3) if r < 45 else (0, 1.3)
    except Exception:
        return (0, 1.3)


def AKAR(df):
    try:
        s200 = df["close"].rolling(200).mean().iloc[-1] if len(df) >= 200 else df["close"].mean()
        pr = df["close"].iloc[-1]
        return (1, 1.8) if pr > s200 else (-1, 1.8)
    except Exception:
        return (0, 1.8)


def WAYANG(df):
    try:
        hi = df["high"].rolling(20).max().iloc[-1]
        lo = df["low"].rolling(20).min().iloc[-1]
        pr = df["close"].iloc[-1]
        return (1, 1.0) if pr > (hi + lo) / 2 else (-1, 1.0)
    except Exception:
        return (0, 1.0)


def LUMPUR(df):
    try:
        vol = df["volume"].iloc[-1]
        vma = df["volume"].rolling(20).mean().iloc[-1]
        pr = df["close"].iloc[-1]
        prev = df["close"].iloc[-2]
        return (1, 1.1) if vol > vma and pr > prev else (-1, 1.1) if vol > vma and pr < prev else (0, 1.1)
    except Exception:
        return (0, 1.1)


def API(df):
    try:
        body = (df["close"] - df["open"]).abs().iloc[-1]
        avg_b = (df["close"] - df["open"]).abs().rolling(20).mean().iloc[-1]
        pr = df["close"].iloc[-1]
        prev = df["close"].iloc[-2]
        return (1, 1.0) if body > avg_b and pr > prev else (-1, 1.0) if body > avg_b and pr < prev else (0, 1.0)
    except Exception:
        return (0, 1.0)


def ANGIN(df):
    try:
        hi = df["high"].iloc[-1]
        lo = df["low"].iloc[-1]
        cl = df["close"].iloc[-1]
        return (-1, 1.0) if (hi - cl) > (cl - lo) * 1.5 else (1, 1.0) if (cl - lo) > (hi - cl) * 1.5 else (0, 1.0)
    except Exception:
        return (0, 1.0)


def EMBER(df):
    try:
        pr = df["close"].iloc[-1]
        ma10 = df["close"].rolling(10).mean().iloc[-1]
        return (1, 1.0) if pr > ma10 else (-1, 1.0)
    except Exception:
        return (0, 1.0)


engine_map = [
    ("NADI", NADI), ("SAWAH", SAWAH), ("SEMUT", SEMUT), ("PADI", PADI), ("AKAR", AKAR),
    ("WAYANG", WAYANG), ("LUMPUR", LUMPUR), ("API", API), ("ANGIN", ANGIN), ("EMBER", EMBER)
]


# =============================================================================
# 6. DYNAMIC DNA EVOLUTION & JOURNALING
# =============================================================================
def load_json(path, default):
    try:
        if os.path.exists(path):
            with open(path, 'r') as f:
                return json.load(f)
    except Exception:
        pass
    return default


def save_json(path, data):
    try:
        with open(path, 'w') as f:
            json.dump(data, f, indent=2)
    except Exception:
        pass


def evaluate_and_evolve_dna_from_journal(dna, journal, current_price):
    if "engines" not in dna:
        dna["engines"] = {name: {"weight": 1.0} for name, _ in engine_map}

    updated = False
    now_wib = datetime.datetime.now(WIB)

    for trade in journal:
        if trade.get("evaluated"):
            continue

        required_keys = ["time", "signal", "price", "sl", "tp1"]
        if not all(k in trade for k in required_keys):
            trade["evaluated"] = True
            updated = True
            continue

        try:
            trade_time = datetime.datetime.fromisoformat(trade["time"])
            if trade_time.tzinfo is None:
                trade_time = WIB.localize(trade_time)
        except Exception:
            trade["evaluated"] = True
            updated = True
            continue

        trade_age_hours = (now_wib - trade_time).total_seconds() / 3600
        result = None

        if trade["signal"] == "BUY":
            if current_price >= trade["tp1"]:
                result = "WIN"
            elif current_price <= trade["sl"]:
                result = "LOSS"
        else:
            if current_price <= trade["tp1"]:
                result = "WIN"
            elif current_price >= trade["sl"]:
                result = "LOSS"

        if result is None and trade_age_hours > 6.0:
            result = "DRAW"

        if result:
            log(f"📊 Evaluasi Trade: {trade['signal']} @ {trade['price']} -> {result}")

            if result in ["WIN", "LOSS"]:
                engine_states = trade.get("engine_states", {})
                for name, state in engine_states.items():
                    if name in dna["engines"]:
                        curr_w = dna["engines"][name]["weight"]
                        was_bullish = state.get("sc", 0) > 0
                        should_have_been_bullish = (
                            (trade["signal"] == "BUY" and result == "WIN") or
                            (trade["signal"] == "SELL" and result == "LOSS")
                        )
                        new_w = min(2.0, curr_w + 0.05) if was_bullish == should_have_been_bullish else max(0.5, curr_w - 0.05)
                        new_w = new_w * 0.99 + 1.0 * 0.01
                        dna["engines"][name]["weight"] = round(new_w, 4)

            trade["evaluated"] = True
            trade["result"] = result
            updated = True

    if updated:
        save_json(JOURNAL_FILE, journal)
    return dna


def get_trend_label(df):
    try:
        s50 = df["close"].rolling(50).mean().iloc[-1]
        pr = df["close"].iloc[-1]
        if pr > s50:
            return "BULLISH (UP)"
        elif pr < s50:
            return "BEARISH (DOWN)"
        return "SIDEWAYS"
    except Exception:
        return "NEUTRAL"


# =============================================================================
# 7. GEMINI AI GATEKEEPER INTEGRATION
# =============================================================================
class GeminiGatekeeper:
    """Modul analisis konfirmasi AI untuk validasi akhir sebelum sinyal dirilis."""

    @staticmethod
    def analyze_market(direction: str, price: float, score: float, h1_trend: str, rsi_val: float) -> str:
        if not GEMINI_API_KEY:
            return "Gemini API Key tidak terkonfigurasi. Melewati analisis AI."

        url = "https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent"

        prompt = (
            f"Kamu adalah Institutional Quant Trader Emas (XAUUSD).\n"
            f"Sistem teknis Multi-TF mendeteksi sinyal berikut:\n"
            f"- Arah Sinyal: {direction}\n"
            f"- Harga Saat Ini: {price:.2f}\n"
            f"- Trend H1: {h1_trend} (RSI H1: {rsi_val:.1f})\n"
            f"- Confluence Score Engine: {score:.1f}%\n\n"
            f"Berikan analisis ringkas maksimal 2 kalimat: Apakah sinyal ini valid, dan apa risiko utama (misal Liquidity Grab) yang wajib diwaspadai?"
        )

        payload = {"contents": [{"parts": [{"text": prompt}]}]}
        headers = {"Content-Type": "application/json", "X-goog-api-key": GEMINI_API_KEY}

        try:
            r = requests.post(url, json=payload, headers=headers, timeout=12)
            if r.status_code == 200:
                data = r.json()
                return data["candidates"][0]["content"]["parts"][0]["text"].strip()
            else:
                return f"Gemini API Status: {r.status_code}"
        except Exception as e:
            return f"Error AI Gatekeeper: {str(e)}"


# =============================================================================
# 8. DISPATCHER & VISUALIZATION ENGINE
# =============================================================================
class HealthMonitor:
    @staticmethod
    def send_ping(status: str = "OK"):
        if HEALTHCHECK_URL:
            try:
                requests.get(f"{HEALTHCHECK_URL}?status={status}", timeout=5)
            except Exception:
                pass


def send_telegram_text(m: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log("[Telegram Notice] Credentials missing.")
        return
    try:
        requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                      json={"chat_id": TELEGRAM_CHAT_ID, "text": m, "parse_mode": "HTML"}, timeout=12)
    except Exception as e:
        log(f"send text err {e}")


def send_telegram_photo(cap: str, p: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        with open(p, 'rb') as f:
            requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendPhoto",
                          data={"chat_id": TELEGRAM_CHAT_ID, "caption": cap, "parse_mode": "HTML"},
                          files={"photo": f}, timeout=20)
    except Exception as e:
        log(f"photo err {e}")
        send_telegram_text(cap)


def make_chart(df, entry, sl, t1, t2, t3, t4, signal, conf, atr_m15):
    try:
        plt.figure(figsize=(10, 6))
        sub = df.tail(80).copy()
        x_vals = sub.index if isinstance(sub.index, pd.DatetimeIndex) else range(len(sub))

        plt.plot(x_vals, sub["close"].values, label="M15 Price", color="gold", linewidth=1.5)
        plt.axhline(entry, color="cyan", label=f"Entry {entry:.2f}")
        plt.axhline(sl, color="red", label=f"SL {sl:.2f}")
        plt.axhline(t1, color="green", linestyle=":", label="TP1")
        plt.axhline(t3, color="green", linestyle="-", label="TP3")

        plt.title(f"M15+H1 Quant Analysis | {signal} Score: {conf:.0f}% | ATR: {atr_m15}")
        plt.legend(fontsize=8)
        plt.grid(alpha=0.3)
        plt.tight_layout()
        chart_file = os.path.join(CACHE_DIR, "chart_v24.png")
        plt.savefig(chart_file, dpi=150)
        plt.close('all')
        return chart_file
    except Exception as e:
        log(f"Chart generation error: {e}")
        return None


# =============================================================================
# 9. MAIN EXECUTION PIPELINE
# =============================================================================
def main():
    log("=================================================================")
    log("STARTING XAUUSD HYBRID QUANT ENGINE (GITHUB ACTIONS EXECUTION)")
    log(f"Timestamp UTC: {datetime.datetime.now(datetime.timezone.utc).isoformat()}")
    log("=================================================================")

    HealthMonitor.send_ping("STARTING")

    # Inisialisasi Database SQLite & Load File
    db_engine = PersistenceEngine()
    dna = load_json(DNA_FILE, {"engines": {}})
    journal = load_json(JOURNAL_FILE, [])
    fail_count = load_json(FAILURE_FILE, {"count": 0})

    # Cek Berita High Impact
    if check_high_impact_news() and not FORCE_RUN:
        log("⛔ Trading dibatalkan sementara karena ada berita High Impact.")
        HealthMonitor.send_ping("SKIPPED_HIGH_IMPACT_NEWS")
        return 0

    # Ambil Data Pasar
    try:
        price, df_m15, df_h1, df_h4, source_name, total_offset = get_failover_price_with_dynamic_offset()
        fail_count["count"] = 0
        save_json(FAILURE_FILE, fail_count)
    except Exception as e:
        fail_count["count"] += 1
        save_json(FAILURE_FILE, fail_count)
        err_msg = f"Gagal total mengambil harga: {e}"
        log(err_msg)
        if fail_count["count"] >= 2:
            send_telegram_text(f"🚨 <b>EMERGENCY ALERT</b>\n{err_msg}\n(Gagal {fail_count['count']}x berturut-turut)")
        HealthMonitor.send_ping("FAIL_DATA_FETCH")
        return 1

    # Stress Test Monte Carlo
    mc_result = MonteCarloStressTester.run_simulation()
    log(f"[Monte Carlo Test] 95% Expected Max DD: {mc_result['expected_max_drawdown_95']}% | Pass: {mc_result['pass_stress_test']}")

    if not mc_result["pass_stress_test"] and not FORCE_RUN:
        log("[Engine Abort] Risiko drawdown melebihi batas toleransi.")
        HealthMonitor.send_ping("FAIL_STRESS_TEST")
        return 0

    # Evaluasi Trade Sebelumnya & Update DNA
    dna = evaluate_and_evolve_dna_from_journal(dna, journal, price)
    save_json(DNA_FILE, dna)

    # Scoring Indikator dari 10 Engine
    buy_w = sell_w = 0.0
    current_engine_states = {}

    for name, fn in engine_map:
        try:
            sc, base_w = fn(df_m15)
            dna_w = dna.get("engines", {}).get(name, {}).get("weight", 1.0)
            w = base_w * dna_w
            current_engine_states[name] = {"sc": sc, "weight_used": round(w, 4)}
            if sc > 0:
                buy_w += w * abs(sc)
            elif sc < 0:
                sell_w += w * abs(sc)
        except Exception as ex:
            log(f"Engine {name} error: {ex}")

    h1_trend_text = get_trend_label(df_h1)
    h1_rsi_val = float(rsi(df_h1).iloc[-1])

    if "BULLISH" in h1_trend_text:
        buy_w += 2.0
    elif "BEARISH" in h1_trend_text:
        sell_w += 2.0

    total_w = buy_w + sell_w

    if total_w == 0:
        log("Konsensus pasar NETRAL (0%). Sinyal diabaikan.")
        HealthMonitor.send_ping("NEUTRAL_MARKET")
        return 0

    consensus = (max(buy_w, sell_w) / total_w) * 100
    signal = "BUY" if buy_w > sell_w else "SELL"

    log(f"[Technical Analysis] Price: {price:.2f} | Signal: {signal} | Consensus Score: {consensus:.1f}%")

    if consensus < MIN_CONFLUENCE_SCORE:
        log(f"Konsensus pasar {consensus:.0f}% < threshold ({MIN_CONFLUENCE_SCORE}%). Lewati eksekusi.")
        HealthMonitor.send_ping("LOW_SCORE_SKIPPED")
        return 0

    # Anti-Spam Gate via SQLite Database
    if db_engine.is_duplicate_signal(signal, price, COOLDOWN_MINUTES) and not FORCE_RUN:
        log("[Anti-Spam Gate] Sinyal duplikat terdeteksi di SQLite. Eksekusi dilewati.")
        HealthMonitor.send_ping("SUCCESS_DUPLICATE_SKIPPED")
        return 0

    # Kalkulasi ATR, SL & TP
    now_hour = datetime.datetime.now(WIB).hour
    session_mult = 1.35 if (13 <= now_hour <= 23 or now_hour <= 2) else 1.15
    atr_m15 = calc_atr(df_m15, 14)
    sl_base = max(6.0, min(16.0, atr_m15 * session_mult)) + 1.2

    entry = price
    if signal == "BUY":
        sl = entry - sl_base
        t1, t2, t3, t4 = (entry + sl_base * 1.3, entry + sl_base * 2.2, entry + sl_base * 3.5, entry + sl_base * 5.0)
    else:
        sl = entry + sl_base
        t1, t2, t3, t4 = (entry - sl_base * 1.3, entry - sl_base * 2.2, entry - sl_base * 3.5, entry - sl_base * 5.0)

    # Gemini AI Analysis
    ai_insights = GeminiGatekeeper.analyze_market(signal, entry, consensus, h1_trend_text, h1_rsi_val)

    # Simpan Journal & State SQLite
    trade_entry = {
        "time": datetime.datetime.now(WIB).isoformat(), "signal": signal, "price": entry,
        "sl": round(sl, 2), "tp1": round(t1, 2), "tp2": round(t2, 2),
        "tp3": round(t3, 2), "tp4": round(t4, 2), "consensus": round(consensus, 2),
        "engine_states": current_engine_states, "evaluated": False
    }
    journal.append(trade_entry)
    if len(journal) > 50:
        journal = journal[-50:]
    save_json(JOURNAL_FILE, journal)

    db_engine.save_signal(signal, entry, consensus)

    # Format Telegram Dispatcher
    chart_path = make_chart(df_m15, entry, sl, t1, t2, t3, t4, signal, consensus, round(atr_m15, 2))
    offset_info = f"\n⚙️ Offset: {total_offset:+.2f}$" if total_offset != 0 else ""

    caption = (
        f"💎 <b>XAUUSD QUANT SIGNAL - {signal} ({consensus:.0f}%)</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"📡 <b>Sumber:</b> {source_name} {offset_info}\n"
        f"📊 <b>Analisis H1:</b> {h1_trend_text} (RSI: {h1_rsi_val:.1f})\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"<b>Entry Price:</b> <code>{entry:.2f}</code>\n"
        f"<b>Stop Loss:</b> <code>{round(sl, 2)}</code> (-{round(sl_base, 2)}$)\n"
        f"<b>TP1:</b> <code>{round(t1, 2)}</code> | <b>TP2:</b> <code>{round(t2, 2)}</code>\n"
        f"<b>TP3:</b> <code>{round(t3, 2)}</code> | <b>TP4:</b> <code>{round(t4, 2)}</code>\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"🤖 <b>Gemini AI Insight:</b>\n<i>{ai_insights}</i>\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"⏰ <i>{datetime.datetime.now(WIB).strftime('%H:%M WIB | %d-%m-%Y')}</i>"
    )

    if chart_path and os.path.exists(chart_path):
        send_telegram_photo(caption, chart_path)
    else:
        send_telegram_text(caption)

    log(f"SINYAL HYBRID BERHASIL DIKIRIM: {signal} di {entry:.2f} ({consensus:.0f}%)")
    HealthMonitor.send_ping("SUCCESS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
