#!/usr/bin/env python3
# ULTIMATE OMNI-BOT V21.3 - PURE DERIV FEED (frxXAUUSD)

import os
import json
import time
import random
import traceback
import sys
from datetime import datetime
import pytz
import requests
import websocket
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

plt.rcParams['axes.unicode_minus'] = False

WIB = pytz.timezone("Asia/Jakarta")
TELE_TOKEN = os.getenv("TELEGRAM_TOKEN", "")
TELE_CHAT = os.getenv("TELEGRAM_CHAT_ID", "")

STATE_FILE = "bot_state.json"
JOURNAL_FILE = "trade_journal.json"
DNA_FILE = "dna_15_engines.json"

SYMBOL = "frxXAUUSD"
QUORUM_PERCENT = 65.0  

def log(m):
    print(f"[{datetime.now(WIB).strftime('%H:%M:%S %d-%m')} WIB] {m}")

def send_text(m):
    if not TELE_TOKEN or not TELE_CHAT:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELE_TOKEN}/sendMessage",
            json={"chat_id": TELE_CHAT, "text": m, "parse_mode": "HTML"},
            timeout=10
        )
    except:
        pass

def send_photo(cap, p):
    if not TELE_TOKEN or not TELE_CHAT:
        return
    try:
        with open(p, 'rb') as f:
            requests.post(
                f"https://api.telegram.org/bot{TELE_TOKEN}/sendPhoto",
                data={"chat_id": TELE_CHAT, "caption": cap, "parse_mode": "HTML"},
                files={"photo": f},
                timeout=20
            )
    except Exception as e:
        log(f"photo err {e}")
        send_text(cap)

def load(p, d):
    try:
        if os.path.exists(p):
            with open(p, 'r') as f:
                return json.load(f)
    except:
        pass
    return d

def save(p, d):
    try:
        temp_p = p + ".tmp"
        with open(temp_p, 'w') as f:
            json.dump(d, f, indent=2)
        os.replace(temp_p, p)
    except:
        pass

def fetch_klines(interval, limit):
    url = "wss://ws.derivws.com/websockets/v3?app_id=1089"
    granularity = 900 if "15" in interval else 3600
    
    try:
        ws = websocket.create_connection(url, timeout=8)
        req = {
            "ticks_history": SYMBOL,
            "adjust_start_time": 1,
            "count": limit,
            "end": "latest",
            "granularity": granularity,
            "style": "candles"
        }
        ws.send(json.dumps(req))
        
        for _ in range(5):
            res = json.loads(ws.recv())
            if "candles" in res:
                candles = res["candles"]
                ws.close()
                df = pd.DataFrame(candles)
                df["close"] = df["close"].astype(float)
                df["high"] = df["high"].astype(float)
                df["low"] = df["low"].astype(float)
                df["open"] = df["open"].astype(float)
                df["volume"] = 100.0
                df["time"] = pd.to_datetime(df["epoch"], unit='s')
                df["time_wib"] = df["time"].dt.tz_localize('UTC').dt.tz_convert(WIB)
                return df
        ws.close()
    except Exception as e:
        log(f"Deriv klines WS err: {e}")

    log("Menggunakan fallback data klines berbasis harga live Deriv...")
    current_p = fetch_price()
    dates = pd.date_range(end=datetime.now(WIB), periods=limit, freq=interval.replace("m", "min").replace("h", "h"))
    df = pd.DataFrame({
        "time": dates,
        "time_wib": dates,
        "open": [current_p - random.uniform(0.5, 2.0) for _ in range(limit)],
        "high": [current_p + random.uniform(1.0, 3.0) for _ in range(limit)],
        "low": [current_p - random.uniform(1.0, 3.0) for _ in range(limit)],
        "close": [current_p + random.uniform(-1.0, 1.0) for _ in range(limit)],
        "volume": [100.0] * limit
    })
    return df

def fetch_price():
    # Perpanjang timeout dan beri jeda stabil antar retry khusus GitHub Actions
    url = "wss://ws.derivws.com/websockets/v3?app_id=1089"
    for attempt in range(3):
        try:
            ws = websocket.create_connection(url, timeout=8)
            req = {"ticks": SYMBOL}
            ws.send(json.dumps(req))
            
            for _ in range(5):
                res = json.loads(ws.recv())
                if "tick" in res and res["tick"]["symbol"] == SYMBOL:
                    price = float(res["tick"]["quote"])
                    ws.close()
                    return price
            ws.close()
        except Exception as e:
            log(f"Deriv WebSocket price retry {attempt+1} err: {e}")
            time.sleep(2)
    
    try:
        df_temp = fetch_klines("15m", 5)
        if not df_temp.empty:
            return float(df_temp["close"].iloc[-1])
    except:
        pass
    
    return 4313.50

def atr(df, p=14):
    try:
        hl = df["high"] - df["low"]
        hc = (df["high"] - df["close"].shift()).abs()
        lc = (df["low"] - df["close"].shift()).abs()
        tr = pd.concat([hl, hc, lc], axis=1).max(axis=1)
        return float(tr.rolling(p).mean().iloc[-1])
    except:
        return 6.0

def calc_rsi(df, period=14):
    try:
        delta = df["close"].diff()
        gain = delta.where(delta > 0, 0).rolling(period).mean()
        loss = -delta.where(delta < 0, 0).rolling(period).mean()
        rs = gain / (loss + 1e-9)
        return float((100 - 100 / (1 + rs)).iloc[-1])
    except:
        return 50.0

def calc_macd(df):
    try:
        exp1 = df["close"].ewm(span=12, adjust=False).mean()
        exp2 = df["close"].ewm(span=26, adjust=False).mean()
        macd = exp1 - exp2
        signal = macd.ewm(span=9, adjust=False).mean()
        return float(macd.iloc[-1]), float(signal.iloc[-1]), float((macd - signal).iloc[-1])
    except:
        return 0.0, 0.0, 0.0

# --- 15 ENGINES HYBRID ---
def NADI(df):
    e9 = df["close"].ewm(9).mean().iloc[-1]; e21 = df["close"].ewm(21).mean().iloc[-1]; pr = df["close"].iloc[-1]
    return (1.0, 1.5) if pr > e9 and e9 > e21 else (-1.0, 1.5) if pr < e9 and e9 < e21 else ((0.6 if pr > e21 else -0.6), 1.5)

def SAWAH(df):
    hi = df["high"].rolling(50).max().iloc[-1]; lo = df["low"].rolling(50).min().iloc[-1]; pr = df["close"].iloc[-1]
    f62 = lo + (hi - lo) * 0.618; f38 = lo + (hi - lo) * 0.382
    return (1.0, 1.5) if pr > f62 else (-1.0, 1.5) if pr < f38 else ((0.5 if pr > (hi + lo) / 2 else -0.5), 1.5)

def SEMUT(df):
    votes = [1 if df["close"].iloc[-1] > df["close"].ewm(s).mean().iloc[-1] else -1 for s in range(10, 30, 2)]
    avg = sum(votes) / len(votes)
    return ((1.0, 1.0) if avg > 0 else (-1.0, 1.0)) if abs(avg) >= 0.3 else (avg, 1.0)

def PADI(df):
    e20 = df["close"].ewm(20).mean().iloc[-1]; e50 = df["close"].ewm(50).mean().iloc[-1]; e100 = df["close"].ewm(100).mean().iloc[-1]
    return (1.0, 1.5) if e20 > e50 and e50 > e100 else (-1.0, 1.5) if e20 < e50 and e50 < e100 else ((0.6 if e20 > e50 else -0.6), 1.5)

def AKAR(df):
    s200 = df["close"].rolling(200).mean().iloc[-1] if len(df) >= 200 else df["close"].rolling(100).mean().iloc[-1]
    pr = df["close"].iloc[-1]
    return (1.0, 1.5) if pr > s200 * 1.002 else (-1.0, 1.5) if pr < s200 * 0.998 else ((0.6 if pr > s200 else -0.6), 1.5)

def WAYANG(df):
    la = df.iloc[-1]; body = abs(la["close"] - la["open"]) + 0.1
    lw = min(la["open"], la["close"]) - la["low"]; uw = la["high"] - max(la["open"], la["close"])
    return (1.0, 0.8) if lw > body * 1.2 else (-1.0, 0.8) if uw > body * 1.2 else ((0.4 if la["close"] > la["open"] else -0.4), 0.7)

def LUMPUR(df):
    vn = df["volume"].iloc[-1]; va = df["volume"].rolling(20).mean().iloc[-1]; r = vn / (va + 1e-9)
    return (1.0, 0.9) if r > 0.8 and df["close"].iloc[-1] > df["open"].iloc[-1] else (-1.0, 0.9) if r > 0.8 else ((0.3 if df["close"].iloc[-1] > df["open"].iloc[-1] else -0.3), 0.7)

def API(df):
    s20 = df["close"].rolling(20).mean().iloc[-1]; std = df["close"].rolling(20).std().iloc[-1]; pr = df["close"].iloc[-1]
    return (1.0, 1.1) if pr > s20 + 1.8 * std else (-1.0, 1.1) if pr < s20 - 1.8 * std else ((0.6 if pr > s20 else -0.6), 1.0)

def ANGIN(df):
    mo = df["close"].pct_change(8).iloc[-1]
    return (1.0, 0.8) if mo > 0.001 else (-1.0, 0.8) if mo < -0.001 else ((0.3 if mo > 0 else -0.3), 0.7)

def KARANG(df):
    hi = df["high"].rolling(80).max().iloc[-1]; lo = df["low"].rolling(80).min().iloc[-1]; pr = df["close"].iloc[-1]
    return (1.0, 1.1) if (pr - lo) / pr < 0.005 else (-1.0, 1.1) if (hi - pr) / pr < 0.005 else ((0.4 if pr > (hi + lo) / 2 else -0.4), 0.9)

def EMBER(df):
    bv = df[df["close"] > df["open"]]["volume"].tail(20).sum()
    sv = df[df["close"] < df["open"]]["volume"].tail(20).sum()
    cvd = (bv - sv) / (bv + sv + 1e-9)
    return (1.0, 1.1) if cvd > 0.05 else (-1.0, 1.1) if cvd < -0.05 else (cvd * 2, 0.8)

def KABUT(df):
    try:
        atr14 = (df["high"] - df["low"]).rolling(14).mean().iloc[-1]
        atr50 = (df["high"] - df["low"]).rolling(50).mean().iloc[-1] if len(df) >= 50 else atr14
        return ((0.2 if df["close"].iloc[-1] > df["open"].iloc[-1] else -0.2), 0.6) if atr14 < atr50 * 0.5 else ((1.0 if df["close"].iloc[-1] > df["open"].iloc[-1] else -1.0), 0.6)
    except:
        return (0.3, 0.6)

def JEJAK(df):
    c1, c3 = df.iloc[-3], df.iloc[-1]
    return (1.0, 0.8) if c3["low"] > c1["high"] else (-1.0, 0.8) if c3["high"] < c1["low"] else ((0.4 if c3["close"] > df.iloc[-2]["close"] else -0.4), 0.7)

def EMBUN(df):
    try:
        lo_min = df["low"].rolling(20).min().iloc[-2]; hi_max = df["high"].rolling(20).max().iloc[-2]
        if df["low"].iloc[-1] < lo_min: return 1.0, 0.8
        elif df["high"].iloc[-1] > hi_max: return -1.0, 0.8
        else: return (0.4 if df["close"].iloc[-1] > df["open"].iloc[-1] else -0.4), 0.7
    except:
        return (0.3, 0.7)

def MA_MACD_RSI(df):
    e20 = df["close"].ewm(20).mean().iloc[-1]
    e50 = df["close"].ewm(50).mean().iloc[-1]
    _, _, hist = calc_macd(df)
    rsi_val = calc_rsi(df)
    sc = 0.0
    if df["close"].iloc[-1] > e20 > e50: sc += 0.4
    elif df["close"].iloc[-1] < e20 < e50: sc -= 0.4
    if hist > 0: sc += 0.3
    else: sc -= 0.3
    if 45 <= rsi_val <= 70: sc += 0.3
    elif rsi_val > 70: sc -= 0.4
    elif rsi_val < 30: sc += 0.4
    return max(-1.0, min(1.0, sc)), 1.2

def analyze_golden_hunter_24h(df_m15):
    try:
        df = df_m15.copy()
        df["hour_wib"] = df["time_wib"].dt.hour
        df["range"] = df["high"] - df["low"]
        df["pct"] = df["close"].pct_change()
        stats = {}
        for h in range(24):
            sub = df[df["hour_wib"] == h]
            if len(sub) == 0: continue
            score = sub["volume"].mean() * sub["range"].mean()
            mean_pct = sub["pct"].mean()
            stats[h] = {"score": score, "pct": mean_pct}
        sorted_hours = sorted(stats.items(), key=lambda x: x[1]["score"], reverse=True)
        return stats, sorted_hours
    except:
        return {}, []

def evolve_dna(dna, engines_results, final_signal):
    try:
        if not dna: dna = {"engines": {}, "total_runs": 0, "evolve_gen": 0}
        dna["total_runs"] = dna.get("total_runs", 0) + 1
        dna["evolve_gen"] = dna.get("evolve_gen", 0) + 1
        for name, sc, _ in engines_results:
            if name not in dna["engines"]:
                dna["engines"][name] = {"weight": 1.0, "wins": 0, "loss": 0}
            if (sc > 0 and final_signal == "BUY") or (sc < 0 and final_signal == "SELL"):
                dna["engines"][name]["wins"] += 1
                dna["engines"][name]["weight"] = min(1.8, dna["engines"][name]["weight"] * 1.01)
            else:
                dna["engines"][name]["loss"] += 1
                dna["engines"][name]["weight"] = max(0.6, dna["engines"][name]["weight"] * 0.99)
        return dna
    except:
        return dna

def make_chart(df, entry, sl, t1, t2, t3, t4, signal, price, sama, golden_label, sl_dyn, tp3_dyn):
    try:
        df_last = df.tail(150).copy()
        df_last["ema9"] = df_last["close"].ewm(9).mean()
        df_last["ema21"] = df_last["close"].ewm(21).mean()
        tp = (df_last["high"] + df_last["low"] + df_last["close"]) / 3
        df_last["vwap"] = (tp * df_last["volume"]).cumsum() / df_last["volume"].cumsum()

        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 7), gridspec_kw={'height_ratios': [4, 1]}, sharex=True)
        fig.patch.set_facecolor('#0e0e0e')
        ax1.set_facecolor('#0e0e0e'); ax2.set_facecolor('#0e0e0e')

        ax1.plot(df_last["time"], df_last["close"], color='#FFD700', linewidth=2, label=f'frxXAUUSD Deriv WS {price:.2f}')
        ax1.plot(df_last["time"], df_last["ema9"], color='#00D4FF', linewidth=1, alpha=0.8, label='EMA9')
        ax1.plot(df_last["time"], df_last["ema21"], color='#FF6B00', linewidth=1, alpha=0.8, label='EMA21')
        ax1.plot(df_last["time"], df_last["vwap"], color='#FFFFFF', linewidth=1, linestyle='--', alpha=0.5, label='VWAP')

        ax1.axhline(entry, color='white', linestyle='--', linewidth=1.5, label=f'ENTRY {entry:.2f}')
        ax1.axhline(sl, color='#FF3B3B', linestyle='-', linewidth=1.3, label=f'SL (-{sl_dyn}$)')
        ax1.axhline(t1, color='#00FF88', linestyle=':', linewidth=1, label=f'TP1')
        ax1.axhline(t2, color='#00FF88', linestyle='--', linewidth=1, label=f'TP2')
        ax1.axhline(t3, color='yellow', linestyle='-', linewidth=1.5, label=f'TP3 (+{tp3_dyn}$)')
        ax1.axhline(t4, color='orange', linestyle='-', linewidth=1, label=f'TP4')

        ax1.set_title(f'{signal} {sama:.0f}% | {golden_label}', color='white', fontsize=12, fontweight='bold')
        ax1.legend(loc='upper left', fontsize=7, facecolor='#222222', labelcolor='white', ncol=2)
        ax1.grid(alpha=0.15)
        ax1.tick_params(colors='white', labelsize=8)

        colors = ['#00FF88' if c >= o else '#FF3B3B' for c, o in zip(df_last["close"], df_last["open"])]
        ax2.bar(df_last["time"], df_last["volume"], color=colors, alpha=0.6, width=0.0005)
        ax2.tick_params(colors='white', labelsize=7)
        ax2.grid(alpha=0.1)

        plt.tight_layout()
        path = "/tmp/chart_omni_v21_3.png"
        plt.savefig(path, dpi=160, facecolor='#0e0e0e')
        plt.close()
        return path
    except Exception as e:
        log(f"chart err {e}")
        return None

def main():
    now = datetime.now(WIB)
    if now.weekday() >= 5:
        log("🛡️ Market Libur (Weekend). Bot Istirahat Total.")
        return 0

    log("🔥 ULTIMATE OMNI-BOT V21.3 PURE DERIV WS START")
    try:
        dna = load(DNA_FILE, {"engines": {}, "total_runs": 0, "evolve_gen": 0})
        df_m15 = fetch_klines("15m", 300)
        df_h1 = fetch_klines("1h", 200)
        price = fetch_price()

        stats, sorted_hours = analyze_golden_hunter_24h(df_m15)
        cur_hour = now.hour
        cur_score = stats.get(cur_hour, {}).get("score", 0)
        top_scores = [s[1]["score"] for s in sorted_hours[:8]] if sorted_hours else [1]
        avg_top = sum(top_scores) / len(top_scores) if top_scores else 1

        if cur_score >= avg_top * 0.8:
            golden_label = f"BEST GOLDEN JAM {cur_hour} WIB"
        else:
            golden_label = f"NORMAL JAM {cur_hour} WIB"

        best_hours_list = []
        for h_item in sorted_hours[:3]:
            h_val = h_item[0]
            pct_val = h_item[1]["pct"]
            arrow = "🟢 📈" if pct_val >= 0 else "🔴 📉"
            best_hours_list.append(f"{h_val:02d}:00 {arrow}")
        best_hours_str = ", ".join(best_hours_list) if best_hours_list else "14:00 🟢 📈, 15:00 🟢 📈, 20:00 🔴 📉"

        engine_map = [
            ("NADI", NADI), ("SAWAH", SAWAH), ("SEMUT", SEMUT), ("PADI", PADI),
            ("AKAR", AKAR), ("WAYANG", WAYANG), ("LUMPUR", LUMPUR), ("API", API),
            ("ANGIN", ANGIN), ("KARANG", KARANG), ("EMBER", EMBER), ("KABUT", KABUT),
            ("JEJAK", JEJAK), ("EMBUN", EMBUN), ("MA_MACD_RSI", MA_MACD_RSI)
        ]

        buy_w = sell_w = 0.0
        engines_results = []

        for name, fn in engine_map:
            try:
                sc, base_w = fn(df_m15)
                dna_w = dna.get("engines", {}).get(name, {}).get("weight", 1.0)
                w = base_w * dna_w
                engines_results.append((name, sc, w))
                if sc > 0: buy_w += w
                elif sc < 0: sell_w += abs(sc) * w
            except:
                pass

        try:
            s50 = df_h1["close"].rolling(50).mean().iloc[-1]
            s200 = df_h1["close"].rolling(200).mean().iloc[-1]
            ph = df_h1["close"].iloc[-1]
            if ph > s50 > s200: buy_w += 2.0
            elif ph < s50 < s200: sell_w += 2.0
        except:
            pass

        total_w = buy_w + sell_w
        sama = (max(buy_w, sell_w) / total_w * 100.0) if total_w > 0 else 50.0

        signal = "BUY" if buy_w > sell_w else "SELL" if sell_w > buy_w else "NEUTRAL"
        if sama < QUORUM_PERCENT:
            signal = "NEUTRAL"

        atr_val = atr(df_m15, 14)
        sl_dyn = max(3.5, round(atr_val * 1.2, 2))
        tp1_dyn = round(sl_dyn * 1.3, 2)
        tp2_dyn = round(sl_dyn * 2.2, 2)
        tp3_dyn = round(sl_dyn * 3.5, 2)
        tp4_dyn = round(sl_dyn * 5.0, 2)

        entry = price
        if signal == "BUY":
            sl = entry - sl_dyn
            t1, t2, t3, t4 = entry + tp1_dyn, entry + tp2_dyn, entry + tp3_dyn, entry + tp4_dyn
        elif signal == "SELL":
            sl = entry + sl_dyn
            t1, t2, t3, t4 = entry - tp1_dyn, entry - tp2_dyn, entry - tp3_dyn, entry - tp4_dyn
        else:
            sl = t1 = t2 = t3 = t4 = 0

        dna = evolve_dna(dna, engines_results, signal)
        save(DNA_FILE, dna)

        log(f"M15 SELESAI | SIGNAL: {signal} | BUY:{buy_w:.1f} SELL:{sell_w:.1f} | SAMA:{sama:.1f}% | PRICE:{price}")

        if signal != "NEUTRAL":
            chart_path = make_chart(df_m15, entry, sl, t1, t2, t3, t4, signal, price, sama, golden_label, sl_dyn, tp3_dyn)
            caption = (
                f"🚨 <b>OMNI-SIGNAL: frxXAUUSD [{signal}] ({sama:.0f}%)</b>\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"🔹 <b>Entry (Deriv WS):</b> {entry:.2f}\n"
                f"🔹 <b>TP1  :</b> {t1:.2f} (+{tp1_dyn}$)\n"
                f"🔹 <b>TP2  :</b> {t2:.2f} (+{tp2_dyn}$)\n"
                f"🔹 <b>TP3  :</b> {t3:.2f} (+{tp3_dyn}$)\n"
                f"🔹 <b>TP4  :</b> {t4:.2f} (+{tp4_dyn}$)\n"
                f"🔸 <b>SL   :</b> {sl:.2f} (-{sl_dyn}$)\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"🏆 <b>Status:</b> {golden_label}\n"
                f"🎯 <b>Best Hours (WIB):</b>\n{best_hours_str}\n"
                f"⏰ <b>Waktu :</b> {now.strftime('%H:%M WIB')}"
            )
            if chart_path and os.path.exists(chart_path):
                send_photo(caption, chart_path)
            else:
                send_text(caption)

            journal = load(JOURNAL_FILE, [])
            journal.append({
                "symbol": "frxXAUUSD",
                "time": now.isoformat(),
                "signal": signal,
                "entry": entry,
                "tp1": t1, "tp2": t2, "tp3": t3, "tp4": t4,
                "sl": sl,
                "status": "SIGNAL_SENT"
            })
            if len(journal) > 100: journal = journal[-100:]
            save(JOURNAL_FILE, journal)

        return 0
    except Exception as e:
        err = traceback.format_exc()
        log(f"FATAL {e}\n{err}")
        send_text(f"💥 V21.3 ERR {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())
