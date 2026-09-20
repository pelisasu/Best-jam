#!/usr/bin/env python3
# V24.6 ULTIMATE OMNI-BOT (News Filter + True DNA Evolution + Health Check)
import os, json, time, sys
from datetime import datetime, timedelta
import pytz, requests, pandas as pd, yfinance as yf
import websocket
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
plt.rcParams['axes.unicode_minus'] = False

WIB = pytz.timezone("Asia/Jakarta")
UTC = pytz.UTC
TELE_TOKEN = os.getenv("TELEGRAM_TOKEN", "") or os.getenv("TELEGRAM_BOT_TOKEN", "")
TELE_CHAT = os.getenv("TELEGRAM_CHAT_ID", "")
SYMBOL = os.getenv("DERIV_SYMBOL", "frxXAUUSD")

DNA_FILE = "dna_10_engines.json"
JOURNAL_FILE = "trade_journal.json"
FAILURE_FILE = "failure_count.json"

def log(m): 
    print(f"[{datetime.now(WIB).strftime('%H:%M:%S %d-%m')} WIB] {m}")

def send_text(m):
    if not TELE_TOKEN or not TELE_CHAT: return
    try:
        requests.post(f"https://api.telegram.org/bot{TELE_TOKEN}/sendMessage", 
                      json={"chat_id": TELE_CHAT, "text": m, "parse_mode": "HTML"}, timeout=12)
    except Exception as e: 
        log(f"send text err {e}")

def send_photo(cap, p):
    if not TELE_TOKEN or not TELE_CHAT: return
    try:
        with open(p, 'rb') as f:
            requests.post(f"https://api.telegram.org/bot{TELE_TOKEN}/sendPhoto", 
                          data={"chat_id": TELE_CHAT, "caption": cap, "parse_mode": "HTML"}, 
                          files={"photo": f}, timeout=20)
    except Exception as e:
        log(f"photo err {e}")
        send_text(cap)

# [FIX 3] Health Check: Emergency Alert
def send_emergency_alert(msg):
    alert_msg = f"🚨 <b>EMERGENCY ALERT</b>\nBot V24.6 mengalami kegagalan kritis:\n\n<code>{msg}</code>\n\nSegera periksa log GitHub Actions atau server Anda."
    send_text(alert_msg)

def load(p, d):
    try:
        if os.path.exists(p):
            with open(p, 'r') as f: return json.load(f)
    except: pass
    return d

def save(p, d):
    try:
        with open(p, 'w') as f: json.dump(d, f, indent=2)
    except: pass

def fetch_deriv(limit=300, gran=900):
    url = "wss://ws.derivws.com/websockets/v3?app_id=1089"
    for attempt in range(3):
        ws = None
        try:
            ws = websocket.create_connection(url, timeout=8)
            ws.send(json.dumps({"ticks_history": SYMBOL, "count": limit, "end": "latest", "granularity": gran, "style": "candles"}))
            
            start_time = time.time()
            candles_received = False
            df, price = None, None
            
            while time.time() - start_time < 8:
                res = json.loads(ws.recv())
                if "candles" in res and res["candles"]:
                    df = pd.DataFrame(res["candles"])
                    for c in ["close", "high", "low", "open"]:
                        df[c] = df[c].astype(float)
                    df["volume"] = 100.0
                    price = float(df["close"].iloc[-1])
                    candles_received = True
                    break
            
            if ws:
                try: ws.close()
                except: pass
                
            if candles_received:
                return df, price
            else:
                log(f"Deriv attempt {attempt+1}: Timeout atau tidak ada data candles.")
                
        except Exception as e:
            log(f"Deriv attempt {attempt+1} err: {e}")
            if ws:
                try: ws.close()
                except: pass
            time.sleep(2)
    return None, None

def fetch_binance_paxg():
    try:
        r = requests.get("https://api.binance.com/api/v3/ticker/price?symbol=PAXGUSDT", timeout=6)
        if r.status_code == 200: return float(r.json()["price"])
    except: pass
    return None

def fetch_yf_gc():
    try:
        df = yf.Ticker("GC=F").history(period="5d", interval="15m")
        if df is None or len(df) < 50: return None, None
        
        price = float(df["Close"].iloc[-1])
        df = df.reset_index()
        date_col = next((c for c in df.columns if 'date' in c.lower()), None)
        if not date_col: date_col = df.columns[0]
        
        df = df.rename(columns={date_col: "datetime", "Close": "close", "High": "high", "Low": "low", "Open": "open"})
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
        try: source_specific_offset = float(os.getenv("DERIV_OFFSET", "0"))
        except: pass

    if raw_price is None:
        log("Deriv WebSocket gagal, beralih ke Binance PAXG...")
        p_binance = fetch_binance_paxg()
        if p_binance:
            raw_price = p_binance
            prices_source = "Binance PAXG"
            df_m15, _ = fetch_yf_gc() 
            try: source_specific_offset = float(os.getenv("BINANCE_OFFSET", "0"))
            except: pass

    if raw_price is None:
        log("Binance gagal, beralih ke Yahoo Finance (GC=F)...")
        df_yf, p_yf = fetch_yf_gc()
        if p_yf:
            raw_price = p_yf
            prices_source = "Yahoo GC=F"
            df_m15 = df_yf
            try: source_specific_offset = float(os.getenv("YAHOO_OFFSET", "-46.50"))
            except: source_specific_offset = -46.50

    if raw_price is None or df_m15 is None:
        raise RuntimeError("Kritis: Seluruh sumber harga gagal diakses!")

    global_mt5_offset = 0.0
    try: global_mt5_offset = float(os.getenv("MT5_OFFSET", "0"))
    except: pass

    total_offset = source_specific_offset + global_mt5_offset
    final_price = raw_price + total_offset
    
    log(f"Sumber: {prices_source} | Raw: {raw_price:.2f} | Offset: {total_offset:+.2f} | Final: {final_price:.2f}")

    df_h1, _ = fetch_deriv(200, 3600)
    if df_h1 is None: df_h1 = df_m15
    df_h4, _ = fetch_deriv(200, 14400)
    if df_h4 is None: df_h4 = df_h1

    return final_price, df_m15, df_h1, df_h4, prices_source, total_offset

# [FIX 1] Filter Berita High Impact (Menggunakan ForexFactory Public JSON)
def check_high_impact_news():
    try:
        url = "https://nfs.faireconomy.media/ff_calendar_thisweek.json"
        response = requests.get(url, timeout=5)
        if response.status_code == 200:
            events = response.json()
            now = datetime.now(UTC)
            for event in events:
                # Filter untuk negara USD atau XAU dengan impact High/Red
                country = event.get('country', '')
                impact = event.get('impact', '')
                if country in ['USD', 'XAU'] and impact in ['High', 'Red']:
                    try:
                        # Format tanggal FF: "2023-10-27T12:30:00.000Z"
                        event_time = datetime.fromisoformat(event['date'].replace('Z', '+00:00'))
                        time_diff = event_time - now
                        # Cek jika berita terjadi dalam 90 menit ke depan atau 30 menit yang lalu
                        if timedelta(minutes=-30) <= time_diff <= timedelta(minutes=90):
                            log(f"⚠️ BERITA HIGH IMPACT TERDETEKSI: {event.get('title', 'Unknown')} pada {event_time.astimezone(WIB).strftime('%H:%M WIB')}")
                            return True
                    except:
                        continue
        return False
    except Exception as e:
        log(f"Gagal cek berita (filter diabaikan): {e}")
        return False # Fail-open: jika API berita gagal, jangan blokir trading

def calc_atr(df, period=14):
    try:
        hl = df["high"] - df["low"]
        hc = (df["high"] - df["close"].shift()).abs()
        lc = (df["low"] - df["close"].shift()).abs()
        tr = pd.concat([hl, hc, lc], axis=1).max(axis=1)
        return float(tr.rolling(period).mean().iloc[-1])
    except: return 8.0

def rsi(df, p=14):
    try:
        delta = df["close"].diff()
        gain = delta.where(delta > 0, 0).rolling(p).mean()
        loss = -delta.where(delta < 0, 0).rolling(p).mean()
        rs = gain / (loss + 1e-9)
        return 100 - 100 / (1 + rs)
    except: return pd.Series([50] * len(df))

# 10 CORE ENGINES
def NADI(df):
    try:
        e9 = df["close"].ewm(9).mean().iloc[-1]; e21 = df["close"].ewm(21).mean().iloc[-1]; pr = df["close"].iloc[-1]
        return (1, 1.5) if pr > e9 and e9 > e21 else (-1, 1.5) if pr < e9 and e9 < e21 else (0, 1.5)
    except: return (0, 1.5)

def SAWAH(df):
    try:
        hi = df["high"].rolling(50).max().iloc[-1]; lo = df["low"].rolling(50).min().iloc[-1]; pr = df["close"].iloc[-1]
        f62 = lo + (hi - lo) * 0.618; f38 = lo + (hi - lo) * 0.382
        return (1, 1.5) if pr > f62 else (-1, 1.5) if pr < f38 else (0, 1.5)
    except: return (0, 1.5)

def SEMUT(df):
    try:
        e50 = df["close"].ewm(50).mean().iloc[-1]; pr = df["close"].iloc[-1]
        return (1, 1.2) if pr > e50 else (-1, 1.2)
    except: return (0, 1.2)

def PADI(df):
    try:
        r = rsi(df).iloc[-1]
        return (1, 1.3) if r > 55 else (-1, 1.3) if r < 45 else (0, 1.3)
    except: return (0, 1.3)

def AKAR(df):
    try:
        s200 = df["close"].rolling(200).mean().iloc[-1] if len(df) >= 200 else df["close"].mean()
        pr = df["close"].iloc[-1]
        return (1, 1.8) if pr > s200 else (-1, 1.8)
    except: return (0, 1.8)

def WAYANG(df):
    try:
        hi = df["high"].rolling(20).max().iloc[-1]; lo = df["low"].rolling(20).min().iloc[-1]; pr = df["close"].iloc[-1]
        return (1, 1.0) if pr > (hi + lo) / 2 else (-1, 1.0)
    except: return (0, 1.0)

def LUMPUR(df):
    try:
        vol = df["volume"].iloc[-1]; vma = df["volume"].rolling(20).mean().iloc[-1]; pr = df["close"].iloc[-1]; prev = df["close"].iloc[-2]
        return (1, 1.1) if vol > vma and pr > prev else (-1, 1.1) if vol > vma and pr < prev else (0, 1.1)
    except: return (0, 1.1)

def API(df):
    try:
        body = (df["close"] - df["open"]).abs().iloc[-1]; avg_b = (df["close"] - df["open"]).abs().rolling(20).mean().iloc[-1]
        pr = df["close"].iloc[-1]; prev = df["close"].iloc[-2]
        return (1, 1.0) if body > avg_b and pr > prev else (-1, 1.0) if body > avg_b and pr < prev else (0, 1.0)
    except: return (0, 1.0)

def ANGIN(df):
    try:
        hi = df["high"].iloc[-1]; lo = df["low"].iloc[-1]; cl = df["close"].iloc[-1]
        return (-1, 1.0) if (hi - cl) > (cl - lo) * 1.5 else (1, 1.0) if (cl - lo) > (hi - cl) * 1.5 else (0, 1.0)
    except: return (0, 1.0)

def EMBER(df):
    try:
        pr = df["close"].iloc[-1]; ma10 = df["close"].rolling(10).mean().iloc[-1]
        return (1, 1.0) if pr > ma10 else (-1, 1.0)
    except: return (0, 1.0)

# [FIX 2] True DNA Evolution: Menyesuaikan bobot berdasarkan hasil trade masa lalu
def evaluate_and_evolve_dna_from_journal(dna, journal, current_price):
    if "engines" not in dna: dna["engines"] = {name: {"weight": 1.0} for name, _ in engine_map}
    
    updated = False
    for trade in journal:
        if trade.get("evaluated"): 
            continue # Sudah dievaluasi sebelumnya
            
        # Cek apakah trade sudah cukup lama (minimal 2 jam) untuk dianggap "selesai" atau kena SL/TP
        trade_time = datetime.fromisoformat(trade["time"])
        if (datetime.now(WIB) - trade_time).total_seconds() < 7200:
            continue

        signal = trade["signal"]
        sl = trade["sl"]
        tp1 = trade["tp1"]
        engine_states = trade.get("engine_states", {})
        
        result = None
        if signal == "BUY":
            if current_price >= tp1: result = "WIN"
            elif current_price <= sl: result = "LOSS"
        else: # SELL
            if current_price <= tp1: result = "WIN"
            elif current_price >= sl: result = "LOSS"

        if result:
            log(f"📊 Evaluasi Trade Masa Lalu: {signal} @ {trade['price']} -> {result}")
            for name, state in engine_states.items():
                if name in dna["engines"]:
                    curr_w = dna["engines"][name]["weight"]
                    # Jika engine searah dengan sinyal yang WIN, naikkan bobot. Jika LOSS, turunkan.
                    was_bullish = state.get("sc", 0) > 0
                    should_have_been_bullish = (signal == "BUY" and result == "WIN") or (signal == "SELL" and result == "LOSS")
                    
                    if was_bullish == should_have_been_bullish:
                        # Engine benar, naikkan bobot (max 2.0)
                        new_w = min(2.0, curr_w + 0.05)
                    else:
                        # Engine salah, turunkan bobot (min 0.5)
                        new_w = max(0.5, curr_w - 0.05)
                    
                    # Mean reversion ringan
                    new_w = new_w * 0.99 + 1.0 * 0.01
                    dna["engines"][name]["weight"] = round(new_w, 4)
            
            trade["evaluated"] = True
            trade["result"] = result
            updated = True

    if updated:
        save(JOURNAL_FILE, journal)
    return dna

def make_chart(df, entry, sl, t1, t2, t3, t4, signal, conf, atr_m15):
    try:
        plt.figure(figsize=(10, 6))
        sub = df.tail(80).copy()
        
        if isinstance(sub.index, pd.DatetimeIndex):
            x_vals = sub.index
            plt.xticks(rotation=45, fontsize=8)
        else:
            x_vals = range(len(sub))
            
        plt.plot(x_vals, sub["close"].values, label="M15 Price", color="gold", linewidth=1.5)
        plt.axhline(entry, color="cyan", label=f"Entry {entry:.2f}")
        plt.axhline(sl, color="red", label=f"SL {sl:.2f}")
        plt.axhline(t1, color="green", linestyle=":", label="TP1")
        plt.axhline(t3, color="green", linestyle="-", label="TP3")
        
        plt.title(f"M15+H1 Analysis | {signal} Score: {conf:.0f}% | ATR: {atr_m15}")
        plt.legend(fontsize=8)
        plt.grid(alpha=0.3)
        plt.tight_layout()
        
        path = "/tmp/chart_v246.png"
        plt.savefig(path, dpi=150)
        plt.close('all')
        return path
    except Exception as e:
        log(f"Chart generation error: {e}")
        return None

# Deklarasi global agar bisa diakses oleh fungsi evaluasi
engine_map = [
    ("NADI", NADI), ("SAWAH", SAWAH), ("SEMUT", SEMUT), ("PADI", PADI), ("AKAR", AKAR),
    ("WAYANG", WAYANG), ("LUMPUR", LUMPUR), ("API", API), ("ANGIN", ANGIN), ("EMBER", EMBER)
]

def main():
    log("V24.6 MULTI-TF ANALYSIS (M15+H1) START")
    dna = load(DNA_FILE, {"engines": {}})
    journal = load(JOURNAL_FILE, [])

    # [FIX 1] Cek Filter Berita
    if check_high_impact_news():
        log("⛔ Trading dibatalkan sementara karena ada berita High Impact. Aman.")
        return 0

    # [FIX 3] Health Check: Reset atau Hitung Kegagalan
    fail_count = load(FAILURE_FILE, {"count": 0})
    
    try:
        price, df_m15, df_h1, df_h4, source_name, total_offset = get_failover_price_with_dynamic_offset()
        # Sukses: reset counter
        fail_count["count"] = 0
        save(FAILURE_FILE, fail_count)
    except Exception as e:
        fail_count["count"] += 1
        save(FAILURE_FILE, fail_count)
        err_msg = f"Gagal total mengambil harga: {e}"
        log(err_msg)
        if fail_count["count"] >= 2:
            send_emergency_alert(f"{err_msg}\n(Gagal {fail_count['count']}x berturut-turut)")
        return 1

    # [FIX 2] Evaluasi trade masa lalu sebelum membuat sinyal baru
    dna = evaluate_and_evolve_dna_from_journal(dna, journal, price)
    save(DNA_FILE, dna)
    
    buy_w = sell_w = 0.0
    engines_results = []
    current_engine_states = {} # Simpan state untuk evaluasi di masa depan
    
    for name, fn in engine_map:
        try:
            sc, base_w = fn(df_m15)
            dna_w = dna.get("engines", {}).get(name, {}).get("weight", 1.0)
            w = base_w * dna_w
            engines_results.append((name, sc, w))
            current_engine_states[name] = {"sc": sc, "weight_used": round(w, 4)}
            
            if sc > 0: buy_w += w * abs(sc)
            elif sc < 0: sell_w += w * abs(sc)
        except Exception as ex:
            log(f"Engine {name} error: {ex}")

    def get_trend_label(df):
        try:
            s50 = df["close"].rolling(50).mean().iloc[-1]
            pr = df["close"].iloc[-1]
            if pr > s50: return "BULLISH (UP)"
            elif pr < s50: return "BEARISH (DOWN)"
            return "SIDEWAYS"
        except: return "NEUTRAL"

    h1_trend_text = get_trend_label(df_h1)
    h1_rsi_val = float(rsi(df_h1).iloc[-1])

    if "BULLISH" in h1_trend_text: buy_w += 2.0
    elif "BEARISH" in h1_trend_text: sell_w += 2.0

    total_w = buy_w + sell_w
    
    if total_w == 0:
        log("Konsensus pasar NETRAL (0%). Tidak ada sinyal BUY atau SELL yang kuat. Lewati eksekusi.")
        return 0

    consensus = (max(buy_w, sell_w) / total_w * 100) if total_w > 0 else 50.0
    signal = "BUY" if buy_w > sell_w else "SELL"

    now_hour = datetime.now(WIB).hour
    session_mult = 1.35 if (13 <= now_hour <= 23 or 0 <= now_hour <= 2) else 1.15

    atr_m15 = calc_atr(df_m15, 14)
    sl_base = max(6.0, min(16.0, atr_m15 * session_mult)) + 1.2
    
    entry = price
    if signal == "BUY":
        sl = entry - sl_base
        t1, t2, t3, t4 = entry + (sl_base*1.3), entry + (sl_base*2.2), entry + (sl_base*3.5), entry + (sl_base*5.0)
    else:
        sl = entry + sl_base
        t1, t2, t3, t4 = entry - (sl_base*1.3), entry - (sl_base*2.2), entry - (sl_base*3.5), entry - (sl_base*5.0)

    if consensus < 60:
        log(f"Konsensus pasar {consensus:.0f}% < 60%. Lewati eksekusi sinyal.")
        return 0

    trade_entry = {
        "time": datetime.now(WIB).isoformat(), "signal": signal, "price": entry,
        "sl": round(sl, 2), "tp1": round(t1, 2), "tp2": round(t2, 2), "tp3": round(t3, 2), "tp4": round(t4, 2), 
        "consensus": round(consensus, 2),
        "engine_states": current_engine_states, # Simpan untuk evaluasi nanti
        "evaluated": False
    }
    
    journal.append(trade_entry)
    if len(journal) > 50: journal = journal[-50:]
    save(JOURNAL_FILE, journal)

    chart_path = make_chart(df_m15, entry, sl, t1, t2, t3, t4, signal, consensus, round(atr_m15, 2))
    offset_info = f"\n⚙️ <b>Offset:</b> {total_offset:+.2f}$" if total_offset != 0 else ""

    caption = (
        f"💎 <b>V24.6 MULTI-TF (M15 + H1) - {signal} ({consensus:.0f}%)</b>\n"
        f"━━━━━━━━━━━━\n"
        f"📡 <b>Sumber:</b> {source_name} {offset_info}\n"
        f"📊 <b>Analisis TF H1:</b> {h1_trend_text} (RSI: {h1_rsi_val:.1f})\n"
        f"━━━━━━━━━━━━\n"
        f"<b>Entry Price:</b> {entry:.2f}\n"
        f"<b>SL:</b> {round(sl,2)} (-{round(sl_base,2)}$)\n"
        f"<b>TP1:</b> {round(t1,2)} | <b>TP2:</b> {round(t2,2)}\n"
        f"<b>TP3:</b> {round(t3,2)} | <b>TP4:</b> {round(t4,2)}\n"
        f"━━━━━━━━━━━━\n"
        f"🎯 M15+H1 Synchronized | {datetime.now(WIB).strftime('%H:%M WIB')}"
    )

    if chart_path and os.path.exists(chart_path):
        send_photo(caption, chart_path)
    else:
        send_text(caption)
    
    log(f"SINYAL M15+H1 BERHASIL DIKIRIM: {signal} di {entry:.2f} ({consensus:.0f}%)")
    return 0

if __name__ == "__main__":
    sys.exit(main())
