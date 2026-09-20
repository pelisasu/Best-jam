#!/usr/bin/env python3
# Simulasi Hit/Miss Sinyal Trade Journal terhadap Data Historis
import json
import os
import pandas as pd
import yfinance as yf

JOURNAL_FILE = "trade_journal.json"

def run_simulation():
    if not os.path.exists(JOURNAL_FILE):
        print("File jurnal tidak ditemukan.")
        return

    with open(JOURNAL_FILE, 'r') as f:
        journal = json.load(f)

    if not journal:
        print("Jurnal kosong.")
        return

    print("=" * 50)
    print("📈 SIMULASI AKURASI & HIT RATE (TP vs SL)")
    print("=" * 50)

    # Ambil data historis emas terbaru dari Yahoo Finance untuk pengecekan
    df_hist = yf.Ticker("GC=F").history(period="7d", interval="1h")
    if df_hist is None or len(df_hist) == 0:
        print("Gagal mengambil data historis untuk validasi.")
        return

    tp_hits = 0
    sl_hits = 0
    pending = 0

    for idx, trade in enumerate(journal, 1):
        signal = trade.get("signal")
        entry = trade.get("entry") or trade.get("price")
        sl = trade.get("sl")
        tp1 = trade.get("tp1")
        time_str = trade.get("time")

        if not entry or not sl or not tp1:
            continue

        # Filter data harga setelah waktu sinyal dibuat
        try:
            trade_time = pd.to_datetime(time_str)
            future_data = df_hist[df_hist.index >= trade_time]
        except:
            future_data = pd.DataFrame()

        if len(future_data) == 0:
            status = "PENDING / BELUM TERUJI"
            pending += 1
        else:
            hit = False
            for _, row in future_data.iterrows():
                high = row["High"]
                low = row["Low"]

                if signal == "BUY":
                    if low <= sl:
                        status = "KENA SL ❌"
                        sl_hits += 1
                        hit = True
                        break
                    elif high >= tp1:
                        status = "KENA TP1 ✅"
                        tp_hits += 1
                        hit = True
                        break
                elif signal == "SELL":
                    if high >= sl:
                        status = "KENA SL ❌"
                        sl_hits += 1
                        hit = True
                        break
                    elif low <= tp1:
                        status = "KENA TP1 ✅"
                        tp_hits += 1
                        hit = True
                        break
            if not hit:
                status = "BERJALAN (FLAT)"
                pending += 1

        print(f"{idx}. [{signal}] Entry: {entry} | SL: {sl} | TP1: {tp1} --> Status: {status}")

    print("=" * 50)
    total_evaluated = tp_hits + sl_hits
    if total_evaluated > 0:
        win_rate = (tp_hits / total_evaluated) * 100
        print(f"Total Tervalidasi : {total_evaluated}")
        print(f"TP1 Tercapai      : {tp_hits}")
        print(f"SL Tersentuh      : {sl_hits}")
        print(f"Estimasi Win Rate : {win_rate:.2f}%")
    else:
        print("Belum ada data yang cukup untuk kalkulasi win rate final.")
    print("=" * 50)

if __name__ == "__main__":
    run_simulation()
