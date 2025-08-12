#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse, os, sys, time, zipfile
from datetime import datetime, timedelta
from collections import Counter
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError
import pandas as pd

# Base URL for the data source, formatted according to the specified pattern:
BASE_URL = "https://data.reversebeacon.net/rbn_history/{YMD}.zip"
DATA_DIR = "data"
OUT_TXT = "morse_runner_calls.txt"
OUT_CSV = "top_calls_sp_cw.csv"

# Raw CSV inside the ZIP file does not have headers – columns are described by RBN:
COLS = [
    "poster",                # 0
    "poster_country_prefix", # 1  <-- 'SP' = skimmer in Poland (heard in SP)
    "poster_continent",      # 2
    "freq_khz",              # 3
    "band",                  # 4
    "dx",                    # 5  <-- sender (ranked)
    "dx_country_prefix",     # 6
    "dx_continent",          # 7
    "cq",                    # 8
    "snr_db",                # 9
    "datetime_utc",          # 10
    "wpm",                   # 11
    "mode",                  # 12  <-- 'CW'
    "date_compact",          # 13 (YYYYMMDD)
    "epoch"                  # 14
]

def daterange(d1, d2):
    d = d1
    while d <= d2:
        yield d
        d += timedelta(days=1)

def ensure_dir(p):
    os.makedirs(p, exist_ok=True)

def url_for(d):
    return BASE_URL.format(YMD=d.strftime("%Y%m%d"))

def download_zip(dst, url, retries=3, timeout=60):
    # Resume: if the file already exists and has >0 bytes, skip downloading
    if os.path.exists(dst) and os.path.getsize(dst) > 0:
        return True
    for att in range(1, retries+1):
        try:
            req = Request(url, headers={"User-Agent": "rbn-fetch/1.0"})
            with urlopen(req, timeout=timeout) as r, open(dst, "wb") as f:
                f.write(r.read())
            if os.path.getsize(dst) == 0:
                raise IOError("Empty file")
            return True
        except (HTTPError, URLError, IOError) as e:
            if att < retries:
                time.sleep(2*att)
            else:
                print(f"[WARN] {url} -> {e}", file=sys.stderr)
                return False

def process_zip(path) -> Counter:
    from collections import Counter
    import pandas as pd, zipfile, sys

    c = Counter()
    try:
        with zipfile.ZipFile(path) as zf:
            names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
            if not names:
                return c
            with zf.open(names[0]) as fh:
                # First, attempt the full format (15 columns)
                try:
                    df = pd.read_csv(
                        fh, header=None,
                        names=[
                            "poster","poster_country_prefix","poster_continent",
                            "freq_khz","band","dx","dx_country_prefix","dx_continent",
                            "cq","snr_db","datetime_utc","wpm","mode","date_compact","epoch"
                        ],
                        usecols=["poster_country_prefix","dx","mode"],
                        dtype={"poster_country_prefix":str,"dx":str,"mode":str},
                        low_memory=False
                    )
                except Exception as e:
                    # Reset the file pointer and try the 13-column variant
                    fh.seek(0)
                    df = pd.read_csv(
                        fh, header=None,
                        names=[
                            "poster","poster_country_prefix","poster_continent",
                            "freq_khz","band","dx","dx_country_prefix","dx_continent",
                            "cq","snr_db","datetime_utc","wpm","mode"
                        ],
                        usecols=["poster_country_prefix","dx","mode"],
                        dtype={"poster_country_prefix":str,"dx":str,"mode":str},
                        low_memory=False
                    )

                # Filter rows where mode is 'CW' and the skimmer is in Poland ('SP')
                sub = df[(df["mode"] == "CW") & (df["poster_country_prefix"] == "SP")]
                if not sub.empty:
                    c.update(sub["dx"].value_counts().to_dict())
    except zipfile.BadZipFile:
        print(f"[WARN] Corrupted ZIP file: {path}", file=sys.stderr)
    except Exception as e:
        print(f"[WARN] {path}: {e}", file=sys.stderr)
    return c

# Reason for considering both 13-column and 15-column headers:
# The data source may vary in format depending on the generation process or updates.
# Some files include additional columns (e.g., 'date_compact' and 'epoch'), while others do not.
# To ensure compatibility with all possible formats, we handle both cases.

def main():
    ap = argparse.ArgumentParser(description="TOP-N DX heard in Poland (RBN) – CW")
    ap.add_argument("--from", dest="date_from", default="2024-08-01", help="YYYY-MM-DD")
    ap.add_argument("--to",   dest="date_to",   default="2025-08-10", help="YYYY-MM-DD")
    ap.add_argument("--dir",  dest="data_dir",  default=DATA_DIR,     help="Directory for ZIP files")
    ap.add_argument("--top",  dest="topn", type=int, default=500,     help="Number of entries (default: 500)")
    args = ap.parse_args()

    try:
        d1 = datetime.strptime(args.date_from, "%Y-%m-%d")
        d2 = datetime.strptime(args.date_to,   "%Y-%m-%d")
    except ValueError:
        print("[ERR] Dates must be in YYYY-MM-DD format", file=sys.stderr); sys.exit(2)
    if d2 < d1:
        print("[ERR] date_to < date_from", file=sys.stderr); sys.exit(2)

    ensure_dir(args.data_dir)
    total = Counter()

    for d in daterange(d1, d2):
        url  = url_for(d)
        dst  = os.path.join(args.data_dir, f"{d.strftime('%Y%m%d')}.zip")
        if not download_zip(dst, url):
            continue
        total.update(process_zip(dst))

    if not total:
        print("[ERR] No data after filtering (CW + heard in SP) in the specified range.", file=sys.stderr)
        sys.exit(1)

    top = total.most_common(args.topn)

    # TXT for MorseRunner (one call sign per line)
    with open(OUT_TXT, "w", encoding="utf-8") as f:
        for call, _cnt in top:
            f.write(f"{call}\n")

    # CSV with counts
    pd.DataFrame(top, columns=["callsign","count"]).to_csv(OUT_CSV, index=False)

    print(f"[OK] {OUT_TXT} ({len(top)} calls)")
    print(f"[OK] {OUT_CSV}")

if __name__ == "__main__":
    main()