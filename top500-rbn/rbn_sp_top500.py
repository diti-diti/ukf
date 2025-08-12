#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, sys, time, zipfile
from datetime import datetime, timedelta
from collections import Counter
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError

import pandas as pd
import streamlit as st

BASE_URL = "https://data.reversebeacon.net/rbn_history/{YMD}.zip"
DATA_DIR = "data"

COLS15 = [
    "poster","poster_country_prefix","poster_continent","freq_khz","band",
    "dx","dx_country_prefix","dx_continent","cq","snr_db","datetime_utc",
    "wpm","mode","date_compact","epoch"
]
COLS13 = [
    "poster","poster_country_prefix","poster_continent","freq_khz","band",
    "dx","dx_country_prefix","dx_continent","cq","snr_db","datetime_utc",
    "wpm","mode"
]
HDR_FIRST = "callsign,de_pfx,de_cont,freq"  # Header for telegraphy CSV format

def daterange(d1, d2):
    # Generate a range of dates between d1 and d2
    d = d1
    while d <= d2:
        yield d
        d += timedelta(days=1)

def url_for(d):
    # Format the URL for a specific date
    return BASE_URL.format(YMD=d.strftime("%Y%m%d"))

def ensure_dir(p):
    # Ensure the directory exists
    os.makedirs(p, exist_ok=True)

def download_zip(dst, url, retries=2, timeout=50):
    # Download a ZIP file from the given URL with retry logic
    if os.path.exists(dst) and os.path.getsize(dst) > 0:
        return True
    for att in range(1, retries+1):
        try:
            req = Request(url, headers={"User-Agent": "rbn-fetch/1.0"})
            with urlopen(req, timeout=timeout) as r, open(dst, "wb") as f:
                f.write(r.read())
            if os.path.getsize(dst) == 0:
                raise IOError("empty")
            return True
        except (HTTPError, URLError, IOError):
            if att == retries:
                return False
            time.sleep(1.5 * att)
    return False

def read_one_csv_from_zip(zpath):
    # Extract and process a CSV file from a ZIP archive
    try:
        with zipfile.ZipFile(zpath) as zf:
            names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
            if not names:
                return None
            name = names[0]
            with zf.open(name) as fh:
                head = fh.readline().decode("utf-8", "ignore").strip()
                fh.seek(0)
                if head.lower().startswith(HDR_FIRST):
                    # Telegraphy CSV format with header
                    df = pd.read_csv(
                        fh, usecols=["de_pfx", "dx", "tx_mode", "band"],
                        dtype=str, low_memory=False
                    )
                    df = df.rename(columns={"de_pfx":"src_pfx", "tx_mode":"mode"})
                    return df[["src_pfx","dx","mode","band"]]
                try:
                    # Raw format without header (15 columns)
                    df = pd.read_csv(
                        fh, header=None, names=COLS15,
                        usecols=["poster_country_prefix","dx","mode","band"],
                        dtype=str, low_memory=False
                    )
                    df = df.rename(columns={"poster_country_prefix":"src_pfx"})
                    return df[["src_pfx","dx","mode","band"]]
                except Exception:
                    # Fallback to 13-column format
                    fh.seek(0)
                    df = pd.read_csv(
                        fh, header=None, names=COLS13,
                        usecols=["poster_country_prefix","dx","mode","band"],
                        dtype=str, low_memory=False
                    )
                    df = df.rename(columns={"poster_country_prefix":"src_pfx"})
                    return df[["src_pfx","dx","mode","band"]]
    except zipfile.BadZipFile:
        return None
    except Exception:
        return None

@st.cache_data(show_spinner=False)
def aggregate_counts(date_from, date_to, band_filter, topn, fetch=True):
    # Aggregate counts of callsigns heard in Poland within the specified date range
    d1 = datetime.strptime(date_from, "%Y-%m-%d")
    d2 = datetime.strptime(date_to, "%Y-%m-%d")
    ensure_dir(DATA_DIR)
    total = Counter()
    days = sum(1 for _ in daterange(d1, d2))
    done = 0
    for d in daterange(d1, d2):
        dst = os.path.join(DATA_DIR, f"{d.strftime('%Y%m%d')}.zip")
        url = url_for(d)
        if fetch:
            download_zip(dst, url)
        if not os.path.exists(dst) or os.path.getsize(dst) == 0:
            done += 1
            st.progress(done / days, text=f"Skipped {d.date()} (file missing)")
            continue
        df = read_one_csv_from_zip(dst)
        done += 1
        st.progress(done / days, text=f"Processing {d.date()}")
        if df is None or df.empty:
            continue
        # Filter: heard in Poland + CW mode
        sub = df[(df["src_pfx"] == "SP") & (df["mode"] == "CW")]
        if band_filter and band_filter != "all":
            sub = sub[sub["band"] == band_filter]
        if sub.empty:
            continue
        total.update(sub["dx"].value_counts().to_dict())
    if not total:
        return pd.DataFrame(columns=["callsign","count"])
    top = total.most_common(topn)
    return pd.DataFrame(top, columns=["callsign","count"])

st.set_page_config(page_title="RBN – TOP CW heard in SP", layout="wide")
st.title("RBN – TOP CW heard in Poland (heard by SP skimmers)")

col1, col2, col3, col4 = st.columns(4)
with col1:
    date_from = st.date_input("From", value=datetime(2025,1,1)).strftime("%Y-%m-%d")
with col2:
    date_to   = st.date_input("To", value=datetime(2025,1,31)).strftime("%Y-%m-%d")
with col3:
    topn      = st.number_input("TOP N", min_value=50, max_value=2000, value=500, step=50)
with col4:
    band      = st.selectbox("Band", ["all","160m","80m","60m","40m","30m","20m","17m","15m","12m","10m"])

fetch = st.checkbox("Fetch missing ZIPs", value=True)
go = st.button("CALCULATE")

if go:
    with st.spinner("Calculating…"):
        df = aggregate_counts(date_from, date_to, band, topn, fetch)
    if df.empty:
        st.warning("No data after filtering (CW + heard in SP) for the selected period.")
    else:
        st.success(f"Done. Records: {len(df)}")
        st.download_button("Download CSV", df.to_csv(index=False).encode("utf-8"), "top_calls_sp_cw.csv", "text/csv")
        txt = "\n".join(df["callsign"].tolist())
        st.download_button("Download TXT (MorseRunner)", txt.encode("utf-8"), "morse_runner_calls.txt", "text/plain")
        st.subheader("TOP Table")
        st.dataframe(df, use_container_width=True)
        st.subheader("TOP – Bar Chart")
        st.bar_chart(df.set_index("callsign"))
else:
    st.info("Set the date range, TOP N, and click CALCULATE.")