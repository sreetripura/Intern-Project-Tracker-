#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FAST GitLab Project Counter (Streamlit)
- Reads a CSV with column: 'gitlab_username' or 'username'
- For each user: counts projects via HEAD on /users/{id}/projects using X-Total
- Async + httpx (HTTP/2 if available) + retries + concurrency throttle
- Outputs a nice table and CSV download

Env:
  GITLAB_URL   e.g. https://code.swecha.org
  GITLAB_TOKEN Personal Access Token with API scope

Tip for best speed:
  pip install "httpx[http2]"  # enables HTTP/2
"""

import os
import csv
import io
import asyncio
import time
import random
from typing import Any, Dict, List, Optional, Tuple

import streamlit as st
import pandas as pd

# --------------- UI & settings ---------------

st.set_page_config(page_title="GitLab Project Counter", layout="wide")

st.title("⚡ GitLab Project Counter ")

with st.sidebar:
    st.subheader("Connection")
    GITLAB_URL = os.getenv("GITLAB_URL", "").rstrip("/")
    GITLAB_TOKEN = os.getenv("GITLAB_TOKEN", "")
    GITLAB_URL = st.text_input("GITLAB_URL", value=GITLAB_URL or "https://code.swecha.org")
    GITLAB_TOKEN = st.text_input("GITLAB_TOKEN", value=GITLAB_TOKEN, type="password")

    st.subheader("Performance")
    MAX_CONCURRENCY = st.slider("Max concurrency", 10, 200, 120, step=10)
    HTTP_TIMEOUT = st.slider("HTTP timeout (s)", 10, 180, 60, step=10)
    RETRIES = st.slider("Retries", 0, 8, 5, step=1)
    BACKOFF_BASE = st.number_input("Backoff base (seconds)", value=0.25, min_value=0.0, step=0.05)

    st.caption('For maximum speed, install HTTP/2 support:\n\n`pip install "httpx[http2]"`')

if not GITLAB_URL or not GITLAB_TOKEN:
    st.warning("Set **GITLAB_URL** and **GITLAB_TOKEN** in the sidebar to continue.")
    st.stop()

# --------------- Async HTTP core ---------------

import httpx  # needs 'pip install httpx', and optionally 'pip install "httpx[http2]"'

API = f"{GITLAB_URL}/api/v4"
HDR = {"PRIVATE-TOKEN": GITLAB_TOKEN, "Accept": "application/json"}

_sem = asyncio.Semaphore(MAX_CONCURRENCY)


async def _backoff(i: int):
    # exponential + jitter
    await asyncio.sleep(BACKOFF_BASE * (2**i) + random.random() * 0.2)


async def get_json(
    client: httpx.AsyncClient, url: str, params: Optional[Dict[str, Any]] = None
) -> Tuple[int, Optional[Any], httpx.Headers]:
    # GET with retries
    last_exc = None
    for i in range(RETRIES + 1):
        async with _sem:
            try:
                r = await client.get(url, params=params)
                if r.status_code in (429, 500, 502, 503, 504):
                    await _backoff(i)
                    continue
                data = None
                if "application/json" in (r.headers.get("content-type", "").lower()):
                    data = r.json()
                return r.status_code, data, r.headers
            except Exception as e:
                last_exc = e
        if i < RETRIES:
            await _backoff(i)
    return -1, None, httpx.Headers()


async def head_only(
    client: httpx.AsyncClient, url: str, params: Optional[Dict[str, Any]] = None
) -> Tuple[int, httpx.Headers]:
    # Prefer HEAD to read X-Total quickly; fallback to GET if HEAD fails
    last_exc = None
    for i in range(RETRIES + 1):
        async with _sem:
            try:
                r = await client.head(url, params=params)
                if r.status_code in (429, 500, 502, 503, 504):
                    await _backoff(i)
                    continue
                return r.status_code, r.headers
            except Exception as e:
                last_exc = e
        if i < RETRIES:
            await _backoff(i)
    # Fallback GET for servers that don't support HEAD well
    st.info(f"Falling back to GET for {url}")
    st.experimental_rerun()  # keep UI consistent (optional)


# --------------- GitLab helpers ---------------


async def get_user_id(
    client: httpx.AsyncClient, username: str
) -> Tuple[Optional[int], Optional[str]]:
    st_code, data, _ = await get_json(client, f"{API}/users", params={"username": username})
    if st_code == 200 and isinstance(data, list) and data:
        return int(data[0]["id"]), None
    if st_code == 401:
        return None, "401 Unauthorized (check token scopes)"
    if st_code == 404:
        return None, "user_not_found"
    return None, f"{st_code}: lookup_failed"


async def count_projects_for_user(client: httpx.AsyncClient, uid: int) -> Tuple[int, Optional[str]]:
    params = {
        "membership": True,
        "archived": False,
        "per_page": 1,
        "page": 1,
        "order_by": "path",
        "sort": "asc",
    }
    try:
        r = await client.head(f"{API}/users/{uid}/projects", params=params)
        if r.status_code != 200:
            # fallback GET to read headers if HEAD unsupported by instance
            r = await client.get(f"{API}/users/{uid}/projects", params=params)
            if r.status_code != 200:
                if r.status_code == 401:
                    return 0, "401 Unauthorized"
                return 0, f"{r.status_code}: list_failed"
        try:
            total = int(r.headers.get("X-Total", "0"))
        except Exception:
            total = 0
        return total, None
    except Exception as e:
        return 0, f"err: {e}"


# --------------- CSV helpers ---------------


def read_usernames_from_upload(file: bytes) -> List[str]:
    # robust CSV decoding
    text = None
    for enc in ("utf-8", "utf-8-sig", "latin-1"):
        try:
            text = file.decode(enc)
            break
        except Exception:
            continue
    if text is None:
        raise ValueError("Could not decode CSV; please save as UTF-8.")

    # Splitlines preserves rows without tripping on stray newlines
    reader = csv.DictReader(io.StringIO(text))
    if not reader.fieldnames:
        raise ValueError("CSV has no header row.")
    cols = [c.strip().lower() for c in reader.fieldnames]
    if "gitlab_username" in cols:
        col = reader.fieldnames[cols.index("gitlab_username")]
    elif "username" in cols:
        col = reader.fieldnames[cols.index("username")]
    else:
        raise ValueError("CSV must contain 'gitlab_username' or 'username' column.")

    names: List[str] = []
    for row in reader:
        u = (row.get(col) or "").strip()
        if u:
            names.append(u)
    # dedupe, keep order
    seen, out = set(), []
    for u in names:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out


# --------------- Main action ---------------

uploaded = st.file_uploader("Upload CSV with 'gitlab_username' or 'username' column", type=["csv"])
if not uploaded:
    st.info("Template CSV:\n\n```csv\nusername\nalice\nbob\ncharlie\n```")
    st.stop()

try:
    usernames = read_usernames_from_upload(uploaded.getvalue())
except Exception as e:
    st.error(str(e))
    st.stop()

st.write(f"Users to process: **{len(usernames)}**")

run_btn = st.button("Run counter", type="primary")
if not run_btn:
    st.stop()

progress = st.progress(0, text="Starting...")
status_area = st.container()

results: List[Dict[str, Any]] = []
t0 = time.time()

limits = httpx.Limits(max_connections=MAX_CONCURRENCY, max_keepalive_connections=MAX_CONCURRENCY)


# try HTTP/2; if not available, httpx will silently do HTTP/1.1
async def drive():
    async with httpx.AsyncClient(
        http2=True, headers=HDR, timeout=HTTP_TIMEOUT, limits=limits
    ) as client:
        # step 1: resolve user ids in parallel
        id_tasks = [get_user_id(client, u) for u in usernames]
        ids = await asyncio.gather(*id_tasks)

        # step 2: count projects in parallel where id resolved
        count_tasks = []
        name_id_pairs = []
        for username, (uid, err) in zip(usernames, ids):
            if uid is None:
                results.append(
                    {
                        "username": username,
                        "user_id": None,
                        "projects": 0,
                        "error": err or "lookup_failed",
                    }
                )
            else:
                name_id_pairs.append((username, uid))
                count_tasks.append(count_projects_for_user(client, uid))

        done = 0
        for (username, uid), coro in zip(name_id_pairs, count_tasks):
            proj, err = await coro
            results.append(
                {"username": username, "user_id": uid, "projects": proj, "error": err or ""}
            )
            done += 1
            progress.progress(
                min(done / max(len(name_id_pairs), 1), 1.0),
                text=f"Processed {done}/{len(name_id_pairs)} users",
            )


asyncio.run(drive())

elapsed = time.time() - t0

# --------------- Render output ---------------

df = pd.DataFrame(results).sort_values(
    ["projects", "username"], ascending=[False, True], kind="mergesort"
)
st.success(f"Done in {elapsed:.1f}s")
st.dataframe(df, width="stretch")

# CSV download
csv_buf = io.StringIO()
df.to_csv(csv_buf, index=False)
st.download_button(
    "Download CSV",
    data=csv_buf.getvalue().encode("utf-8"),
    file_name="project_counts.csv",
    mime="text/csv",
)
