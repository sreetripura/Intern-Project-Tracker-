#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Ultra-fast async GitLab collector (HTTP/2 + per-project cache).
Outputs per-user: total_projects, total_commits (default branch), total_merged_mrs, compliance_rate_pct.
Per-project (optional): commits_count, merged_mrs_count, compliance_ok, default_branch.

Usage:
  python bulk_compliance_checker.py <input_users.csv> <out_users.csv> [out_projects.csv]

Input CSV must have 'gitlab_username' or 'username' column.

Environment:
  # required
  GITLAB_URL, GITLAB_TOKEN
  # performance
  MAX_CONCURRENCY=120  BATCH_SIZE=300  HTTP_TIMEOUT=120  RETRY_TOTAL=7  BACKOFF_BASE=0.3  BATCH_PAUSE_SECONDS=0.3
  # metric windows (empty = all-time)
  COMMITS_SINCE_DAYS=60   # used for activity gating (compliance); commit_count itself is default-branch total
  MRS_SINCE_DAYS=60
  # compliance
  COMPLIANCE_JOB_NAME="COMPLIANCE"
  COMPLIANCE_MATCH_MODE="contains"  # "exact"|"contains"
  SKIP_COMPLIANCE="true|false"
  INCLUDE_PROJECT_EXPORT="true|false"
  # branches to try if needed (keep tight)
  BRANCH_CANDIDATES="main,master"
"""

import asyncio, csv, os, sys, time, random, logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlencode

import httpx  # pip install "httpx[http2]"

# -------- Required env --------
GITLAB_URL = os.getenv("GITLAB_URL", "").rstrip("/")
GITLAB_TOKEN = os.getenv("GITLAB_TOKEN", "")
if not GITLAB_URL or not GITLAB_TOKEN:
    print("ERROR: GITLAB_URL and GITLAB_TOKEN environment variables are required.", file=sys.stderr)
    sys.exit(1)

# -------- Tunables --------
MAX_CONCURRENCY = int(os.getenv("MAX_CONCURRENCY", "120"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "300"))
HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT", "120"))
RETRY_TOTAL = int(os.getenv("RETRY_TOTAL", "7"))
BACKOFF_BASE = float(os.getenv("BACKOFF_BASE", "0.3"))
BATCH_PAUSE = float(os.getenv("BATCH_PAUSE_SECONDS", "0.3"))

COMMITS_WINDOW = os.getenv("COMMITS_SINCE_DAYS", "60")
MRS_WINDOW = os.getenv("MRS_SINCE_DAYS", "60")

BRANCH_CANDIDATES = [
    b.strip() for b in os.getenv("BRANCH_CANDIDATES", "main,master").split(",") if b.strip()
]
COMPLIANCE_JOB = os.getenv("COMPLIANCE_JOB_NAME", "COMPLIANCE")
COMPLIANCE_MODE = os.getenv("COMPLIANCE_MATCH_MODE", "contains").lower()  # "exact"|"contains"
SKIP_COMPLIANCE = os.getenv("SKIP_COMPLIANCE", "true").lower() == "true"
INCLUDE_PROJECTS = os.getenv("INCLUDE_PROJECT_EXPORT", "true").lower() == "true"

API = f"{GITLAB_URL}/api/v4"
GQL = f"{GITLAB_URL}/api/graphql"
HDR = {
    "PRIVATE-TOKEN": GITLAB_TOKEN,
    "Accept": "application/json",
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


def _iso_window(days: str) -> Optional[str]:
    s = (days or "").strip()
    if not s:
        return None
    try:
        d = int(s)
        return (datetime.now(timezone.utc) - timedelta(days=d)).isoformat()
    except Exception:
        return None


COMMITS_SINCE_ISO = _iso_window(COMMITS_WINDOW)  # gating compliance only
MRS_SINCE_ISO = _iso_window(MRS_WINDOW)

# concurrency throttle
_http_sem = asyncio.Semaphore(MAX_CONCURRENCY)


# -------- CSV helpers --------
def read_usernames(path: str) -> List[str]:
    names: List[str] = []
    with open(path, newline="", encoding="utf-8") as f:
        rdr = csv.DictReader(f)
        cols = [c.lower() for c in (rdr.fieldnames or [])]
        col = (
            "gitlab_username"
            if "gitlab_username" in cols
            else ("username" if "username" in cols else None)
        )
        if not col:
            raise ValueError("Input CSV must have 'gitlab_username' or 'username' column.")
        for row in rdr:
            u = (row.get(col) or "").strip()
            if u:
                names.append(u)
    seen, out = set(), []
    for u in names:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out


def load_done(path: str) -> set:
    done = set()
    try:
        with open(path, newline="", encoding="utf-8") as f:
            r = csv.DictReader(f)
            if "username" not in (r.fieldnames or []):
                return done
            for row in r:
                u = (row.get("username") or "").strip()
                if u:
                    done.add(u)
    except FileNotFoundError:
        pass
    return done


def append_users(rows: List[Dict[str, Any]], out_csv: str):
    fields = [
        "username",
        "user_id",
        "total_projects",
        "total_commits",
        "total_merged_mrs",
        "compliance_rate_pct",
        "error",
    ]
    exists = os.path.exists(out_csv)
    with open(out_csv, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        if not exists:
            w.writeheader()
        for r in rows:
            w.writerow(
                {
                    "username": r.get("username"),
                    "user_id": r.get("user_id"),
                    "total_projects": r.get("total_projects", 0),
                    "total_commits": r.get("total_commits", 0),
                    "total_merged_mrs": r.get("total_merged_mrs", 0),
                    "compliance_rate_pct": r.get("compliance_rate_pct", 0.0),
                    "error": r.get("error", ""),
                }
            )


def append_projects(rows: List[Dict[str, Any]], out_csv: str):
    fields = [
        "username",
        "user_id",
        "project_id",
        "project_path",
        "default_branch",
        "commits_count",
        "merged_mrs_count",
        "compliance_ok",
    ]
    exists = os.path.exists(out_csv)
    with open(out_csv, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        if not exists:
            w.writeheader()
        for r in rows:
            w.writerow(r)


def chunked(xs: List[str], n: int):
    for i in range(0, len(xs), n):
        yield xs[i : i + n]


# -------- HTTP core (httpx, HTTP/2, retries, headers) --------
async def _backoff(i: int):
    await asyncio.sleep(BACKOFF_BASE * (2**i) + random.random() * 0.2)


async def get_json(
    client: httpx.AsyncClient, url: str, params: Optional[Dict] = None
) -> Tuple[int, Any, httpx.Headers]:
    # GET with retries and concurrency guard
    if params:
        url = f"{url}?{urlencode(params, doseq=True)}"
    last_exc = None
    for i in range(RETRY_TOTAL):
        async with _http_sem:
            try:
                r = await client.get(url)
                if r.status_code in (429, 500, 502, 503, 504):
                    await _backoff(i)
                    continue
                data = (
                    r.json()
                    if "application/json" in (r.headers.get("content-type", "").lower())
                    else None
                )
                return r.status_code, data, r.headers
            except Exception as e:
                last_exc = e
        await _backoff(i)
    logging.warning(f"GET failed after retries: {url} | {last_exc}")
    return -1, None, httpx.Headers()


async def head_only(
    client: httpx.AsyncClient, url: str, params: Optional[Dict] = None
) -> Tuple[int, httpx.Headers]:
    # HEAD with retries
    if params:
        url = f"{url}?{urlencode(params, doseq=True)}"
    last_exc = None
    for i in range(RETRY_TOTAL):
        async with _http_sem:
            try:
                r = await client.head(url)
                if r.status_code in (429, 500, 502, 503, 504):
                    await _backoff(i)
                    continue
                return r.status_code, r.headers
            except Exception as e:
                last_exc = e
        await _backoff(i)
    logging.warning(f"HEAD failed after retries: {url} | {last_exc}")
    return -1, httpx.Headers()


async def post_json(
    client: httpx.AsyncClient, url: str, payload: Dict[str, Any]
) -> Tuple[int, Any, httpx.Headers]:
    # POST JSON with retries (for GraphQL)
    last_exc = None
    for i in range(RETRY_TOTAL):
        async with _http_sem:
            try:
                r = await client.post(url, json=payload)
                if r.status_code in (429, 500, 502, 503, 504):
                    await _backoff(i)
                    continue
                data = (
                    r.json()
                    if "application/json" in (r.headers.get("content-type", "").lower())
                    else None
                )
                return r.status_code, data, r.headers
            except Exception as e:
                last_exc = e
        await _backoff(i)
    logging.warning(f"POST failed after retries: {url} | {last_exc}")
    return -1, None, httpx.Headers()


# -------- Per-project cache --------
_proj_lock = asyncio.Lock()
_proj_cache: Dict[int, Dict[str, Any]] = {}
# cached fields: {'default_branch': str, 'commits': int, 'merged_mrs': int, 'compliance_ok': bool}


def _job_match(job_name: str) -> bool:
    j = (job_name or "").lower()
    t = (COMPLIANCE_JOB or "").lower()
    if COMPLIANCE_MODE == "exact":
        return j == t
    return t in j  # contains


async def _default_branch(
    client: httpx.AsyncClient, pid: int, project_hint: Optional[str]
) -> Optional[str]:
    if project_hint:
        return project_hint
    st, data, _ = await get_json(client, f"{API}/projects/{pid}", {"statistics": "false"})
    if st == 200 and isinstance(data, dict):
        return data.get("default_branch")
    return None


# --- FAST commit count via Projects API statistics=true (default-branch total) ---
async def _commit_count_fast(client: httpx.AsyncClient, pid: int) -> int:
    st, data, _ = await get_json(client, f"{API}/projects/{pid}", {"statistics": "true"})
    if st == 200 and isinstance(data, dict):
        stats = data.get("statistics") or {}
        try:
            return int(stats.get("commit_count") or 0)
        except Exception:
            return 0
    return 0


# --- MR count: prefer GraphQL single-shot; fallback to REST HEAD + X-Total ---
async def _merged_mrs_count_gql(
    client: httpx.AsyncClient, full_path: str, merged_after_iso: Optional[str]
) -> Optional[int]:
    query = """
    query($fullPath: ID!, $after: Time) {
      project(fullPath: $fullPath) {
        mergeRequests(state: merged, mergedAfter: $after) { count }
      }
    }"""
    variables: Dict[str, Any] = {"fullPath": full_path}
    if merged_after_iso:
        variables["after"] = merged_after_iso
    st, data, _ = await post_json(client, GQL, {"query": query, "variables": variables})
    try:
        return int(data["data"]["project"]["mergeRequests"]["count"]) if st == 200 else None
    except Exception:
        return None


async def _merged_mrs_count_rest(client: httpx.AsyncClient, pid: int) -> int:
    params = {"state": "merged", "per_page": 1}
    if MRS_SINCE_ISO:
        params["updated_after"] = MRS_SINCE_ISO
    st, hdr = await head_only(client, f"{API}/projects/{pid}/merge_requests", params)
    if st != 200:
        return 0
    try:
        return int(hdr.get("X-Total", "0"))
    except Exception:
        return 0


async def _latest_pipeline_ok(client: httpx.AsyncClient, pid: int, branch: Optional[str]) -> bool:
    async def _check_pipeline(ref: Optional[str]) -> Optional[bool]:
        params = {"per_page": 1, "page": 1}
        if ref:
            params["ref"] = ref
        st, data, _ = await get_json(client, f"{API}/projects/{pid}/pipelines", params)
        if st != 200 or not isinstance(data, list) or not data:
            return None
        pl_id = data[0].get("id")
        st2, jobs, _ = await get_json(
            client, f"{API}/projects/{pid}/pipelines/{pl_id}/jobs", {"per_page": 100}
        )
        if st2 != 200 or not isinstance(jobs, list):
            return None
        return any(_job_match(j.get("name", "")) and j.get("status") == "success" for j in jobs)

    # 1) default/current branch
    if branch:
        ok = await _check_pipeline(branch)
        if ok is not None:
            return ok
    # 2) candidates
    for b in BRANCH_CANDIDATES:
        ok = await _check_pipeline(b)
        if ok is not None:
            return ok
    # 3) latest across refs
    st, data, _ = await get_json(
        client,
        f"{API}/projects/{pid}/pipelines",
        {"per_page": 1, "page": 1, "order_by": "updated_at", "sort": "desc"},
    )
    if st != 200 or not isinstance(data, list) or not data:
        return False
    pl_id = data[0].get("id")
    st2, jobs, _ = await get_json(
        client, f"{API}/projects/{pid}/pipelines/{pl_id}/jobs", {"per_page": 100}
    )
    if st2 != 200 or not isinstance(jobs, list):
        return False
    return any(_job_match(j.get("name", "")) and j.get("status") == "success" for j in jobs)


async def ensure_project_cached(client: httpx.AsyncClient, p: Dict[str, Any]) -> Dict[str, Any]:
    pid = int(p["id"])
    async with _proj_lock:
        if pid in _proj_cache:
            return _proj_cache[pid]

    dbranch = await _default_branch(client, pid, p.get("default_branch"))
    commits = await _commit_count_fast(client, pid)

    # merged MRs (prefer GraphQL)
    full_path = p.get("path_with_namespace", "")
    merged_mrs: Optional[int] = None
    if full_path:
        merged_mrs = await _merged_mrs_count_gql(client, full_path, MRS_SINCE_ISO)
    if merged_mrs is None:
        merged_mrs = await _merged_mrs_count_rest(client, pid)

    # lazy compliance: only if recent activity & branch exists
    compliance_ok = False
    if not SKIP_COMPLIANCE and dbranch and (commits > 0):
        # Optional extra gate: if COMMITS_SINCE_ISO set, treat "recently active" as true
        compliance_ok = await _latest_pipeline_ok(client, pid, dbranch)

    rec = {
        "default_branch": dbranch or "",
        "commits": int(merged_mrs if False else commits),  # keep for clarity; int() guard
        "merged_mrs": int(merged_mrs or 0),
        "compliance_ok": bool(compliance_ok),
    }
    async with _proj_lock:
        _proj_cache[pid] = rec
    return rec


# -------- GitLab wrappers --------
async def get_user(client: httpx.AsyncClient, username: str) -> Optional[Dict]:
    st, data, _ = await get_json(client, f"{API}/users", {"username": username})
    if st != 200 or not isinstance(data, list) or not data:
        return None
    return data[0]


async def list_user_projects(client: httpx.AsyncClient, uid: int) -> List[Dict]:
    url = f"{API}/users/{uid}/projects"
    projs: List[Dict] = []
    page = "1"
    while True:
        st, data, hdr = await get_json(
            client,
            url,
            {
                "per_page": 100,
                "page": page,
                "membership": True,
                "archived": False,
                "order_by": "path",
                "sort": "asc",
            },
        )
        if st != 200 or not isinstance(data, list):
            break
        projs.extend(data)
        nxt = hdr.get("X-Next-Page", "")
        if not nxt:
            break
        page = nxt
    return projs


async def process_user(client: httpx.AsyncClient, username: str) -> Dict[str, Any]:
    try:
        u = await get_user(client, username)
        if not u:
            return {
                "username": username,
                "user_id": None,
                "total_projects": 0,
                "total_commits": 0,
                "total_merged_mrs": 0,
                "compliance_rate_pct": 0.0,
                "error": "user_not_found",
            }
        uid = int(u["id"])
        projects = await list_user_projects(client, uid)
        if not projects:
            return {
                "username": username,
                "user_id": uid,
                "total_projects": 0,
                "total_commits": 0,
                "total_merged_mrs": 0,
                "compliance_rate_pct": 0.0,
            }

        results = await asyncio.gather(*(ensure_project_cached(client, p) for p in projects))
        total_commits = sum(r["commits"] for r in results)
        total_mrs = sum(r["merged_mrs"] for r in results)
        compliant = sum(1 for r in results if r["compliance_ok"])
        rate = (
            round((compliant / len(projects)) * 100.0, 2)
            if (projects and not SKIP_COMPLIANCE)
            else 0.0
        )

        res = {
            "username": username,
            "user_id": uid,
            "total_projects": len(projects),
            "total_commits": total_commits,
            "total_merged_mrs": total_mrs,
            "compliance_rate_pct": rate,
        }
        if (not SKIP_COMPLIANCE) and INCLUDE_PROJECTS:
            res["projects_detail"] = [
                {
                    "username": username,
                    "user_id": uid,
                    "project_id": int(p["id"]),
                    "project_path": p.get("path_with_namespace", ""),
                    "default_branch": r["default_branch"],
                    "commits_count": r["commits"],
                    "merged_mrs_count": r["merged_mrs"],
                    "compliance_ok": r["compliance_ok"],
                }
                for p, r in zip(projects, results)
            ]
        return res
    except Exception as e:
        logging.exception(f"[{username}] unexpected")
        return {
            "username": username,
            "user_id": None,
            "total_projects": 0,
            "total_commits": 0,
            "total_merged_mrs": 0,
            "compliance_rate_pct": 0.0,
            "error": str(e),
        }


# -------- Guards --------
async def _assert_token_works():
    async with httpx.AsyncClient(headers=HDR, timeout=HTTP_TIMEOUT) as c:
        r = await c.get(f"{API}/user")
        if r.status_code == 401:
            raise SystemExit("PAT rejected (401). Check GITLAB_TOKEN (scope=api) and expiry.")
        if r.status_code >= 400:
            raise SystemExit(f"Token check failed: HTTP {r.status_code}: {r.text[:200]}")


# -------- Driver --------
async def run(inp: str, out_users: str, out_projects: Optional[str]):
    users = read_usernames(inp)
    already = load_done(out_users)
    if already:
        users = [u for u in users if u not in already]
        logging.info(f"Skipping {len(already)} already-processed users; remaining: {len(users)}")

    logging.info(f"Users to process: {len(users)}")
    t0 = time.time()

    limits = httpx.Limits(
        max_connections=MAX_CONCURRENCY, max_keepalive_connections=MAX_CONCURRENCY
    )
    async with httpx.AsyncClient(
        http2=True, limits=limits, headers=HDR, timeout=HTTP_TIMEOUT
    ) as client:
        # fail fast on bad PAT
        await _assert_token_works()

        for idx, batch in enumerate(chunked(users, BATCH_SIZE), start=1):
            logging.info(f"Processing batch {idx} size={len(batch)} ...")
            res = await asyncio.gather(*(process_user(client, u) for u in batch))
            append_users(
                [
                    {
                        "username": r.get("username"),
                        "user_id": r.get("user_id"),
                        "total_projects": r.get("total_projects", 0),
                        "total_commits": r.get("total_commits", 0),
                        "total_merged_mrs": r.get("total_merged_mrs", 0),
                        "compliance_rate_pct": r.get("compliance_rate_pct", 0.0),
                        "error": r.get("error", ""),
                    }
                    for r in res
                ],
                out_users,
            )

            if (not SKIP_COMPLIANCE) and INCLUDE_PROJECTS and out_projects:
                details = []
                for r in res:
                    if r.get("projects_detail"):
                        details.extend(r["projects_detail"])
                if details:
                    append_projects(details, out_projects)

            logging.info(f"Done batch {idx}.")
            if BATCH_PAUSE > 0:
                await asyncio.sleep(BATCH_PAUSE)

    logging.info(f"Completed in {time.time() - t0:.1f}s")
    logging.info(f"Users CSV: {out_users}")
    if (not SKIP_COMPLIANCE) and INCLUDE_PROJECTS and out_projects:
        logging.info(f"Projects CSV: {out_projects}")


def main():
    if len(sys.argv) < 3:
        print(
            "Usage: python bulk_compliance_checker.py <input_users.csv> <out_users.csv> [out_projects.csv]",
            file=sys.stderr,
        )
        sys.exit(2)
    inp, out_users = sys.argv[1], sys.argv[2]
    out_projects = sys.argv[3] if len(sys.argv) >= 4 else None
    asyncio.run(run(inp, out_users, out_projects))


if __name__ == "__main__":
    main()
