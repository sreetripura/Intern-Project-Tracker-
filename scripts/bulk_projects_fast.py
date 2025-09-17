#!/usr/bin/env python3
# bulk_projects_fast.py
import os, sys, csv, io, asyncio, random, time
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlencode
import httpx

URL = os.getenv("GITLAB_URL", "").rstrip("/")
TOKEN = os.getenv("GITLAB_TOKEN", "")
if not URL or not TOKEN:
    print("ERROR: set GITLAB_URL and GITLAB_TOKEN", file=sys.stderr)
    sys.exit(1)
API = f"{URL}/api/v4"
HDR = {"PRIVATE-TOKEN": TOKEN, "Accept": "application/json"}

MAX_CONC = int(os.getenv("MAX_CONCURRENCY", "100"))
TIMEOUT = int(os.getenv("HTTP_TIMEOUT", "90"))
RETRIES = int(os.getenv("RETRY_TOTAL", "6"))
BACKOFF = float(os.getenv("BACKOFF_BASE", "0.25"))
COMMITS_DAYS = os.getenv("COMMITS_SINCE_DAYS", "60")
MRS_DAYS = os.getenv("MRS_SINCE_DAYS", "60")
CHECK_COMPLIANCE = os.getenv("SKIP_COMPLIANCE", "false").lower() != "true"
JOB_NAME = os.getenv("COMPLIANCE_JOB_NAME", "COMPLIANCE")
MATCH_MODE = os.getenv("COMPLIANCE_MATCH_MODE", "contains")


def _iso(days: str) -> Optional[str]:
    days = (days or "").strip()
    if not days:
        return None
    import datetime as dt

    return (dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=int(days))).isoformat()


COMMITS_ISO = _iso(COMMITS_DAYS)
MRS_ISO = _iso(MRS_DAYS)


def _clean_csv(path: str) -> Tuple[List[str], str]:
    with open(path, "rb") as f:
        text = f.read().decode("utf-8", errors="ignore")
    r = csv.DictReader(io.StringIO(text, newline=""))
    cols = [(h or "").strip().lower() for h in (r.fieldnames or [])]
    col = None
    for c in cols:
        if c in ("gitlab_username", "username", "user_id"):
            col = c
            break
    if not col:
        raise ValueError("CSV needs gitlab_username/username/user_id column")
    names, seen = [], set()
    for row in r:
        v = (row.get(col) or "").strip()
        if v and v not in seen:
            seen.add(v)
            names.append(v)
    return names, col


async def _backoff(i: int):
    await asyncio.sleep(BACKOFF * (2**i) + random.random() * 0.15)


async def get_json(client, url, params=None):
    if params:
        url = f"{url}?{urlencode(params, doseq=True)}"
    last = None
    for i in range(RETRIES):
        try:
            r = await client.get(url)
            if r.status_code in (429, 500, 502, 503, 504):
                await _backoff(i)
                continue
            return (
                r.status_code,
                r.json() if "json" in r.headers.get("content-type", "").lower() else None,
                r.headers,
            )
        except Exception as e:
            last = e
        await _backoff(i)
    return -1, None, httpx.Headers()


async def head_only(client, url, params=None):
    if params:
        url = f"{url}?{urlencode(params, doseq=True)}"
    last = None
    for i in range(RETRIES):
        try:
            r = await client.head(url)
            if r.status_code in (429, 500, 502, 503, 504):
                await _backoff(i)
                continue
            return r.status_code, r.headers
        except Exception as e:
            last = e
        await _backoff(i)
    return -1, httpx.Headers()


async def resolve_user(client, ident: str):
    if ident.isdigit():
        stc, data, _ = await get_json(client, f"{API}/users/{ident}")
        return data if stc == 200 else None
    stc, data, _ = await get_json(client, f"{API}/users", {"username": ident})
    return data[0] if (stc == 200 and isinstance(data, list) and data) else None


async def list_projects(client, uid: int) -> List[Dict[str, Any]]:
    projs = []
    page = "1"
    while True:
        stc, data, hdr = await get_json(
            client,
            f"{API}/users/{uid}/projects",
            {
                "per_page": 100,
                "page": page,
                "membership": True,
                "archived": False,
                "order_by": "path",
                "sort": "asc",
            },
        )
        if stc != 200 or not isinstance(data, list):
            break
        projs.extend(data)
        nxt = hdr.get("X-Next-Page", "")
        if not nxt:
            break
        page = nxt
    return projs


async def default_branch(client, pid: int, hint: Optional[str]):
    if hint:
        return hint
    stc, data, _ = await get_json(client, f"{API}/projects/{pid}", {"statistics": False})
    return data.get("default_branch") if (stc == 200 and isinstance(data, dict)) else None


def _match(n, t, mode):
    n, t = (n or "").lower(), (t or "").lower()
    return n == t if mode == "exact" else (t in n)


async def commits(client, pid: int, branch: str):
    params = {"ref_name": branch, "per_page": 1}
    if COMMITS_ISO:
        params["since"] = COMMITS_ISO
    stc, hdr = await head_only(client, f"{API}/projects/{pid}/repository/commits", params)
    if stc != 200:
        return 0
    try:
        return int(hdr.get("X-Total", "0"))
    except:
        return 0


async def merged_mrs(client, pid: int):
    params = {"state": "merged", "per_page": 1}
    if MRS_ISO:
        params["updated_after"] = MRS_ISO
    stc, hdr = await head_only(client, f"{API}/projects/{pid}/merge_requests", params)
    if stc != 200:
        return 0
    try:
        return int(hdr.get("X-Total", "0"))
    except:
        return 0


async def compliant(client, pid: int, branch: Optional[str]):
    params = {"per_page": 1, "page": 1}
    if branch:
        params["ref"] = branch
    stc, data, _ = await get_json(client, f"{API}/projects/{pid}/pipelines", params)
    if stc != 200 or not isinstance(data, list) or not data:
        return False
    pl = data[0]
    plid = pl.get("id")
    stc2, jobs, _ = await get_json(
        client, f"{API}/projects/{pid}/pipelines/{plid}/jobs", {"per_page": 100}
    )
    if stc2 != 200 or not isinstance(jobs, list):
        return False
    return any(
        _match(j.get("name", ""), JOB_NAME, MATCH_MODE) and j.get("status") == "success"
        for j in jobs
    )


async def per_project(client, p: Dict[str, Any]):
    pid = int(p["id"])
    dbr = await default_branch(client, pid, p.get("default_branch"))
    c = await commits(client, pid, dbr or "") if dbr else 0
    m = await merged_mrs(client, pid)
    ok = await compliant(client, pid, dbr) if CHECK_COMPLIANCE else False
    return pid, dbr or "", c, m, ok


async def run(inp: str, out_users: str):
    idents, _ = _clean_csv(inp)
    limits = httpx.Limits(max_connections=MAX_CONC, max_keepalive_connections=MAX_CONC)
    sem = asyncio.Semaphore(MAX_CONC)
    t0 = time.time()
    users_rows = []
    async with httpx.AsyncClient(http2=True, limits=limits, headers=HDR, timeout=TIMEOUT) as client:
        for i in range(0, len(idents), 500):
            batch = idents[i : i + 500]
            print(f"[INFO] Batch {i // 500 + 1} size={len(batch)}")

            async def one(ident: str):
                async with sem:
                    u = await resolve_user(client, ident)
                    if not u:
                        users_rows.append(
                            {
                                "username": ident,
                                "user_id": None,
                                "projects": 0,
                                "total_commits": 0,
                                "total_merged_mrs": 0,
                                "compliance_rate": 0.0,
                                "error": "user_not_found",
                            }
                        )
                        return
                    uid = int(u["id"])
                    projs = await list_projects(client, uid)
                    if not projs:
                        users_rows.append(
                            {
                                "username": ident,
                                "user_id": uid,
                                "projects": 0,
                                "total_commits": 0,
                                "total_merged_mrs": 0,
                                "compliance_rate": 0.0,
                            }
                        )
                        return
                    metrics = await asyncio.gather(*[per_project(client, p) for p in projs])
                    total_c = sum(m[2] for m in metrics)
                    total_m = sum(m[3] for m in metrics)
                    compliant_cnt = sum(1 for m in metrics if m[4])
                    rate = (
                        round((compliant_cnt / len(metrics)) * 100.0, 2)
                        if CHECK_COMPLIANCE
                        else 0.0
                    )
                    users_rows.append(
                        {
                            "username": ident,
                            "user_id": uid,
                            "projects": len(metrics),
                            "total_commits": total_c,
                            "total_merged_mrs": total_m,
                            "compliance_rate": rate,
                        }
                    )

            await asyncio.gather(*[one(x) for x in batch])
    # write
    with open(out_users, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "username",
                "user_id",
                "projects",
                "total_commits",
                "total_merged_mrs",
                "compliance_rate",
                "error",
            ],
        )
        w.writeheader()
        for r in users_rows:
            w.writerow(r)
    print(f"[INFO] Done in {time.time() - t0:.1f}s. Output -> {out_users}")


def main():
    if len(sys.argv) < 3:
        print("Usage: python bulk_projects_fast.py users.csv out_users.csv")
        sys.exit(2)
    asyncio.run(run(sys.argv[1], sys.argv[2]))


if __name__ == "__main__":
    main()
