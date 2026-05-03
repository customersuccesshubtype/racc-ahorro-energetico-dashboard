#!/usr/bin/env python3
"""
generate_data.py - RACC Estalvi Energetic Dashboard
Calls Tinybird pipe endpoints and writes data.json for GitHub Pages.

Token required: PIPES:READ on racc_funnel, racc_weekly, racc_closure,
                racc_incorrect, racc_raw (scoped read-only token).
"""

import os, json, sys
import urllib.request, urllib.parse
from datetime import date, timedelta, datetime, timezone

# -- CONFIG --------------------------------------------------
TOKEN = os.environ.get("TINYBIRD_TOKEN", "")
START = "2026-01-31"
BASE  = "https://api.tinybird.co/v0/pipes"

if not TOKEN:
    sys.exit("ERROR: TINYBIRD_TOKEN environment variable not set.")

# -- TINYBIRD PIPE CALL --------------------------------------
def tb_pipe(pipe_name, start_date, end_date):
    """Call a Tinybird pipe endpoint with date parameters."""
    params = urllib.parse.urlencode({
        "start_date": start_date,
        "end_date":   end_date,
    })
    url = f"{BASE}/{pipe_name}.json?{params}"
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {TOKEN}"})
    try:
        with urllib.request.urlopen(req, timeout=90) as resp:
            return json.loads(resp.read()).get("data", [])
    except urllib.error.HTTPError as e:
        body = e.read().decode(errors="replace")
        sys.exit(f"Tinybird {pipe_name} HTTP {e.code}: {body[:400]}")

# -- PERIOD RUNNER -------------------------------------------
def run_period(s, e, label):
    """Run funnel, closure and incorrect queries for a given date range."""
    print(f"    funnel ...")
    funnel_rows  = tb_pipe("racc_funnel",    s, e)
    print(f"    closure ...")
    closure_rows = tb_pipe("racc_closure",   s, e)
    print(f"    incorrect ...")
    inc_rows     = tb_pipe("racc_incorrect", s, e)
    return {
        "date_from": s,
        "date_to":   e,
        "funnel":    funnel_rows,
        "closure":   closure_rows,
        "incorrect": inc_rows[0] if inc_rows else {},
    }

# -- MAIN ----------------------------------------------------
if __name__ == "__main__":
    today       = date.today()
    end         = (today + timedelta(days=1)).isoformat()
    month_start = today.replace(day=1).isoformat()
    week_start  = (today - timedelta(days=6)).isoformat()

    print(f"Querying Tinybird pipes (today: {today.isoformat()})")

    print("  [1/5] all-time ...")
    p_all   = run_period(START, end, "all")

    print("  [2/5] this month ...")
    p_month = run_period(month_start, end, "month")

    print("  [3/5] last 7 days ...")
    p_week  = run_period(week_start, end, "week")

    print("  [4/5] weekly trend ...")
    weekly  = tb_pipe("racc_weekly", START, end)
    print(f"    {len(weekly)} rows")

    print("  [5/5] raw conversations ...")
    raw_data = tb_pipe("racc_raw", START, end)
    print(f"    {len(raw_data)} rows")

    data = {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "all":    p_all,
        "month":  p_month,
        "week":   p_week,
        "weekly": weekly,
        "raw":    raw_data,
    }

    with open("data.json", "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    size = len(json.dumps(data, ensure_ascii=False))
    total = sum(int(r.get("conversaciones", 0)) for r in p_all["funnel"])
    contractes = sum(int(r.get("contractes", 0)) for r in p_all["funnel"])
    print(f"\n  data.json written ({size:,} bytes)")
    print(f"  Converses totals (all-time): {total}")
    print(f"  Contractes (all-time): {contractes}")
