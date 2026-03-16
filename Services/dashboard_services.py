import os
import time
from datetime import date, datetime, timedelta
from typing import Any, Dict, Iterable

import httpx

AMO_BASE_URL = os.getenv("AMO_BASE_URL")
AMO_TOKEN = os.getenv("AMO_TOKEN")

PIPELINE_ID = int(os.getenv("AMO_PIPELINE_ID", "9411942"))

SUCCESS_STATUS_IDS = {
    int(s)
    for s in (os.getenv("SUCCESS_STATUS_IDS", "75366150,78036790,142").split(","))
    if s.strip().isdigit()
}
UNSUCCESS_STATUS_IDS = {
    int(s)
    for s in (os.getenv("UNSUCCESS_STATUS_IDS", "143").split(","))
    if s.strip().isdigit()
}


def _get_headers() -> Dict[str, str]:
    return {"Authorization": f"Bearer {AMO_TOKEN}", "Content-Type": "application/json"}


def _week_bounds(
    weeks: int = 26,
    start: date | None = None,
    end: date | None = None,
) -> list[tuple[datetime, datetime]]:
    today = date.today()
    start_of_week = today - timedelta(days=today.weekday())
    start_dt = datetime.combine(start_of_week, datetime.min.time())
    last_week_end = start_dt - timedelta(seconds=1)

    if end is None:
        end_dt = last_week_end
    else:
        end_dt = datetime.combine(end, datetime.max.time())
        if end_dt > last_week_end:
            end_dt = last_week_end

    if start is None:
        start_dt = (end_dt - timedelta(weeks=weeks)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
    else:
        start_week = start - timedelta(days=start.weekday())
        start_dt = datetime.combine(start_week, datetime.min.time())

    if start_dt > end_dt:
        return []

    bounds: list[tuple[datetime, datetime]] = []
    cursor = start_dt
    while cursor <= end_dt:
        week_start = cursor
        week_end = week_start + timedelta(days=7) - timedelta(seconds=1)
        if week_end > end_dt:
            week_end = end_dt
        bounds.append((week_start, week_end))
        cursor = week_start + timedelta(days=7)
    return bounds


async def _fetch_users(client: httpx.AsyncClient) -> Dict[int, str]:
    response = await client.get(
        f"{AMO_BASE_URL}/users",
        headers=_get_headers(),
    )
    response.raise_for_status()
    payload = response.json()
    users: Dict[int, str] = {}
    for user in payload.get("_embedded", {}).get("users", []):
        user_id = user.get("id")
        if user_id is not None:
            users[user_id] = user.get("name") or user.get("email") or str(user_id)
    return users


async def _fetch_leads_for_range(
    client: httpx.AsyncClient, start_dt: datetime, end_dt: datetime
) -> list[dict[str, Any]]:
    start_ts = int(time.mktime(start_dt.timetuple()))
    end_ts = int(time.mktime(end_dt.timetuple()))
    page = 1
    results: list[dict[str, Any]] = []
    while True:
        params = {
            "filter[created_at][from]": start_ts,
            "filter[created_at][to]": end_ts,
            "limit": 250,
            "page": page,
        }
        response = await client.get(
            f"{AMO_BASE_URL}/leads",
            headers=_get_headers(),
            params=params,
        )
        response.raise_for_status()
        payload = response.json()
        leads = payload.get("_embedded", {}).get("leads", [])
        results.extend(leads)
        if not payload.get("_links", {}).get("next"):
            break
        page += 1
    return results


def _empty_stats() -> dict:
    return {"total": 0, "success": 0, "unsuccessful": 0, "ratio": 0.0}


async def get_weekly_conversion_by_manager(
    weeks: int = 26,
    start: date | None = None,
    end: date | None = None,
    manager_ids: Iterable[int] | None = None,
    sort_by: str = "total",
    sort_dir: str = "desc",
) -> dict:
    if not AMO_BASE_URL or not AMO_TOKEN:
        raise RuntimeError("AMO_BASE_URL/AMO_TOKEN are required")

    bounds = _week_bounds(weeks=weeks, start=start, end=end)
    manager_ids_set = set(manager_ids or [])
    async with httpx.AsyncClient(timeout=httpx.Timeout(30.0, connect=10.0)) as client:
        users = await _fetch_users(client)

        weeks_data: list[dict[str, Any]] = []
        totals: Dict[str, dict] = {}
        total_revenues: Dict[str, float] = {}

        for week_start, week_end in bounds:
            leads = await _fetch_leads_for_range(client, week_start, week_end)
            stats: Dict[str, dict] = {}
            summary = {"total": 0, "success": 0, "unsuccessful": 0, "ratio": 0.0, "revenue": 0.0}
            revenues: Dict[str, float] = {}

            for lead in leads:
                if lead.get("pipeline_id") != PIPELINE_ID:
                    continue
                manager_id = lead.get("responsible_user_id")
                if manager_ids_set and manager_id not in manager_ids_set:
                    continue
                manager_name = users.get(manager_id, str(manager_id))
                if manager_name not in stats:
                    stats[manager_name] = _empty_stats()
                if manager_name not in totals:
                    totals[manager_name] = _empty_stats()
                if manager_name not in revenues:
                    revenues[manager_name] = 0.0
                if manager_name not in total_revenues:
                    total_revenues[manager_name] = 0.0

                stats[manager_name]["total"] += 1
                totals[manager_name]["total"] += 1
                summary["total"] += 1

                status_id = lead.get("status_id")
                if status_id in SUCCESS_STATUS_IDS:
                    stats[manager_name]["success"] += 1
                    totals[manager_name]["success"] += 1
                    summary["success"] += 1
                    revenue = float(lead.get("price") or 0)
                    summary["revenue"] += revenue
                    revenues[manager_name] += revenue
                    total_revenues[manager_name] += revenue
                elif UNSUCCESS_STATUS_IDS:
                    if status_id in UNSUCCESS_STATUS_IDS:
                        stats[manager_name]["unsuccessful"] += 1
                        totals[manager_name]["unsuccessful"] += 1
                        summary["unsuccessful"] += 1
                else:
                    stats[manager_name]["unsuccessful"] += 1
                    totals[manager_name]["unsuccessful"] += 1
                    summary["unsuccessful"] += 1

            for manager_name, data in stats.items():
                total = data["total"]
                data["ratio"] = (data["success"] / total) if total else 0.0
            summary_total = summary["total"]
            summary["ratio"] = (summary["success"] / summary_total) if summary_total else 0.0

            weeks_data.append(
                {
                    "label": f"{week_start:%d.%m.%Y} - {week_end:%d.%m.%Y}",
                    "start": week_start.isoformat(),
                    "end": week_end.isoformat(),
                    "stats": stats,
                    "revenues": revenues,
                    "summary": summary,
                }
            )

        for manager_name, data in totals.items():
            total = data["total"]
            data["ratio"] = (data["success"] / total) if total else 0.0

    sort_map = {
        "ratio": lambda name: totals[name]["ratio"],
        "success": lambda name: totals[name]["success"],
        "total": lambda name: totals[name]["total"],
        "unsuccessful": lambda name: totals[name]["unsuccessful"],
        "revenue": lambda name: total_revenues.get(name, 0.0),
    }
    key_fn = sort_map.get(sort_by, sort_map["total"])
    managers = sorted(
        totals.keys(),
        key=key_fn,
        reverse=(sort_dir != "asc"),
    )

    return {
        "weeks": weeks_data,
        "managers": managers,
        "totals": totals,
        "revenues": total_revenues,
        "users": users,
    }
