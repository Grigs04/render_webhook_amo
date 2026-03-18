import os
import time
from datetime import date, datetime, timedelta
from typing import Any, Dict, Iterable, List

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

MIN_START_DATE = date(2025, 9, 15)
ORDER_DATE_FIELD_NAME = os.getenv("AMO_ORDER_DATE_FIELD_NAME", "Дата")


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
        base_start = date(2025, 9, 15)
        start_dt = datetime.combine(base_start, datetime.min.time())
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


def _month_bounds(months: int = 6) -> list[tuple[datetime, datetime]]:
    today = date.today()
    first_of_current = date(today.year, today.month, 1)
    end_last_month = datetime.combine(first_of_current, datetime.min.time()) - timedelta(seconds=1)

    bounds: list[tuple[datetime, datetime]] = []
    year = end_last_month.year
    month = end_last_month.month
    for _ in range(months):
        start = datetime(year, month, 1, 0, 0, 0)
        if month == 12:
            next_month_start = datetime(year + 1, 1, 1, 0, 0, 0)
        else:
            next_month_start = datetime(year, month + 1, 1, 0, 0, 0)
        end = next_month_start - timedelta(seconds=1)
        bounds.append((start, end))
        month -= 1
        if month == 0:
            month = 12
            year -= 1
    bounds.reverse()
    return bounds


def _last_full_week_end() -> datetime:
    today = date.today()
    start_of_week = today - timedelta(days=today.weekday())
    return datetime.combine(start_of_week, datetime.min.time()) - timedelta(seconds=1)


def _full_week_bounds(weeks: int = 26) -> list[tuple[datetime, datetime]]:
    end_dt = _last_full_week_end()
    bounds: list[tuple[datetime, datetime]] = []
    for i in range(weeks):
        week_end = end_dt - timedelta(weeks=weeks - 1 - i)
        week_start = (week_end - timedelta(days=6)).replace(hour=0, minute=0, second=0, microsecond=0)
        week_end = week_start + timedelta(days=7) - timedelta(seconds=1)
        bounds.append((week_start, week_end))
    return bounds


def _full_day_bounds(days: int = 30) -> list[tuple[datetime, datetime]]:
    end_dt = datetime.combine(date.today(), datetime.min.time()) - timedelta(seconds=1)
    bounds: list[tuple[datetime, datetime]] = []
    for i in range(days):
        day_start = (end_dt - timedelta(days=days - 1 - i)).replace(hour=0, minute=0, second=0, microsecond=0)
        day_end = day_start + timedelta(days=1) - timedelta(seconds=1)
        bounds.append((day_start, day_end))
    return bounds


def _full_month_bounds(months: int = 12) -> list[tuple[datetime, datetime]]:
    today = date.today()
    first_of_current = date(today.year, today.month, 1)
    end_last_month = datetime.combine(first_of_current, datetime.min.time()) - timedelta(seconds=1)
    bounds: list[tuple[datetime, datetime]] = []
    year = end_last_month.year
    month = end_last_month.month
    for _ in range(months):
        start = datetime(year, month, 1, 0, 0, 0)
        if month == 12:
            next_month_start = datetime(year + 1, 1, 1, 0, 0, 0)
        else:
            next_month_start = datetime(year, month + 1, 1, 0, 0, 0)
        end = next_month_start - timedelta(seconds=1)
        bounds.append((start, end))
        month -= 1
        if month == 0:
            month = 12
            year -= 1
    bounds.reverse()
    return bounds


def _full_quarter_bounds(quarters: int = 8) -> list[tuple[datetime, datetime]]:
    today = date.today()
    current_quarter = (today.month - 1) // 3 + 1
    start_month = (current_quarter - 1) * 3 + 1
    first_of_current = date(today.year, start_month, 1)
    end_last_quarter = datetime.combine(first_of_current, datetime.min.time()) - timedelta(seconds=1)
    bounds: list[tuple[datetime, datetime]] = []
    year = end_last_quarter.year
    month = end_last_quarter.month
    for _ in range(quarters):
        q_start_month = ((month - 1) // 3) * 3 + 1
        start = datetime(year, q_start_month, 1, 0, 0, 0)
        q_end_month = q_start_month + 2
        if q_end_month == 12:
            next_q_start = datetime(year + 1, 1, 1, 0, 0, 0)
        else:
            next_q_start = datetime(year, q_end_month + 1, 1, 0, 0, 0)
        end = next_q_start - timedelta(seconds=1)
        bounds.append((start, end))
        month -= 3
        if month <= 0:
            month += 12
            year -= 1
    bounds.reverse()
    return bounds


def _clamp_bounds(bounds: list[tuple[datetime, datetime]]) -> list[tuple[datetime, datetime]]:
    if not bounds:
        return []
    clamped: list[tuple[datetime, datetime]] = []
    min_dt = datetime.combine(MIN_START_DATE, datetime.min.time())
    for start_dt, end_dt in bounds:
        if end_dt < min_dt:
            continue
        if start_dt < min_dt:
            start_dt = min_dt
        clamped.append((start_dt, end_dt))
    return clamped


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


def _extract_order_date(lead: dict) -> datetime | None:
    custom_fields = lead.get("custom_fields_values") or []
    raw_value = None
    for field in custom_fields:
        if field.get("field_name") != ORDER_DATE_FIELD_NAME:
            continue
        values = field.get("values") or []
        if not values:
            continue
        raw_value = values[0].get("value")
        break
    if raw_value is None:
        return None
    try:
        timestamp = int(float(raw_value))
        if timestamp > 10_000_000_000:
            timestamp = int(timestamp / 1000)
        return datetime.fromtimestamp(timestamp)
    except (TypeError, ValueError):
        pass
    if isinstance(raw_value, str):
        for fmt in ("%Y-%m-%d", "%d.%m.%Y", "%d/%m/%Y"):
            try:
                return datetime.strptime(raw_value, fmt)
            except ValueError:
                continue
    return None


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


async def get_monthly_conversion_rows(months: int = 6) -> List[dict]:
    if not AMO_BASE_URL or not AMO_TOKEN:
        raise RuntimeError("AMO_BASE_URL/AMO_TOKEN are required")

    bounds = _month_bounds(months=months)
    rows: List[dict] = []
    async with httpx.AsyncClient(timeout=httpx.Timeout(30.0, connect=10.0)) as client:
        users = await _fetch_users(client)
        for start_dt, end_dt in bounds:
            leads = await _fetch_leads_for_range(client, start_dt, end_dt)
            stats: Dict[str, dict] = {}
            revenues: Dict[str, float] = {}
            for lead in leads:
                if lead.get("pipeline_id") != PIPELINE_ID:
                    continue
                manager_id = lead.get("responsible_user_id")
                manager_name = users.get(manager_id, str(manager_id))
                if manager_name not in stats:
                    stats[manager_name] = _empty_stats()
                    revenues[manager_name] = 0.0

                stats[manager_name]["total"] += 1
                status_id = lead.get("status_id")
                if status_id in SUCCESS_STATUS_IDS:
                    stats[manager_name]["success"] += 1
                    revenues[manager_name] += float(lead.get("price") or 0)
                elif UNSUCCESS_STATUS_IDS:
                    if status_id in UNSUCCESS_STATUS_IDS:
                        stats[manager_name]["unsuccessful"] += 1
                else:
                    stats[manager_name]["unsuccessful"] += 1

            period_label = f"{start_dt:%d.%m}–{end_dt:%d.%m}"
            for manager_name, data in stats.items():
                total = data["total"]
                ratio = (data["success"] / total) if total else 0.0
                rows.append(
                    {
                        "period": period_label,
                        "manager": manager_name,
                        "total": total,
                        "success": data["success"],
                        "ratio": ratio,
                        "revenue": revenues.get(manager_name, 0.0),
                    }
                )

    rows.sort(key=lambda r: (r["period"], r["manager"]))
    return rows


async def get_weekly_conversion_rows(weeks: int = 26) -> List[dict]:
    if not AMO_BASE_URL or not AMO_TOKEN:
        raise RuntimeError("AMO_BASE_URL/AMO_TOKEN are required")

    bounds = _week_bounds(weeks=weeks)
    rows: List[dict] = []
    async with httpx.AsyncClient(timeout=httpx.Timeout(30.0, connect=10.0)) as client:
        users = await _fetch_users(client)
        for start_dt, end_dt in bounds:
            leads = await _fetch_leads_for_range(client, start_dt, end_dt)
            stats: Dict[str, dict] = {}
            revenues: Dict[str, float] = {}
            for lead in leads:
                if lead.get("pipeline_id") != PIPELINE_ID:
                    continue
                manager_id = lead.get("responsible_user_id")
                manager_name = users.get(manager_id, str(manager_id))
                if manager_name not in stats:
                    stats[manager_name] = _empty_stats()
                    revenues[manager_name] = 0.0

                stats[manager_name]["total"] += 1
                status_id = lead.get("status_id")
                if status_id in SUCCESS_STATUS_IDS:
                    stats[manager_name]["success"] += 1
                    revenues[manager_name] += float(lead.get("price") or 0)
                elif UNSUCCESS_STATUS_IDS:
                    if status_id in UNSUCCESS_STATUS_IDS:
                        stats[manager_name]["unsuccessful"] += 1
                else:
                    stats[manager_name]["unsuccessful"] += 1

            period_label = f"{start_dt:%d.%m.%Y}–{end_dt:%d.%m.%Y}"
            for manager_name, data in stats.items():
                total = data["total"]
                ratio = (data["success"] / total) if total else 0.0
                rows.append(
                    {
                        "period": period_label,
                        "period_start": start_dt.date().isoformat(),
                        "manager": manager_name,
                        "total": total,
                        "success": data["success"],
                        "ratio": ratio,
                        "revenue": revenues.get(manager_name, 0.0),
                    }
                )

    rows.sort(key=lambda r: (r["period_start"], r["manager"]))
    return rows


async def get_financial_rows(granularity: str, offset: int = 0, limit: int = 30) -> dict:
    if not AMO_BASE_URL or not AMO_TOKEN:
        raise RuntimeError("AMO_BASE_URL/AMO_TOKEN are required")

    if granularity == "day":
        bounds = _full_day_bounds(days=365)
    elif granularity == "week":
        bounds = _full_week_bounds(weeks=26)
    elif granularity == "month":
        bounds = _full_month_bounds(months=12)
    elif granularity == "quarter":
        bounds = _full_quarter_bounds(quarters=8)
    else:
        bounds = _full_week_bounds(weeks=26)

    bounds = _clamp_bounds(bounds)
    if not bounds:
        return {"rows": [], "has_more": False}

    if granularity == "day":
        slice_start = offset
        slice_end = offset + limit
        bounds_page = bounds[slice_start:slice_end]
        has_more = slice_end < len(bounds)
    else:
        bounds_page = bounds
        has_more = False

    overall_start = datetime.combine(MIN_START_DATE, datetime.min.time())
    overall_end = datetime.now()
    rows: List[dict] = []

    async with httpx.AsyncClient(timeout=httpx.Timeout(30.0, connect=10.0)) as client:
        leads = await _fetch_leads_for_range(client, overall_start, overall_end)

    intervals = [(int(start.timestamp()), int(end.timestamp())) for start, end in bounds_page]
    totals = [0 for _ in bounds_page]
    successes = [0 for _ in bounds_page]
    revenues = [0.0 for _ in bounds_page]

    for lead in leads:
        if lead.get("pipeline_id") != PIPELINE_ID:
            continue
        order_dt = _extract_order_date(lead)
        if order_dt is None:
            continue
        ts = int(order_dt.replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
        for idx, (start_ts, end_ts) in enumerate(intervals):
            if start_ts <= ts <= end_ts:
                totals[idx] += 1
                status_id = lead.get("status_id")
                if status_id in SUCCESS_STATUS_IDS:
                    successes[idx] += 1
                    revenues[idx] += float(lead.get("price") or 0)
                break

    for idx, (start_dt, end_dt) in enumerate(bounds_page):
        total = totals[idx]
        success = successes[idx]
        revenue = revenues[idx]
        ratio = (success / total) if total else 0.0
        expenses = 0.0
        profit = revenue - expenses

        if granularity == "day":
            period_label = f"{start_dt:%d.%m.%Y}"
        elif granularity == "month":
            period_label = f"{start_dt:%m.%Y}"
        elif granularity == "quarter":
            quarter = (start_dt.month - 1) // 3 + 1
            period_label = f"Q{quarter} {start_dt.year}"
        else:
            period_label = f"{start_dt:%d.%m.%Y}-{end_dt:%d.%m.%Y}"

        rows.append(
            {
                "period": period_label,
                "period_start": start_dt.date().isoformat(),
                "revenue": revenue,
                "ratio": ratio,
                "expenses": expenses,
                "profit": profit,
                "total": total,
                "success": success,
            }
        )

    rows.sort(key=lambda r: r["period_start"])
    return {"rows": rows, "has_more": has_more}
