import os
import time
from datetime import datetime
from typing import Dict, Iterable, Tuple

import httpx
from dotenv import load_dotenv

load_dotenv()

AMO_BASE_URL = os.getenv("AMO_BASE_URL")
AMO_TOKEN = os.getenv("AMO_TOKEN")

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


def _week_range_epoch() -> Tuple[int, int]:
    start = datetime(2026, 3, 2, 0, 0, 0)
    end = datetime(2026, 3, 8, 23, 59, 59)
    return int(time.mktime(start.timetuple())), int(time.mktime(end.timetuple()))


def _fetch_leads() -> Iterable[dict]:
    week_from, week_to = _week_range_epoch()
    page = 1
    while True:
        params = {
            "filter[created_at][from]": week_from,
            "filter[created_at][to]": week_to,
            "limit": 250,
            "page": page,
        }
        response = httpx.get(
            f"{AMO_BASE_URL}/leads",
            headers=_get_headers(),
            params=params,
            timeout=httpx.Timeout(30.0, connect=10.0),
        )
        response.raise_for_status()
        payload = response.json()
        leads = payload.get("_embedded", {}).get("leads", [])
        for lead in leads:
            yield lead
        if not payload.get("_links", {}).get("next"):
            break
        page += 1


def _fetch_users() -> Dict[int, str]:
    response = httpx.get(
        f"{AMO_BASE_URL}/users",
        headers=_get_headers(),
        timeout=httpx.Timeout(30.0, connect=10.0),
    )
    response.raise_for_status()
    payload = response.json()
    users = {}
    for user in payload.get("_embedded", {}).get("users", []):
        user_id = user.get("id")
        if user_id is not None:
            users[user_id] = user.get("name") or user.get("email") or str(user_id)
    return users


def compute_conversion_by_manager() -> Dict[str, dict]:
    users = _fetch_users()
    stats: Dict[str, dict] = {}

    for lead in _fetch_leads():
        if lead.get("pipeline_id") != 9411942:
            continue
        manager_id = lead.get("responsible_user_id")
        manager_name = users.get(manager_id, str(manager_id))
        if manager_name not in stats:
            stats[manager_name] = {
                "total": 0,
                "success": 0,
                "unsuccessful": 0,
                "ratio": 0.0,
            }
        stats[manager_name]["total"] += 1

        status_id = lead.get("status_id")
        if status_id in SUCCESS_STATUS_IDS:
            stats[manager_name]["success"] += 1
        elif UNSUCCESS_STATUS_IDS:
            if status_id in UNSUCCESS_STATUS_IDS:
                stats[manager_name]["unsuccessful"] += 1
        else:
            stats[manager_name]["unsuccessful"] += 1

    for manager_name, data in stats.items():
        total = data["total"]
        data["ratio"] = (data["success"] / total) if total else 0.0

    return stats


if __name__ == "__main__":
    print(5)
