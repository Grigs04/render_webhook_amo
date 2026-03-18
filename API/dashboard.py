from datetime import datetime, timezone
from fastapi import APIRouter, Request
from fastapi.templating import Jinja2Templates

from fastapi.responses import JSONResponse
from Clients import amocrm
from Services.dashboard_services import get_financial_rows

router = APIRouter()
templates = Jinja2Templates(directory="templates")


@router.get("/dashboard/conversion")
async def dashboard_conversion(request: Request):
    data = await get_financial_rows(granularity="week")
    return templates.TemplateResponse(
        "dashboard_conversion.html",
        {
            "request": request,
            "rows": data["rows"],
        },
    )


@router.get("/dashboard/finance/data")
async def dashboard_finance_data(request: Request):
    granularity = request.query_params.get("granularity", "week")
    offset = int(request.query_params.get("offset", "0"))
    limit = int(request.query_params.get("limit", "30"))
    data = await get_financial_rows(granularity=granularity, offset=offset, limit=limit)
    return JSONResponse({"rows": data["rows"], "has_more": data["has_more"]})


@router.get("/dashboard/incoming-messages")
async def dashboard_incoming_messages():
    events = await amocrm.get_last_incoming_message_events(limit=10)
    rows = []
    for event in events:
        ts = datetime.fromtimestamp(event["created_at"], tz=timezone.utc).isoformat()
        rows.append(
            {
                "datetime": ts,
                "lead_id": event["lead_id"],
                "lead_url": event["lead_url"],
            }
        )
    return JSONResponse({"rows": rows})


@router.get("/dashboard/response-time")
async def dashboard_response_time(request: Request):
    from_ts_raw = request.query_params.get("from")
    to_ts_raw = request.query_params.get("to")
    limit_raw = request.query_params.get("limit")

    now_ts = int(datetime.now(tz=timezone.utc).timestamp())
    default_from = now_ts - 7 * 24 * 60 * 60

    try:
        start_ts = int(from_ts_raw) if from_ts_raw else default_from
    except ValueError:
        start_ts = default_from

    try:
        end_ts = int(to_ts_raw) if to_ts_raw else now_ts
    except ValueError:
        end_ts = now_ts

    try:
        limit_events = int(limit_raw) if limit_raw else 2000
    except ValueError:
        limit_events = 2000

    data = await amocrm.get_chat_response_times(
        start_ts=start_ts,
        end_ts=end_ts,
        limit_events=limit_events,
    )

    rows = []
    for row in data["rows"]:
        rows.append(
            {
                "incoming_at": datetime.fromtimestamp(row["incoming_at"], tz=timezone.utc).isoformat(),
                "outgoing_at": datetime.fromtimestamp(row["outgoing_at"], tz=timezone.utc).isoformat(),
                "response_seconds": row["response_seconds"],
                "lead_id": row["lead_id"],
                "lead_url": row["lead_url"],
                "manager_id": row["manager_id"],
            }
        )

    managers = []
    for manager_id, stats in data["managers"].items():
        name = "unknown"
        if manager_id:
            name = await amocrm.get_user_name(manager_id)
        managers.append(
            {
                "manager_id": manager_id,
                "manager_name": name,
                "count": stats["count"],
                "avg_seconds": stats["avg_seconds"],
                "total_seconds": stats["total_seconds"],
            }
        )
    managers.sort(key=lambda m: m["avg_seconds"])

    return JSONResponse(
        {
            "rows": rows,
            "managers": managers,
            "overall_avg_seconds": data["overall_avg_seconds"],
            "events_fetched": data["events_fetched"],
            "from": start_ts,
            "to": end_ts,
        }
    )
