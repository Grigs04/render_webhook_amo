from datetime import date
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from fastapi.templating import Jinja2Templates

from Services.dashboard_services import get_weekly_conversion_by_manager

router = APIRouter()
templates = Jinja2Templates(directory="templates")


@router.get("/dashboard/conversion")
async def dashboard_conversion(request: Request):
    data = await get_weekly_conversion_by_manager(weeks=26)
    users = data.get("users", {})
    user_list = [{"id": user_id, "name": name} for user_id, name in users.items()]
    user_list.sort(key=lambda item: item["name"])
    return templates.TemplateResponse(
        "dashboard_conversion.html",
        {
            "request": request,
            "user_list": user_list,
        },
    )


@router.get("/dashboard/conversion/data")
async def dashboard_conversion_data(request: Request):
    params = request.query_params
    start = params.get("start")
    end = params.get("end")
    managers_raw = params.get("managers")
    sort_by = params.get("sort_by", "total")
    sort_dir = params.get("sort_dir", "desc")

    start_date = date.fromisoformat(start) if start else None
    end_date = date.fromisoformat(end) if end else None

    manager_ids = []
    if managers_raw:
        for part in managers_raw.split(","):
            part = part.strip()
            if part.isdigit():
                manager_ids.append(int(part))

    data = await get_weekly_conversion_by_manager(
        weeks=26,
        start=start_date,
        end=end_date,
        manager_ids=manager_ids,
        sort_by=sort_by,
        sort_dir=sort_dir,
    )
    return JSONResponse(
        {
            "weeks": data["weeks"],
            "managers": data["managers"],
            "totals": data["totals"],
            "revenues": data["revenues"],
        }
    )
