import logging
import time
from typing import Any

from Clients import amocrm
from Clients.db import get_pool

logger = logging.getLogger("sync")

PIPELINE_ID = 9411942
WON_STATUS_IDS = {142, 75366150, 78036790}
LOST_STATUS_IDS = {143}
FIELD_IDS = {
    "city": 814191,
    "tariff": 825601,
    "format": 817871,
    "hours_count": 822635,
    "hosts_count": 814211,
    "event_date": 814193,
    "start_time": 814203,
    "address": 814201,
    "persons_count": 814205,
    "payment_method": 814197,
}


def _now_ts() -> int:
    return int(time.time())


def _first_value(field: dict) -> Any:
    values = field.get("values") or []
    if not values:
        return None
    return values[0].get("value")


def _to_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _field_by_id(custom_fields: list[dict], field_id: int) -> dict | None:
    for field in custom_fields:
        if field.get("field_id") == field_id:
            return field
    return None


def _extract_text(custom_fields: list[dict], field_id: int) -> str | None:
    field = _field_by_id(custom_fields, field_id)
    if not field:
        return None
    value = _first_value(field)
    if value is None:
        return None
    return str(value)


def _extract_int(custom_fields: list[dict], field_id: int) -> int | None:
    field = _field_by_id(custom_fields, field_id)
    if not field:
        return None
    return _to_int(_first_value(field))


def _extract_multiselect(custom_fields: list[dict], field_id: int) -> str | None:
    field = _field_by_id(custom_fields, field_id)
    if not field:
        return None
    values = field.get("values") or []
    if not values:
        return None
    first = values[0].get("value")
    return str(first) if first is not None else None


async def _get_last_updated_at(pool) -> int | None:
    row = await pool.fetchrow("SELECT MAX(amo_updated_at) AS max_ts FROM deals")
    if not row or row["max_ts"] is None:
        return None
    return int(row["max_ts"])

async def _get_last_event_ts(pool) -> int | None:
    row = await pool.fetchrow("SELECT MAX(created_at) AS max_ts FROM chat_events")
    if not row or row["max_ts"] is None:
        return None
    return int(row["max_ts"])


async def _upsert_pipelines(pool, pipelines: list[dict]) -> int:
    if not pipelines:
        return 0
    sql = """
        INSERT INTO pipelines (amo_pipeline_id, name, synced_at)
        VALUES ($1, $2, $3)
        ON CONFLICT (amo_pipeline_id)
        DO UPDATE SET name = EXCLUDED.name, synced_at = EXCLUDED.synced_at
    """
    synced_at = _now_ts()
    records = [
        (int(p["id"]), p.get("name") or "", synced_at)
        for p in pipelines
        if p.get("id") and int(p["id"]) == PIPELINE_ID
    ]
    if not records:
        return 0
    await pool.executemany(sql, records)
    return len(records)


def _status_id_to_code(status_id: int | None) -> int:
    if status_id is None:
        return 0
    if status_id in WON_STATUS_IDS:
        return 1
    if status_id in LOST_STATUS_IDS:
        return 2
    return 0


async def _upsert_statuses(pool, statuses: list[dict]) -> int:
    if not statuses:
        return 0
    sql = """
        INSERT INTO pipeline_statuses (
            amo_status_id, pipeline_id, name, deal_status
        )
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (amo_status_id)
        DO UPDATE SET
            pipeline_id = EXCLUDED.pipeline_id,
            name = EXCLUDED.name,
            deal_status = EXCLUDED.deal_status
    """
    records = []
    for status in statuses:
        status_id = status.get("id")
        pipeline_id = status.get("pipeline_id")
        if not status_id or not pipeline_id:
            continue
        if int(pipeline_id) != PIPELINE_ID:
            continue
        records.append(
            (
                int(status_id),
                int(pipeline_id),
                status.get("name") or "",
                _status_id_to_code(int(status_id)),
            )
        )
    if not records:
        return 0
    await pool.executemany(sql, records)
    return len(records)


async def _upsert_managers(pool, managers: list[dict]) -> int:
    if not managers:
        return 0
    sql = """
        INSERT INTO managers (amo_user_id, name, synced_at)
        VALUES ($1, $2, $3)
        ON CONFLICT (amo_user_id)
        DO UPDATE SET name = EXCLUDED.name, synced_at = EXCLUDED.synced_at
    """
    synced_at = _now_ts()
    records = [
        (int(user["id"]), user.get("name") or user.get("email") or str(user["id"]), synced_at)
        for user in managers
        if user.get("id")
    ]
    if not records:
        return 0
    await pool.executemany(sql, records)
    return len(records)


async def _upsert_deals(
    pool,
    deals: list[dict],
) -> int:
    if not deals:
        return 0
    sql = """
        INSERT INTO deals (
            amo_deal_id, pipeline_id, status_id, responsible_user_id, source_name, price,
            city, tariff, format, hours_count, hosts_count, event_date, start_time,
            address, persons_count, payment_method, amo_created_at, amo_closed_at,
            amo_updated_at, synced_at
        )
        VALUES (
            $1, $2, $3, $4, $5, $6,
            $7, $8, $9, $10, $11, $12, $13,
            $14, $15, $16, $17, $18, $19,
            $20
        )
        ON CONFLICT (amo_deal_id)
        DO UPDATE SET
            pipeline_id = EXCLUDED.pipeline_id,
            status_id = EXCLUDED.status_id,
            responsible_user_id = EXCLUDED.responsible_user_id,
            source_name = EXCLUDED.source_name,
            price = EXCLUDED.price,
            city = EXCLUDED.city,
            tariff = EXCLUDED.tariff,
            format = EXCLUDED.format,
            hours_count = EXCLUDED.hours_count,
            hosts_count = EXCLUDED.hosts_count,
            event_date = EXCLUDED.event_date,
            start_time = EXCLUDED.start_time,
            address = EXCLUDED.address,
            persons_count = EXCLUDED.persons_count,
            payment_method = EXCLUDED.payment_method,
            amo_created_at = EXCLUDED.amo_created_at,
            amo_closed_at = EXCLUDED.amo_closed_at,
            amo_updated_at = EXCLUDED.amo_updated_at,
            synced_at = EXCLUDED.synced_at
    """
    synced_at = _now_ts()
    records = []
    for lead in deals:
        lead_id = lead.get("id")
        if not lead_id:
            continue
        pipeline_id_raw = lead.get("pipeline_id")
        pipeline_id = int(pipeline_id_raw) if pipeline_id_raw is not None else None
        if pipeline_id != PIPELINE_ID:
            continue
        status_id_raw = lead.get("status_id")
        status_id = int(status_id_raw) if status_id_raw is not None else None
        manager_id_raw = lead.get("responsible_user_id")
        manager_id = int(manager_id_raw) if manager_id_raw is not None else None
        source_name = None
        embedded = lead.get("_embedded", {})
        source = embedded.get("source") or {}
        if isinstance(source, dict):
            source_name = source.get("name")

        custom_fields = lead.get("custom_fields_values") or []
        city = _extract_text(custom_fields, FIELD_IDS["city"])
        tariff = _extract_multiselect(custom_fields, FIELD_IDS["tariff"])
        format_value = _extract_text(custom_fields, FIELD_IDS["format"])
        hours_count = _extract_int(custom_fields, FIELD_IDS["hours_count"])
        hosts_count = _extract_int(custom_fields, FIELD_IDS["hosts_count"])
        event_date = _extract_int(custom_fields, FIELD_IDS["event_date"])
        start_time = _extract_text(custom_fields, FIELD_IDS["start_time"])
        address = _extract_text(custom_fields, FIELD_IDS["address"])
        persons_count = _extract_int(custom_fields, FIELD_IDS["persons_count"])
        payment_method = _extract_text(custom_fields, FIELD_IDS["payment_method"])

        records.append(
            (
                int(lead_id),
                pipeline_id,
                status_id,
                manager_id,
                str(source_name) if source_name else None,
                _to_int(lead.get("price")),
                city,
                tariff,
                format_value,
                hours_count,
                hosts_count,
                event_date,
                start_time,
                address,
                persons_count,
                payment_method,
                _to_int(lead.get("created_at")) or 0,
                _to_int(lead.get("closed_at")),
                _to_int(lead.get("updated_at")) or 0,
                synced_at,
            )
        )

    if not records:
        return 0
    await pool.executemany(sql, records)
    return len(records)


def _event_origin(event: dict) -> str | None:
    value_after = event.get("value_after") or []
    if not value_after:
        return None
    first = value_after[0] if isinstance(value_after, list) else {}
    message = first.get("message") if isinstance(first, dict) else None
    if not isinstance(message, dict):
        return None
    origin = message.get("origin")
    return str(origin) if origin else None


async def _ensure_deals_exist(pool, deal_ids: list[int]) -> int:
    if not deal_ids:
        return 0
    rows = await pool.fetch(
        "SELECT amo_deal_id FROM deals WHERE amo_deal_id = ANY($1)",
        deal_ids,
    )
    existing = {int(row["amo_deal_id"]) for row in rows}
    missing = [deal_id for deal_id in deal_ids if deal_id not in existing]
    if not missing:
        return 0
    leads: list[dict] = []
    for deal_id in missing:
        try:
            lead = await amocrm.get_lead(deal_id)
            leads.append(lead)
        except Exception:
            logger.exception("failed to fetch deal for event deal_id=%s", deal_id)
    if not leads:
        return 0
    return await _upsert_deals(pool, leads)

async def _get_existing_deal_ids(pool, deal_ids: list[int]) -> set[int]:
    if not deal_ids:
        return set()
    rows = await pool.fetch(
        "SELECT amo_deal_id FROM deals WHERE amo_deal_id = ANY($1)",
        deal_ids,
    )
    return {int(row["amo_deal_id"]) for row in rows}


async def _upsert_chat_events(pool, events: list[dict]) -> int:
    if not events:
        return 0
    sql = """
        INSERT INTO chat_events (
            amo_event_id, amo_deal_id, amo_user_id, direction, origin, created_at
        )
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (amo_event_id)
        DO UPDATE SET
            amo_deal_id = EXCLUDED.amo_deal_id,
            amo_user_id = EXCLUDED.amo_user_id,
            direction = EXCLUDED.direction,
            origin = EXCLUDED.origin,
            created_at = EXCLUDED.created_at
    """
    records = []
    for event in events:
        event_id = event.get("id")
        if not event_id:
            continue
        if event.get("entity_type") != "lead":
            continue
        deal_id = event.get("entity_id")
        if not deal_id:
            continue
        event_type = event.get("type")
        if event_type == "incoming_chat_message":
            direction = "inbound"
        elif event_type == "outgoing_chat_message":
            direction = "outbound"
        else:
            continue
        created_at = _to_int(event.get("created_at")) or 0
        created_by = _to_int(event.get("created_by"))
        amo_user_id = None if not created_by else created_by
        records.append(
            (
                str(event_id),
                int(deal_id),
                int(amo_user_id) if amo_user_id else None,
                direction,
                _event_origin(event),
                created_at,
            )
        )
    if not records:
        return 0
    await pool.executemany(sql, records)
    return len(records)


async def sync_reference_data() -> dict[str, int]:
    pool = await get_pool()

    pipelines = await amocrm.get_pipelines()
    statuses: list[dict] = []
    for pipeline in pipelines:
        embedded = pipeline.get("_embedded", {})
        for status in embedded.get("statuses", []):
            status["pipeline_id"] = pipeline.get("id")
            statuses.append(status)

    pipeline_count = await _upsert_pipelines(pool, pipelines)
    status_count = await _upsert_statuses(pool, statuses)
    managers = await amocrm.get_users()
    manager_count = await _upsert_managers(pool, managers)

    return {
        "pipelines": pipeline_count,
        "statuses": status_count,
        "managers": manager_count,
    }


async def sync_deals_and_events(updated_from: int | None = None) -> dict[str, int]:
    pool = await get_pool()

    if updated_from is None:
        last_updated = await _get_last_updated_at(pool)
        if last_updated:
            updated_from = max(0, last_updated - 24 * 60 * 60)

    deals = await amocrm.get_leads_updated(updated_from=updated_from)
    deals_count = await _upsert_deals(pool, deals)

    last_event_ts = await _get_last_event_ts(pool)
    if last_event_ts:
        events_from = max(0, last_event_ts - 24 * 60 * 60)
    else:
        events_from = None

    events = await amocrm.get_events(
        event_types=["incoming_chat_message", "outgoing_chat_message"],
        created_from=events_from,
    )
    deal_ids = []
    for event in events:
        if event.get("entity_type") != "lead":
            continue
        deal_id = event.get("entity_id")
        if deal_id:
            deal_ids.append(int(deal_id))
    await _ensure_deals_exist(pool, deal_ids)
    existing_deals = await _get_existing_deal_ids(pool, deal_ids)
    filtered_events = [
        event
        for event in events
        if event.get("entity_type") == "lead"
        and event.get("entity_id")
        and int(event.get("entity_id")) in existing_deals
    ]
    events_count = await _upsert_chat_events(pool, filtered_events)

    return {
        "deals": deals_count,
        "chat_events": events_count,
    }


async def sync_crm_to_db(
    updated_from: int | None = None, full: bool = False
) -> dict[str, int]:
    if not full:
        return await sync_deals_and_events(updated_from=updated_from)
    ref_result = await sync_reference_data()
    data_result = await sync_deals_and_events(updated_from=updated_from)
    return {**ref_result, **data_result}
