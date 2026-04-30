import logging
import time
from datetime import datetime, timezone, timedelta, date
from typing import Any

from Clients import amocrm
from Clients.db import get_pool

logger = logging.getLogger("sync")

TZ_GMT_PLUS_3 = timezone(timedelta(hours=3))

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
    "host_price": 826281,
}


def _now_ts() -> int:
    return int(time.time())


def _to_datetime(value: Any) -> datetime | None:
    ts = _to_int(value)
    if ts is None:
        return None
    if ts > 10_000_000_000:
        ts = int(ts / 1000)
    # DB uses TIMESTAMP without timezone, so store local GMT+3 as naive datetime
    return datetime.fromtimestamp(ts, tz=TZ_GMT_PLUS_3).replace(tzinfo=None)


def _to_date(value: Any) -> date | None:
    ts = _to_int(value)
    if ts is None:
        return None
    if ts > 10_000_000_000:
        ts = int(ts / 1000)
    return datetime.fromtimestamp(ts, tz=TZ_GMT_PLUS_3).date()


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
    parts = []
    for item in values:
        value = item.get("value")
        if value is None:
            continue
        parts.append(str(value))
    if not parts:
        return None
    return "+".join(parts)


def _extract_source_name(lead: dict) -> str | None:
    embedded = lead.get("_embedded") or {}
    source = embedded.get("source")
    if isinstance(source, list):
        source = source[0] if source else None
    if isinstance(source, dict):
        name = source.get("name")
        if name:
            return str(name)
    sources = embedded.get("sources")
    if isinstance(sources, list) and sources:
        name = sources[0].get("name")
        if name:
            return str(name)
    name = lead.get("source_name")
    if name:
        return str(name)
    return None


async def _get_last_updated_at(pool) -> int | None:
    row = await pool.fetchrow("SELECT MAX(amo_updated_at) AS max_ts FROM deals")
    if not row or row["max_ts"] is None:
        return None
    max_ts = row["max_ts"]
    if isinstance(max_ts, datetime):
        if max_ts.tzinfo is not None:
            return int(max_ts.timestamp())
        return int(max_ts.replace(tzinfo=TZ_GMT_PLUS_3).timestamp())
    return _to_int(max_ts)

async def _get_last_event_ts(pool) -> int | None:
    row = await pool.fetchrow("SELECT MAX(created_at) AS max_ts FROM chat_events")
    if not row or row["max_ts"] is None:
        return None
    max_ts = row["max_ts"]
    if isinstance(max_ts, datetime):
        if max_ts.tzinfo is not None:
            return int(max_ts.timestamp())
        return int(max_ts.replace(tzinfo=TZ_GMT_PLUS_3).timestamp())
    return _to_int(max_ts)


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
        if p.get("id")
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
            address, persons_count, payment_method, is_deleted, host_price, profit,
            amo_created_at, amo_closed_at, amo_updated_at, synced_at
        )
        VALUES (
            $1, $2, $3, $4, $5, $6,
            $7, $8, $9, $10, $11, $12, $13,
            $14, $15, $16, $17, $18, $19,
            $20, $21, $22, $23
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
            is_deleted = EXCLUDED.is_deleted,
            host_price = EXCLUDED.host_price,
            profit = EXCLUDED.profit,
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
        status_id_raw = lead.get("status_id")
        status_id = int(status_id_raw) if status_id_raw is not None else None
        manager_id_raw = lead.get("responsible_user_id")
        manager_id = int(manager_id_raw) if manager_id_raw is not None else None
        if manager_id == 0:
            manager_id = None
        source_name = _extract_source_name(lead)

        custom_fields = lead.get("custom_fields_values") or []
        city = _extract_text(custom_fields, FIELD_IDS["city"])
        tariff = _extract_multiselect(custom_fields, FIELD_IDS["tariff"])
        format_value = _extract_text(custom_fields, FIELD_IDS["format"])
        hours_count = _extract_int(custom_fields, FIELD_IDS["hours_count"])
        hosts_count = _extract_int(custom_fields, FIELD_IDS["hosts_count"])
        event_date = _to_date(_extract_int(custom_fields, FIELD_IDS["event_date"]))
        start_time = _extract_text(custom_fields, FIELD_IDS["start_time"])
        address = _extract_text(custom_fields, FIELD_IDS["address"])
        persons_count = _extract_int(custom_fields, FIELD_IDS["persons_count"])
        payment_method = _extract_text(custom_fields, FIELD_IDS["payment_method"])
        is_deleted = bool(lead.get("is_deleted", False))
        host_price = _extract_int(custom_fields, FIELD_IDS["host_price"])
        deal_price = _to_int(lead.get("price"))
        profit = None
        if deal_price is not None and host_price is not None:
            profit = deal_price - host_price

        created_at = _to_datetime(lead.get("created_at"))
        updated_at = _to_datetime(lead.get("updated_at"))
        if created_at is None or updated_at is None:
            continue

        records.append(
            (
                int(lead_id),
                pipeline_id,
                status_id,
                manager_id,
                str(source_name) if source_name else None,
                deal_price,
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
                is_deleted,
                host_price,
                profit,
                created_at,
                _to_datetime(lead.get("closed_at")),
                updated_at,
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


async def _mark_deals_deleted(pool, deal_ids: list[int]) -> int:
    if not deal_ids:
        return 0
    await pool.execute(
        "UPDATE deals SET is_deleted = TRUE, synced_at = $1 WHERE amo_deal_id = ANY($2)",
        _now_ts(),
        deal_ids,
    )
    return len(deal_ids)


async def _ensure_deals_exist(pool, deal_ids: list[int]) -> int:
    if not deal_ids:
        return 0
    rows = await pool.fetch(
        "SELECT amo_deal_id FROM deals WHERE amo_deal_id = ANY($1)",
        deal_ids,
    )
    existing = {int(row["amo_deal_id"]) for row in rows}
    missing = [deal_id for deal_id in deal_ids if deal_id not in existing]

    leads: list[dict] = []
    ids_to_delete: list[int] = []

    for deal_id in missing:
        try:
            lead = await amocrm.get_lead(deal_id)
            leads.append(lead)
        except amocrm.LeadDeletedError:
            logger.info("lead not found in amo (204), skipping deal_id=%s", deal_id)
        except Exception:
            logger.exception("failed to fetch deal for event deal_id=%s", deal_id)

    for deal_id in existing:
        try:
            await amocrm.get_lead(deal_id)
        except amocrm.LeadDeletedError:
            ids_to_delete.append(deal_id)
            logger.info("deal deleted in amo, marking is_deleted=True deal_id=%s", deal_id)
        except Exception:
            logger.exception("failed to check deletion status deal_id=%s", deal_id)

    inserted = await _upsert_deals(pool, leads)
    deleted = await _mark_deals_deleted(pool, ids_to_delete)
    return inserted + deleted

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
        created_at = _to_datetime(event.get("created_at"))
        if created_at is None:
            continue
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
