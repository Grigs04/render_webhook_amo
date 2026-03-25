import asyncio
import json
import os

import httpx
import anyio
import logging
from dotenv import load_dotenv
from Clients.tochka import check_status

load_dotenv()
AMO_BASE_URL = os.getenv('AMO_BASE_URL')
AMO_TOKEN = os.getenv('AMO_TOKEN')

client = httpx.AsyncClient(timeout=httpx.Timeout(30.0, connect=10.0))
HEADERS = {"Authorization": f"Bearer {AMO_TOKEN}", "Content-Type": "application/json"}
logger = logging.getLogger("amocrm")

class AmoDataError(Exception):
    def __init__(self, message: str, code: str | None = None):
        self.code = code
        super().__init__(message)



async def notify_manager(order_id: int, text: str):
    try:
        response = await client.post(
            url=f'{AMO_BASE_URL}/leads/{order_id}/notes',
            headers=HEADERS,
            json=[
                {
                    "note_type": "common",
                    "params": {"text": text},
                }
            ],
        )
        if response.status_code >= 400:
            logger.error(
                "amocrm notify_manager failed status=%s body=%s",
                response.status_code,
                response.text,
            )
        response.raise_for_status()
    except httpx.HTTPError:
        logger.exception("amocrm notify_manager exception order_id=%s", order_id)
        return False
    return True



async def get_entity_data(entity_id: int):
    try:
        response = await _get_with_retry(url=f'{AMO_BASE_URL}/leads/{entity_id}')
        response.raise_for_status()
        data = response.json()

        company_id = data.get('_embedded', {}).get('companies')[0].get('id')

        price = data.get('price', 0)
        if price == 0:
            raise AmoDataError(message='Пустое поле цены', code='EMPTY_PRICE')

        custom_fields = data.get('custom_fields_values') or []
        transfer = (
            t['values'][0]['value']
            for t in custom_fields
            if t.get('field_name') == '\u0422\u0440\u0430\u043d\u0441\u0444\u0435\u0440' and t.get('values')
        )
        total_price = price + sum(map(int, transfer))

    except IndexError:
        raise AmoDataError(message='Пустые поля компании', code='EMPTY_COMPANY_DATA')

    except (TypeError, ValueError):
        raise AmoDataError(message='Сделка карточки не заполнена или трансфер не является числовым значением', code='INCORRECT_FIELDS_DATA')
    return company_id, total_price


async def get_lead(lead_id: int) -> dict:
    response = await _get_with_retry(
        url=f'{AMO_BASE_URL}/leads/{lead_id}',
        params={"with": "contacts,source"},
    )
    response.raise_for_status()
    return response.json()



async def get_company_data(company_id: int):
    try:
        response = await _get_with_retry(url=f'{AMO_BASE_URL}/companies/{company_id}')
        response.raise_for_status()
        custom_fields = response.json().get('custom_fields_values') or []
        company_raw_data = None
        for field in custom_fields:
            if field.get("field_type") != "legal_entity":
                continue
            values = field.get("values") or []
            if not values:
                continue
            company_raw_data = values[0].get("value")
            break

    except TypeError:
        raise AmoDataError(message='Company data is missing or invalid', code='INCORRECT_FIELDS_DATA')
    except IndexError:
        raise AmoDataError(message='Empty company fields', code='EMPTY_COMPANY_DATA')

    if not isinstance(company_raw_data, dict):
        raise AmoDataError(message='Company data is missing or invalid', code='INCORRECT_FIELDS_DATA')

    return company_raw_data


async def add_file_in_crm(file: bytes, file_name: str) -> str:
    response = await client.post(
        url='https://drive-b.amocrm.ru/v1.0/sessions',
        headers=HEADERS,
        json={"file_name": file_name, "file_size": len(file)},
    )
    response.raise_for_status()
    upload_url = response.json().get('upload_url')

    response = await client.post(url=upload_url, data=file)
    response.raise_for_status()

    uuid = response.json().get('uuid')
    return uuid

async def link_file_order(order_id, uuid):
    response = await client.put(url=f'{AMO_BASE_URL}/leads/{order_id}/files',
                                headers=HEADERS,
                                json=[
                                    {
                                        "file_uuid": uuid
                                    }
                                ]
                                )
    response.raise_for_status()

async def add_tochka_uuid(order_id, uuid):
    response = await client.patch(url=f'{AMO_BASE_URL}/leads/{order_id}',
                                  headers=HEADERS,
                                  json={
                                      'id': order_id,
                                      "custom_fields_values": [
                                          {
                                              'field_id': 825929,
                                               'values': [
                                                   {'value': uuid}
                                               ]
                                          }
                                      ]
                                  }
                                  )

    response.raise_for_status()


async def get_orders_uuid():
    leads = await get_deals_by_status(75366150)  # DOCUMENTS/PREPAYMENT
    for lead in leads:
        lead_uuid = None

        custom_fields = lead['custom_fields_values']
        if not custom_fields:
            continue

        for field in custom_fields:
            if field['field_name'] == 'UUID_invoice':
                lead_uuid = field['values'][0]['value']
                break
        if lead_uuid is not None:
            payment_status = await check_status(lead_uuid)
            if payment_status == 'payment_paid':
                await change_lead_status(lead['id'])


async def change_lead_status(order_id):
    response = await client.patch(url=f'{AMO_BASE_URL}/leads/{order_id}',
                                  headers=HEADERS,
                                  json={'id': order_id,
                                        'status_id': 78036790}) # ОПЛАЧЕН
    response.raise_for_status()


async def get_deals_by_status(status_id: int):
    deals = []
    page = 1
    while True:
        response = await _get_with_retry(
            url=f"{AMO_BASE_URL}/leads",
            params={
                "filter[status]": status_id,
                "limit": 250,
                "page": page,
                "with": "contacts",
            },
        )
        response.raise_for_status()
        payload = response.json()
        leads = payload.get("_embedded", {}).get("leads", [])
        for lead in leads:
            deals.append(
                {
                    "id": lead.get("id"),
                    "price": lead.get("price", 0) or 0,
                    "responsible_user_id": lead.get("responsible_user_id"),
                    "custom_fields_values": lead.get("custom_fields_values") or [],
                    "embedded_contacts": lead.get("_embedded", {}).get("contacts") or [],
                }
            )
        if not payload.get("_links", {}).get("next"):
            break
        page += 1
    return deals




async def get_contact(contact_id: int) -> dict:
    response = await _get_with_retry(
        url=f"{AMO_BASE_URL}/contacts/{contact_id}",
    )
    response.raise_for_status()
    return response.json()


async def get_user_name(user_id: int) -> str:
    response = await _get_with_retry(
        url=f"{AMO_BASE_URL}/users/{user_id}",
    )
    response.raise_for_status()
    data = response.json()
    return data.get("name") or data.get("email") or str(user_id)


async def get_users() -> list[dict]:
    response = await _get_with_retry(
        url=f"{AMO_BASE_URL}/users",
    )
    response.raise_for_status()
    data = response.json()
    return data.get("_embedded", {}).get("users", [])


async def get_pipelines() -> list[dict]:
    response = await _get_with_retry(
        url=f"{AMO_BASE_URL}/leads/pipelines",
    )
    response.raise_for_status()
    data = response.json()
    return data.get("_embedded", {}).get("pipelines", [])


async def get_sources() -> list[dict]:
    response = await _get_with_retry(
        url=f"{AMO_BASE_URL}/sources",
    )
    response.raise_for_status()
    data = response.json()
    return data.get("_embedded", {}).get("sources", [])


async def get_leads_updated(
    updated_from: int | None = None,
    updated_to: int | None = None,
    limit: int = 250,
) -> list[dict]:
    leads: list[dict] = []
    page = 1
    while True:
        params: dict[str, int | str] = {"limit": limit, "page": page, "with": "source"}
        if updated_from is not None:
            params["filter[updated_at][from]"] = int(updated_from)
        if updated_to is not None:
            params["filter[updated_at][to]"] = int(updated_to)
        response = await _get_with_retry(
            url=f"{AMO_BASE_URL}/leads",
            params=params,
        )
        response.raise_for_status()
        payload = response.json()
        batch = payload.get("_embedded", {}).get("leads", [])
        leads.extend(batch)
        if not payload.get("_links", {}).get("next"):
            break
        page += 1
    return leads


async def get_events(
    event_types: list[str] | None = None,
    created_from: int | None = None,
    created_to: int | None = None,
    limit: int = 250,
) -> list[dict]:
    events: list[dict] = []
    page = 1
    while True:
        params: list[tuple[str, int | str]] = [("limit", limit), ("page", page)]
        if event_types:
            for event_type in event_types:
                params.append(("filter[type][]", event_type))
        if created_from is not None:
            params.append(("filter[created_at][from]", int(created_from)))
        if created_to is not None:
            params.append(("filter[created_at][to]", int(created_to)))
        response = await _get_with_retry(
            url=f"{AMO_BASE_URL}/events",
            params=params,
        )
        response.raise_for_status()
        payload = response.json()
        batch = payload.get("_embedded", {}).get("events", [])
        events.extend(batch)
        if not payload.get("_links", {}).get("next"):
            break
        page += 1
    return events


def _account_base_url() -> str:
    if not AMO_BASE_URL:
        return ""
    base = AMO_BASE_URL.rstrip("/")
    if base.endswith("/api/v4"):
        base = base[:-7]
    return base


async def get_last_incoming_message_events(limit: int = 10) -> list[dict]:
    if not AMO_BASE_URL or not AMO_TOKEN:
        raise RuntimeError("AMO_BASE_URL/AMO_TOKEN are required")

    base_url = _account_base_url()
    results: list[dict] = []
    page = 1
    while len(results) < limit:
        response = await _get_with_retry(
            url=f"{AMO_BASE_URL}/events",
            params={
                "limit": 100,
                "page": page,
                "filter[type][]": "incoming_chat_message",
            },
        )
        response.raise_for_status()
        payload = response.json()
        events = payload.get("_embedded", {}).get("events", [])
        for event in events:
            if event.get("entity_type") != "lead":
                continue
            lead_id = event.get("entity_id")
            created_at = event.get("created_at")
            if not lead_id or not created_at:
                continue
            lead_url = f"{base_url}/leads/detail/{lead_id}" if base_url else str(lead_id)
            results.append(
                {
                    "created_at": int(created_at),
                    "lead_id": int(lead_id),
                    "lead_url": lead_url,
                }
            )
            if len(results) >= limit:
                break
        if not payload.get("_links", {}).get("next"):
            break
        page += 1
    return results


async def get_chat_response_times(
    start_ts: int | None = None,
    end_ts: int | None = None,
    limit_events: int = 2000,
) -> dict:
    if not AMO_BASE_URL or not AMO_TOKEN:
        raise RuntimeError("AMO_BASE_URL/AMO_TOKEN are required")

    base_url = _account_base_url()
    events: list[dict] = []
    page = 1
    fetched = 0
    while fetched < limit_events:
        params: list[tuple[str, int | str]] = [
            ("limit", 100),
            ("page", page),
            ("filter[type][]", "incoming_chat_message"),
            ("filter[type][]", "outgoing_chat_message"),
        ]
        if start_ts is not None:
            params.append(("filter[created_at][from]", int(start_ts)))
        if end_ts is not None:
            params.append(("filter[created_at][to]", int(end_ts)))

        response = await _get_with_retry(url=f"{AMO_BASE_URL}/events", params=params)
        response.raise_for_status()
        payload = response.json()
        batch = payload.get("_embedded", {}).get("events", [])
        if not batch:
            break
        for event in batch:
            events.append(event)
            fetched += 1
            if fetched >= limit_events:
                break
        if not payload.get("_links", {}).get("next") or fetched >= limit_events:
            break
        page += 1

    events.sort(key=lambda e: int(e.get("created_at") or 0))

    pending: dict[int, list[dict]] = {}
    rows: list[dict] = []
    manager_stats: dict[int, dict] = {}

    for event in events:
        if event.get("entity_type") != "lead":
            continue
        lead_id = event.get("entity_id")
        created_at = event.get("created_at")
        event_type = event.get("type")
        if not lead_id or not created_at or not event_type:
            continue

        lead_id = int(lead_id)
        created_at = int(created_at)

        if event_type == "incoming_chat_message":
            pending.setdefault(lead_id, []).append(event)
            continue

        if event_type != "outgoing_chat_message":
            continue

        queue = pending.get(lead_id)
        if not queue:
            continue

        incoming = queue.pop(0)
        incoming_at = int(incoming.get("created_at") or 0)
        if incoming_at == 0:
            continue

        response_seconds = max(0, created_at - incoming_at)
        manager_id = event.get("created_by")
        if manager_id is None:
            manager_id = 0
        else:
            manager_id = int(manager_id)

        lead_url = f"{base_url}/leads/detail/{lead_id}" if base_url else str(lead_id)
        rows.append(
            {
                "lead_id": lead_id,
                "lead_url": lead_url,
                "incoming_at": incoming_at,
                "outgoing_at": created_at,
                "response_seconds": response_seconds,
                "manager_id": manager_id,
            }
        )

        stats = manager_stats.setdefault(manager_id, {"count": 0, "total_seconds": 0})
        stats["count"] += 1
        stats["total_seconds"] += response_seconds

    for manager_id, stats in manager_stats.items():
        count = stats["count"]
        stats["avg_seconds"] = (stats["total_seconds"] / count) if count else 0

    overall_count = sum(s["count"] for s in manager_stats.values())
    overall_seconds = sum(s["total_seconds"] for s in manager_stats.values())
    overall_avg = (overall_seconds / overall_count) if overall_count else 0

    return {
        "rows": rows,
        "managers": manager_stats,
        "overall_avg_seconds": overall_avg,
        "events_fetched": len(events),
    }



async def _get_with_retry(url: str, params: dict | None = None, retries: int = 3) -> httpx.Response:
    last_error = None
    for attempt in range(retries):
        try:
            response = await client.get(
                url=url,
                params=params,
                headers=HEADERS,
            )
            return response
        except httpx.ConnectTimeout as exc:
            last_error = exc
            if attempt < retries - 1:
                await anyio.sleep(1.0 + attempt)
                continue
            raise
    raise last_error

if __name__ == "__main__":
    result = asyncio.run(get_last_incoming_message_events())
    print(json.dumps(result, indent=2, ensure_ascii=False))
