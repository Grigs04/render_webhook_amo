import httpx
import anyio
import os
from dotenv import load_dotenv
from Clients.tochka import check_status

load_dotenv()
AMO_BASE_URL = os.getenv('AMO_BASE_URL')
AMO_TOKEN = os.getenv('AMO_TOKEN')

client = httpx.AsyncClient(timeout=httpx.Timeout(30.0, connect=10.0))
HEADERS = {"Authorization": f"Bearer {AMO_TOKEN}", "Content-Type": "application/json"}

class AmoDataError(Exception):
    def __init__(self, message: str, code: str | None = None):
        self.code = code
        super().__init__(message)



async def notify_manager(order_id: int, text: str):
    response = await client.post(url=f'{AMO_BASE_URL}/leads/{order_id}/notes',
                                 headers=HEADERS,
                                 json=[{
                                     "note_type": "common",
                                     "params":
                                         {'text': text}
                                 }]
                                 )
    response.raise_for_status()



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
        params={"with": "contacts"},
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




