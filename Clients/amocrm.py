import httpx
import json
import os
from load_dotenv import load_dotenv
from datetime import date

load_dotenv()
AMO_BASE_URL = os.getenv('AMO_BASE_URL')
AMO_TOKEN = os.getenv('AMO_TOKEN')

client = httpx.AsyncClient()

class AmoDataError(Exception):
    def __init__(self, message: str, code: str | None = None):
        self.code = code
        super().__init__(message)

async def notify_manager(order_id: int, text: str):
    response = await client.post(url=f'{AMO_BASE_URL}/leads/{order_id}/notes',
                                 headers={'Authorization': f'Bearer {AMO_TOKEN}',
                                          'Content-Type': 'application/json'},
                                 json=[{
                                     "note_type": "common",
                                     "params":
                                         {'text': text}
                                 }]
                                 )
    response.raise_for_status()



async def get_entity_data(entity_id: int):
    try:
        response = await client.get(url=f'{AMO_BASE_URL}/leads/{entity_id}',
                                    headers={'Authorization': f'Bearer {AMO_TOKEN}',
                                             'Content-Type': 'application/json'})
        response.raise_for_status()
        data = response.json()

        company_id = data.get('_embedded', {}).get('companies')[0].get('id')

        price = data.get('price', 0)
        if price == 0:
            raise AmoDataError(message='Пустое поле цены', code='EMPTY_PRICE')

        transfer = (t['values'][0]['value'] for t in data.get('custom_fields_values') if t['field_name'] == 'Трансфер')
        total_price = price + sum(map(int, transfer))

    except IndexError:
        raise AmoDataError(message='Пустые поля компании', code='EMPTY_COMPANY_DATA')

    except (TypeError, ValueError):
        raise AmoDataError(message='Сделка карточки не заполнена или трансфер не является числовым значением', code='INCORRECT_FIELDS_DATA')
    return company_id, total_price



async def get_company_data(company_id: int):
    try:
        response = await client.get(url=f'{AMO_BASE_URL}/companies/{company_id}',
                                    headers={'Authorization': f'Bearer {AMO_TOKEN}',
                                             'Content-Type': 'application/json'})
        response.raise_for_status()
        company_raw_data = response.json().get('custom_fields_values', {})[0].get('values')[0].get('value')

    except TypeError:
        raise AmoDataError(message='Сделка карточки не заполнена или трансфер не является числовым значением',
                           code='INCORRECT_FIELDS_DATA')

    return company_raw_data

async def add_file_in_crm(file, invoice_num):
    response = await client.post(url='https://drive-b.amocrm.ru/v1.0/sessions',
                                 headers={'Authorization': f'Bearer {AMO_TOKEN}',
                                          'Content-Type': 'application/json'},
                                 json={"file_name": f"Счёт №{invoice_num} от {date.today()}.pdf",
                                       "file_size": len(file)}
                                 )
    response.raise_for_status()
    upload_url = response.json().get('upload_url')


    response = await client.post(url=upload_url,
                                 data=file)
    response.raise_for_status()

    uuid = response.json().get('uuid')

    return uuid

async def link_file_order(order_id, uuid):
    response = await client.put(url=f'{AMO_BASE_URL}/leads/{order_id}/files',
                                headers={'Authorization': f'Bearer {AMO_TOKEN}',
                                         'Content-Type': 'application/json'},
                                json=[
                                    {
                                        "file_uuid": uuid
                                    }
                                ]
                                )
    response.raise_for_status()

