import asyncio
import httpx
from load_dotenv import load_dotenv
import os
from pydantic import BaseModel
from typing import Optional
from datetime import date, timedelta
from random import randint
import json

load_dotenv()
TOCHKA_TOKEN = os.getenv('TOCHKA_TOKEN')
ACCOUNT_NUM = os.getenv('ACCOUNT_NUM')
CUSTOMER_CODE = os.getenv('CUSTOMER_CODE')
TAX_CODE = os.getenv('TAX_CODE')
TOCHKA_BASE_URL = os.getenv('TOCHKA_BASE_URL')

client = httpx.AsyncClient()

class CompanyData(BaseModel):
    secondSideName: str
    taxCode: str
    legalAddress: Optional[str]
    kpp: Optional[str]
    type: str

async def create_invoice(company, price: float, order_id: str):
    payment_date = (date.today() + timedelta(days=7)).isoformat()

    if not all(company.get(field) for field in ['name', 'vat_id']):
        raise Clients.amocrm.AmoDataError(message='Не заполнены инн или название организации', code='INCOMPLETE_COMPANY_DATA')

    company_data = CompanyData(secondSideName=company.get('name'),
                               taxCode=company.get('vat_id'),
                               legalAddress=company.get('address'),
                               kpp=company.get('kpp'),
                               type='ip' if company.get('name', '').startswith('ИП') else 'company')


    payload = {
        "Data": {
            "accountId": ACCOUNT_NUM,
            "customerCode": CUSTOMER_CODE,
            "SecondSide": company_data.model_dump(exclude_none=True),
            "Content": {
                "Invoice": {
                    "Positions": [
                        {
                            "positionName": "Проведение мероприятия",
                            "unitCode": "услуга.",
                            "ndsKind": "without_nds",
                            "price": price,
                            "quantity": "1",
                            "totalAmount": price,
                            "totalNds": "0"
                        }
                    ],
                    "totalAmount": price,
                    "totalNds": "0",
                    "number": order_id,
                    "paymentExpiryDate": payment_date}
            }
        }
    }

    response = await client.post(url=f'{TOCHKA_BASE_URL}/bills',
                                 json=payload,
                                 headers={'Authorization': f'Bearer {TOCHKA_TOKEN}',
                                          'Accept': 'application/json'})
    response.raise_for_status()
    invoice_id = response.json().get('Data', {}).get('documentId')

    return invoice_id

async def get_invoice(invoice_id: str):
    response = await client.get(url=f'{TOCHKA_BASE_URL}/bills/{CUSTOMER_CODE}/{invoice_id}/file',
                                 headers={'Authorization': f'Bearer {TOCHKA_TOKEN}',
                                          'Accept': 'application/pdf'})

    response.raise_for_status()

    return response.read()

async def check_status(uuid):
    response = await client.get(url=f'{TOCHKA_BASE_URL}/bills/{CUSTOMER_CODE}/{uuid}/payment-status',
                                 headers={'Authorization': f'Bearer {TOCHKA_TOKEN}',
                                          'Accept': 'application/pdf'})
    response.raise_for_status()

    response = response.json()

    result = response.get('Data', {}).get('paymentStatus')
    return result

async def create_act(parent_uuid: str | None = None):
    response = await client.post(url=f'{TOCHKA_BASE_URL}/closing-documents',
                           headers={'Authorization': f'Bearer {TOCHKA_TOKEN}',
                                    'Accept': 'application/pdf'},
                           json={
        "Data": {
            "accountId": ACCOUNT_NUM,
            "customerCode": CUSTOMER_CODE,
            'documentId': '7ab2d914-296b-462a-9b9b-ef07ca143108',
            "Content": "Act"
        }
                           }
                           )

    print(json.dumps(response.json(), indent=2, ensure_ascii=False))