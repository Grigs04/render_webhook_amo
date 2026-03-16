import httpx
import logging
from dotenv import load_dotenv
import os
from pydantic import BaseModel
from typing import Optional
from datetime import date, timedelta
import json

load_dotenv()
TOCHKA_TOKEN = os.getenv('TOCHKA_TOKEN')
ACCOUNT_NUM = os.getenv('ACCOUNT_NUM')
CUSTOMER_CODE = os.getenv('CUSTOMER_CODE')
TAX_CODE = os.getenv('TAX_CODE')
TOCHKA_BASE_URL = os.getenv('TOCHKA_BASE_URL')

client = httpx.AsyncClient(timeout=httpx.Timeout(20.0, connect=5.0))
logger = logging.getLogger("tochka")

class CompanyData(BaseModel):
    secondSideName: str
    taxCode: str
    legalAddress: Optional[str]
    kpp: Optional[str]
    type: str

async def create_invoice(company, price: float, order_id: str):
    payment_date = (date.today() + timedelta(days=7)).isoformat()

    if not all(company.get(field) for field in ['name', 'vat_id']):
        from Clients.amocrm import AmoDataError
        raise AmoDataError(message='Missing company name or VAT ID', code='INCOMPLETE_COMPANY_DATA')

    name = company.get('name', '')
    type_value = 'ip' if name.upper().startswith('РРџ') else 'company'
    company_data = CompanyData(secondSideName=name,
                               taxCode=company.get('vat_id'),
                               legalAddress=company.get('address'),
                               kpp=company.get('kpp'),
                               type=type_value)


    payload = {
        "Data": {
            "accountId": ACCOUNT_NUM,
            "customerCode": CUSTOMER_CODE,
            "SecondSide": company_data.model_dump(exclude_none=True),
            "Content": {
                "Invoice": {
                    "Positions": [
                        {
                            "positionName": "РџСЂРѕРІРµРґРµРЅРёРµ РјРµСЂРѕРїСЂРёСЏС‚РёСЏ",
                            "unitCode": "СѓСЃР»СѓРіР°.",
                            "ndsKind": "without_nds",
                            "price": price,
                            "quantity": "1",
                            "totalAmount": price,
                            "totalNds": "0"
                        }
                    ],
                    "totalAmount": price,
                    "totalNds": "0",
                    "number": str(order_id),
                    "paymentExpiryDate": payment_date}
            }
        }
    }

    response = await client.post(url=f'{TOCHKA_BASE_URL}/bills',
                                 json=payload,
                                 headers={'Authorization': f'Bearer {TOCHKA_TOKEN}',
                                          'Accept': 'application/json'})
    if response.status_code >= 400:
        logger.error("tochka create invoice failed status=%s body=%s", response.status_code, response.text)
    response.raise_for_status()
    invoice_id = response.json().get('Data', {}).get('documentId')

    return invoice_id

async def get_invoice(invoice_id: str):
    response = await client.get(url=f'{TOCHKA_BASE_URL}/bills/{CUSTOMER_CODE}/{invoice_id}/file',
                                 headers={'Authorization': f'Bearer {TOCHKA_TOKEN}',
                                          'Accept': 'application/pdf'})

    response.raise_for_status()

    return await response.aread()

async def check_status(uuid):
    response = await client.get(url=f'{TOCHKA_BASE_URL}/bills/{CUSTOMER_CODE}/{uuid}/payment-status',
                                 headers={'Authorization': f'Bearer {TOCHKA_TOKEN}',
                                          'Accept': 'application/pdf'})
    response.raise_for_status()

    response = response.json()

    result = response.get('Data', {}).get('paymentStatus')
    return result

async def create_act(company, price: float, order_id: str, invoice_uuid: str):
    if not all(company.get(field) for field in ['name', 'vat_id']):
        from Clients.amocrm import AmoDataError
        raise AmoDataError(message='Missing company name or VAT ID', code='INCOMPLETE_COMPANY_DATA')

    name = company.get('name', '')
    type_value = 'ip' if name.upper().startswith('РРџ') else 'company'
    company_data = CompanyData(secondSideName=name,
                               taxCode=company.get('vat_id'),
                               legalAddress=company.get('address'),
                               kpp=company.get('kpp'),
                               type=type_value)

    total_amount = f"{price:.2f}"
    payload = {
        "Data": {
            "accountId": ACCOUNT_NUM,
            "customerCode": CUSTOMER_CODE,
            "SecondSide": {
                "accountId": ACCOUNT_NUM,
                "legalAddress": company_data.legalAddress,
                "kpp": company_data.kpp,
                "taxCode": company_data.taxCode,
                "type": company_data.type,
                "secondSideName": company_data.secondSideName,
            },
            "documentId": invoice_uuid,
            "Content": {
                "Act": {
                    "Positions": [
                        {
                            "positionName": "РџСЂРѕРІРµРґРµРЅРёРµ РјРµСЂРѕРїСЂРёСЏС‚РёСЏ",
                            "unitCode": "СѓСЃР»СѓРіР°.",
                            "ndsKind": "without_nds",
                            "price": total_amount,
                            "quantity": "1",
                            "totalAmount": total_amount,
                            "totalNds": "0",
                        }
                    ],
                    "totalAmount": total_amount,
                    "totalNds": "0",
                    "number": str(order_id),
                }
            }
        }
    }

    response = await client.post(url=f'{TOCHKA_BASE_URL}/closing-documents',
                           headers={'Authorization': f'Bearer {TOCHKA_TOKEN}',
                                    'Accept': 'application/json',
                                    'Content-Type': 'application/json'},
                           json=payload
                           )
    if response.status_code >= 400:
        logger.error("tochka create act failed status=%s body=%s", response.status_code, response.text)
    response.raise_for_status()
    document_id = response.json().get('Data', {}).get('documentId')
    return document_id


async def get_act(document_id: str):
    response = await client.get(
        url=f'{TOCHKA_BASE_URL}/closing-documents/{CUSTOMER_CODE}/{document_id}/file',
        headers={'Authorization': f'Bearer {TOCHKA_TOKEN}',
                 'Accept': 'application/pdf'})

    response.raise_for_status()
    return await response.aread()
