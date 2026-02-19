import httpx
from load_dotenv import load_dotenv
import os
from pydantic import BaseModel
from typing import Optional
from datetime import date, timedelta
from random import randint

load_dotenv()
TOCHKA_TOKEN = os.getenv('TOCHKA_TOKEN')
ACCOUNT_NUM = os.getenv('ACCOUNT_NUM')
CUSTOMER_CODE = os.getenv('CUSTOMER_CODE')
TAX_CODE = os.getenv('TAX_CODE')
TOCHKA_BASE_URL = os.getenv('TOCHKA_BASE_URL')

class CompanyData(BaseModel):
    secondSideName: str
    taxCode: str
    legalAddress: Optional[str]
    kpp: Optional[str]
    type: str

async def create_invoice(company, price: float):
    company_data = CompanyData(secondSideName=company.get('name'),
                               taxCode=company.get('vat_id'),
                               legalAddress=company.get('address'),
                               kpp=company.get('kpp'),
                               type=('company', 'ip')[company.get('name')[:2] == 'ИП'])

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
                    "number": str(randint(1000, 9999)),
                    "paymentExpiryDate": (date.today() + timedelta(days=7)).isoformat()}
            }
        }
    }
    async with httpx.AsyncClient() as client:
        response = await client.post(url=f'{TOCHKA_BASE_URL}/bills',
                                     json=payload,
                                     headers={'Authorization': f'Bearer {TOCHKA_TOKEN}',
                                              'Accept': 'application/json',
                                              'Content-Type': 'application/json'})
        response.raise_for_status()
