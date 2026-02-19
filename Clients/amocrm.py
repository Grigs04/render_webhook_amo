import httpx
import json
import os
from load_dotenv import load_dotenv
from pydantic import BaseModel
from typing import Optional, List
from Clients.tochka import create_invoice

load_dotenv()
AMO_BASE_URL = os.getenv('AMO_BASE_URL')
AMO_TOKEN = os.getenv('AMO_TOKEN')

async def get_entity_data(entity_id: int):
    async with httpx.AsyncClient() as client:
        response = await client.get(url=f'{AMO_BASE_URL}/leads/{entity_id}',
                                    headers={'Authorization': f'Bearer {AMO_TOKEN}',
                                             'Content-Type': 'application/json'})
        response.raise_for_status()
        try:
            company_id = response.json().get('_embedded', {}).get('companies')[0].get('id')
            price = response.json().get('price', 0)
            if price == 0:
                raise 'Ошибка! Заполните данные цены в сделке'
        except Exception as e:
            raise f'{e}\nОшибка! Проверьте наличие данных у компании сделки'
        await get_company_data(company_id, price)



async def get_company_data(company_id: int, price: float):
    async with httpx.AsyncClient() as client:
        response = await client.get(url=f'{AMO_BASE_URL}/companies/{company_id}',
                                    headers={'Authorization': f'Bearer {AMO_TOKEN}',
                                             'Content-Type': 'application/json'})
        response.raise_for_status()
        company_raw_data = response.json().get('custom_fields_values', {})[0].get('values')[0].get('value')

        await create_invoice(company_raw_data, price)