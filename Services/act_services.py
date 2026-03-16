import logging
import asyncio
from datetime import date
import Clients.amocrm as amo
import Clients.tochka as tochka

logger = logging.getLogger("act")

INVOICE_UUID_FIELD_ID = 825929


def _extract_invoice_uuid(custom_fields: list[dict]) -> str:
    for field in custom_fields:
        if field.get("field_id") != INVOICE_UUID_FIELD_ID:
            continue
        values = field.get("values") or []
        if not values:
            return ""
        value = values[0].get("value")
        return str(value) if value else ""
    return ""


async def runner(order_id: int) -> dict:
    try:
        logger.info("act runner start order_id=%s", order_id)
        company_id, price = await amo.get_entity_data(order_id)
        lead = await amo.get_lead(order_id)
        custom_fields = lead.get("custom_fields_values") or []
        invoice_uuid = _extract_invoice_uuid(custom_fields)
        if not invoice_uuid:
            detail = "Не найден UUID счета в сделке"
            await amo.notify_manager(order_id, detail)
            return {"status": "error", "code": "MISSING_INVOICE_UUID", "detail": detail}

        company_raw_data = await amo.get_company_data(company_id)
        act_id = await tochka.create_act(company_raw_data, price, order_id, invoice_uuid)
        act_file = await tochka.get_act(act_id)
        file_name = f"Акт №{act_id} от {date.today()}.pdf"
        uuid = await amo.add_file_in_crm(act_file, file_name)
        await amo.link_file_order(order_id=order_id, uuid=uuid)
        logger.info("act created act_id=%s order_id=%s", act_id, order_id)
        await amo.notify_manager(order_id, f"Акт создан: {act_id}")
        return {"status": "ok", "act_id": act_id}

    except amo.AmoDataError as e:
        logger.warning("act runner amo data error order_id=%s code=%s", order_id, e.code)
        detail = "Ошибка данных сделки"
        if e.code == 'EMPTY_PRICE':
            detail = 'Необходимо заполнить поле "Цена"'
            await amo.notify_manager(order_id, detail)
        if e.code == 'EMPTY_COMPANY_DATA':
            detail = 'Необходимо заполнить поля компании'
            await amo.notify_manager(order_id, detail)
        if e.code == 'INCORRECT_FIELDS_DATA':
            detail = 'Необходимо заполнить поле "Трансфер".\nТак же проверьте, что поле трансфер должно быть числовым значением'
            await amo.notify_manager(order_id, detail)
        if e.code == 'INCOMPLETE_COMPANY_DATA':
            detail = 'Необходимо заполнить поля "ИНН" и имя Юр. лица'
            await amo.notify_manager(order_id, detail)
        return {"status": "error", "code": e.code, "detail": detail}

    except Exception as e:
        logger.exception("act runner error order_id=%s", order_id)
        detail = str(e.args[0]) if e.args else str(e)
        await amo.notify_manager(order_id, detail)
        return {"status": "error", "code": "EXCEPTION", "detail": detail}
