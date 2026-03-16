import logging
from datetime import date
import Clients.amocrm as amo
import Clients.tochka as tochka

logger = logging.getLogger("invoice")


async def runner(order_id: int) -> dict:
    try:
        logger.info("runner start order_id=%s", order_id)
        company_id, price = await amo.get_entity_data(order_id)
        logger.info("entity data company_id=%s price=%s", company_id, price)
        company_raw_dara = await amo.get_company_data(company_id)
        logger.info("company data loaded company_id=%s", company_id)
        invoice_id = await tochka.create_invoice(company_raw_dara, price, order_id)
        logger.info("invoice created invoice_id=%s", invoice_id)
        bytes_invoice_file = await tochka.get_invoice(invoice_id)
        logger.info("invoice file downloaded invoice_id=%s size=%s", invoice_id, len(bytes_invoice_file))
        file_name = f"Счет №{order_id} от {date.today()}.pdf"
        uuid = await amo.add_file_in_crm(bytes_invoice_file, file_name)
        logger.info("invoice file uploaded uuid=%s", uuid)
        await amo.link_file_order(order_id=order_id, uuid=uuid)
        await amo.add_tochka_uuid(order_id=order_id, uuid=invoice_id)
        await amo.notify_manager(order_id, 'Счет создан')
        logger.info("runner completed order_id=%s", order_id)
        return {"status": "ok"}

    except amo.AmoDataError as e:
        logger.warning("runner amo data error order_id=%s code=%s", order_id, e.code)
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
        logger.exception("runner error order_id=%s", order_id)
        detail = str(e.args[0]) if e.args else str(e)
        await amo.notify_manager(order_id, detail)
        return {"status": "error", "code": "EXCEPTION", "detail": detail}


async def checkker():
    await amo.get_orders_uuid()
