import Clients.amocrm as amo
import Clients.tochka as tochka


async def runner(order_id: int):
    try:
        company_id, price = await amo.get_entity_data(order_id)
        company_raw_dara = await amo.get_company_data(company_id)
        invoice_id, invoice_num = await tochka.create_invoice(company_raw_dara, price)
        bytes_invoice_file = await tochka.get_invoice(invoice_id)
        uuid = await amo.add_file_in_crm(bytes_invoice_file, invoice_num)
        await amo.link_file_order(order_id=order_id, uuid=uuid)
        await amo.add_tochka_uuid(order_id=order_id, uuid=invoice_id)

    except amo.AmoDataError as e:
        if e.code == 'EMPTY_PRICE':
            await amo.notify_manager(order_id, 'Необходимо заполнить поле "Цена"')
        if e.code == 'EMPTY_COMPANY_DATA':
            await amo.notify_manager(order_id, 'Необходимо заполнить поля компании')
        if e.code == 'INCORRECT_FIELDS_DATA':
            await amo.notify_manager(order_id, 'Необходимо заполнить поле "Трансфер".\nТак же проверьте, что поле трансфер должно быть числовым значением')
        if e.code == 'INCOMPLETE_COMPANY_DATA':
            await amo.notify_manager(order_id, 'Необходимо заполнить поля "ИНН" и имя Юр. лица')

    except Exception as e:
        await amo.notify_manager(order_id, str(e.args[0]))