import asyncio
import logging
import os
import tempfile
from datetime import date, datetime, timedelta
from typing import Any
import anyio
import io
from docxtpl import DocxTemplate
from docx2pdf import convert as docx2pdf_convert

import Clients.amocrm as amo

logger = logging.getLogger("agreement")

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
TEMPLATE_PATH = os.path.join(BASE_DIR, "templates", "contract_template.docx")


def _get_custom_field(custom_fields: list[dict[str, Any]], name: str) -> str:
    for field in custom_fields:
        if field.get("field_name") != name:
            continue
        values = field.get("values") or []
        if not values:
            return ""
        value = values[0].get("value")
        if value is None:
            return ""
        return str(value)
    return ""


def _get_custom_field_by_names(custom_fields: list[dict[str, Any]], names: list[str]) -> str:
    for name in names:
        value = _get_custom_field(custom_fields, name)
        if value:
            return value
    return ""


def _format_date(value: str) -> str:
    if not value:
        return ""
    try:
        timestamp = int(float(value))
        return date.fromtimestamp(timestamp).strftime("%d.%m.%Y")
    except (TypeError, ValueError):
        pass
    for fmt in ("%Y-%m-%d", "%d.%m.%Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(value, fmt).strftime("%d.%m.%Y")
        except ValueError:
            continue
    return value


def _format_money(value: Any) -> str:
    if value is None:
        return ""
    try:
        return str(int(float(value)))
    except (TypeError, ValueError):
        return str(value)


def _int_to_words_ru(value: int) -> str:
    if value == 0:
        return "ноль"

    units = (
        ("", "", ""),
        ("один", "одна", "одно"),
        ("два", "две", "два"),
        ("три", "три", "три"),
        ("четыре", "четыре", "четыре"),
        ("пять", "пять", "пять"),
        ("шесть", "шесть", "шесть"),
        ("семь", "семь", "семь"),
        ("восемь", "восемь", "восемь"),
        ("девять", "девять", "девять"),
    )
    teens = (
        "десять",
        "одиннадцать",
        "двенадцать",
        "тринадцать",
        "четырнадцать",
        "пятнадцать",
        "шестнадцать",
        "семнадцать",
        "восемнадцать",
        "девятнадцать",
    )
    tens = (
        "",
        "",
        "двадцать",
        "тридцать",
        "сорок",
        "пятьдесят",
        "шестьдесят",
        "семьдесят",
        "восемьдесят",
        "девяносто",
    )
    hundreds = (
        "",
        "сто",
        "двести",
        "триста",
        "четыреста",
        "пятьсот",
        "шестьсот",
        "семьсот",
        "восемьсот",
        "девятьсот",
    )

    def _group_name(n: int, forms: tuple[str, str, str]) -> str:
        n = abs(n) % 100
        if 10 < n < 20:
            return forms[2]
        n = n % 10
        if n == 1:
            return forms[0]
        if 2 <= n <= 4:
            return forms[1]
        return forms[2]

    def _group_to_words(n: int, gender: int) -> list[str]:
        words: list[str] = []
        h = n // 100
        t = (n % 100) // 10
        u = n % 10
        if h:
            words.append(hundreds[h])
        if t == 1:
            words.append(teens[u])
            return words
        if t:
            words.append(tens[t])
        if u:
            words.append(units[u][gender])
        return words

    groups = [
        (0, ("", "", ""), 0),
        (1, ("тысяча", "тысячи", "тысяч"), 1),
        (2, ("миллион", "миллиона", "миллионов"), 0),
        (3, ("миллиард", "миллиарда", "миллиардов"), 0),
    ]

    parts: list[str] = []
    group_index = 0
    while value > 0 and group_index < len(groups):
        value, chunk = divmod(value, 1000)
        if chunk:
            _, forms, gender = groups[group_index]
            words = _group_to_words(chunk, gender)
            if forms[0]:
                words.append(_group_name(chunk, forms))
            parts = words + parts
        group_index += 1

    return " ".join([w for w in parts if w])


def _money_to_words_ru(value: Any) -> str:
    try:
        amount = int(float(value))
    except (TypeError, ValueError):
        return str(value)

    words = _int_to_words_ru(amount)
    rub_form = ("рубль", "рубля", "рублей")
    n = abs(amount) % 100
    if 10 < n < 20:
        ruble = rub_form[2]
    else:
        n = n % 10
        if n == 1:
            ruble = rub_form[0]
        elif 2 <= n <= 4:
            ruble = rub_form[1]
        else:
            ruble = rub_form[2]
    return f"{words} {ruble}"


def _calc_finish_time(start_time: str, hours: str) -> str:
    if not start_time or not hours:
        return ""
    try:
        duration = float(hours)
    except (TypeError, ValueError):
        return ""
    for fmt in ("%H:%M", "%H.%M"):
        try:
            start_dt = datetime.strptime(start_time, fmt)
            finish = start_dt + timedelta(hours=duration)
            return finish.strftime("%H:%M")
        except ValueError:
            continue
    return ""


def _get_contact_field(contact: dict[str, Any], field_code: str) -> str:
    for field in contact.get("custom_fields_values") or []:
        if field.get("field_code") != field_code:
            continue
        values = field.get("values") or []
        if not values:
            return ""
        value = values[0].get("value")
        return str(value) if value is not None else ""
    return ""


def _get_company_field(company: dict[str, Any], keys: list[str]) -> str:
    for key in keys:
        value = company.get(key)
        if value is None or value == "":
            continue
        return str(value)
    return ""


def _render_contract(template_path: str, context: dict[str, Any]) -> bytes:
    doc = DocxTemplate(template_path)
    doc.render(context)
    with io.BytesIO() as buffer:
        doc.save(buffer)
        return buffer.getvalue()


def _convert_docx_to_pdf_bytes(docx_bytes: bytes) -> bytes:
    with tempfile.TemporaryDirectory() as tmpdir:
        docx_path = os.path.join(tmpdir, "contract.docx")
        pdf_path = os.path.join(tmpdir, "contract.pdf")
        with open(docx_path, "wb") as docx_file:
            docx_file.write(docx_bytes)
        docx2pdf_convert(docx_path, pdf_path)
        with open(pdf_path, "rb") as pdf_file:
            return pdf_file.read()


async def run(order_id: int) -> dict:
    try:
        logger.info("agreement runner start order_id=%s", order_id)
        company_id, total_price = await amo.get_entity_data(order_id)
        lead = await amo.get_lead(order_id)
        company = await amo.get_company_data(company_id)

        custom_fields = lead.get("custom_fields_values") or []
        contract_num = str(order_id)
        contract_date = date.today().strftime("%d.%m.%Y")
        contract_name = f"Договор № {contract_num} от {contract_date}"

        order_date_raw = _get_custom_field_by_names(custom_fields, ["Дата"])
        order_date = _format_date(order_date_raw)
        order_address = _get_custom_field_by_names(custom_fields, ["Адрес"])
        order_start_time = _get_custom_field_by_names(custom_fields, ["Время начала"])
        order_hours = _get_custom_field_by_names(custom_fields, ["Количество часов"])
        order_finish_time = _calc_finish_time(order_start_time, order_hours)
        order_master_count = _get_custom_field_by_names(custom_fields,["Количество ведущих"])
        order_1_hour_price = ""
        if order_hours:
            try:
                order_1_hour_price = _format_money(float(total_price) / float(order_hours))
            except (TypeError, ValueError, ZeroDivisionError):
                order_1_hour_price = ""

        order_full_price = _format_money(total_price)
        order_full_price_text = _money_to_words_ru(total_price)

        contact_name = ""
        contact_phone = ""
        contact_email = ""
        embedded_contacts = lead.get("_embedded", {}).get("contacts") or []
        if embedded_contacts:
            contact_id = embedded_contacts[0].get("id")
            if contact_id:
                contact = await amo.get_contact(contact_id)
                contact_name = str(contact.get("name") or "")
                contact_phone = _get_contact_field(contact, "PHONE")
                contact_email = _get_contact_field(contact, "EMAIL")

        context = {
            "contract_num": contract_num,
            "contract_date": contract_date,
            "customer_name": str(company.get("name") or ""),
            "customer_admin": contact_name,
            "order_address": order_address,
            "order_date": order_date,
            "order_start_time": order_start_time,
            "order_finish_time": order_finish_time,
            "order_master_count": order_master_count,
            "order_1_hour_price": order_1_hour_price,
            "order_full_price": order_full_price,
            "order_full_price_text": order_full_price_text,
            "customer_adderss": str(company.get("address") or ""),
            "customer_inn": _get_company_field(company, ["vat_id"]),
            "customer_kpp": _get_company_field(company, ["tax_registration_reason_code", "kpp"]),
            "customer_ogrn": _get_company_field(company, ["ogrn", "ogrnip", "bin", "unp"]),
            "customer_phone": contact_phone,
            "customer_email": contact_email,
            "customer_invoice_num": _get_company_field(
                company, ["bank_account_number", "bank_account", "account_number", "payment_account"]
            ),
            "customer_bank_name": _get_company_field(company, ["bank_name"]),
            "customer_bik": _get_company_field(company, ["bank_code", "bank_bic", "bik"]),
            "customer_korinvoice_num": _get_company_field(
                company, ["bank_correspondent_account", "correspondent_account", "corr_account"]
            ),
        }

        bytes_docx = await anyio.to_thread.run_sync(_render_contract, TEMPLATE_PATH, context)
        bytes_pdf = await anyio.to_thread.run_sync(_convert_docx_to_pdf_bytes, bytes_docx)
        file_name = f"{contract_name}.pdf"
        uuid = await amo.add_file_in_crm(bytes_pdf, file_name)
        await amo.link_file_order(order_id=order_id, uuid=uuid)
        await amo.notify_manager(order_id, f"Договор сформирован: {file_name}")
        logger.info("agreement created order_id=%s", order_id)
        return {"status": "ok", "file_name": file_name}

    except amo.AmoDataError as e:
        logger.warning("agreement amo data error order_id=%s code=%s", order_id, e.code)
        detail = "Ошибка данных сделки"
        await amo.notify_manager(order_id, detail)
        return {"status": "error", "code": e.code, "detail": detail}

    except Exception as e:
        logger.exception("agreement runner error order_id=%s", order_id)
        detail = str(e.args[0]) if e.args else str(e)
        await amo.notify_manager(order_id, detail)
        return {"status": "error", "code": "EXCEPTION", "detail": detail}

if __name__ == "__main__":
    asyncio.run(run(37131731))
