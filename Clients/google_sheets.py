from datetime import date
import os
from typing import Iterable
import re

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

SERVICE_ACCOUNT_PATH = os.getenv(
    "SHEETS_SERVICE_ACCOUNT",
    "/etc/secrets/google_service_account.json",
)
SPREADSHEET_ID = os.getenv("SHEETS_SPREADSHEET_ID")


def _get_sheet_name(target_date: date | None = None) -> str:
    current = target_date or date.today()
    quarter = (current.month - 1) // 3 + 1
    year_short = current.year % 100
    return f"{quarter}Q'{year_short:02d}"


def _get_service():
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_PATH, scopes=SCOPES)
    return build("sheets", "v4", credentials=creds, cache_discovery=False)


def _ensure_sheet(service, sheet_name: str) -> tuple[int, bool]:
    spreadsheet = (
        service.spreadsheets()
        .get(spreadsheetId=SPREADSHEET_ID, fields="sheets.properties.title,sheets.properties.sheetId")
        .execute()
    )
    sheets = spreadsheet.get("sheets", [])
    existing = {s["properties"]["title"]: s["properties"]["sheetId"] for s in sheets}
    if sheet_name in existing:
        return existing[sheet_name], False
    body = {"requests": [{"addSheet": {"properties": {"title": sheet_name}}}]}
    response = service.spreadsheets().batchUpdate(
        spreadsheetId=SPREADSHEET_ID, body=body
    ).execute()
    return response["replies"][0]["addSheet"]["properties"]["sheetId"], True


def _get_sheet_id(service, sheet_name: str) -> int:
    spreadsheet = (
        service.spreadsheets()
        .get(spreadsheetId=SPREADSHEET_ID, fields="sheets.properties.title,sheets.properties.sheetId")
        .execute()
    )
    for sheet in spreadsheet.get("sheets", []):
        if sheet["properties"]["title"] == sheet_name:
            return sheet["properties"]["sheetId"]
    raise ValueError(f"Sheet {sheet_name} not found")


HEADERS = [
    "id",
    "Дата",
    "Цена",
    "Трансфер",
    "Город",
    "Тариф",
    "Время начала",
    "Часы",
    "Кол-во человек",
    "Примечание",
    "Контакт",
    "Ведущий",
    "Способ оплаты",
    "Реквизит",
    "Информация",
    "Ставка ведущего",
    "Прибыль",
]

def _extract_deal_id(value: str) -> str:
    if not value:
        return ""
    matches = re.findall(r"\d+", value)
    return matches[-1] if matches else ""


def upsert_deals(
    rows: Iterable[tuple[str, list[str]]],
    apply_format: bool = False,
    current_deal_ids: set[str] | None = None,
) -> dict[str, int]:
    if not SPREADSHEET_ID:
        raise ValueError("SHEETS_SPREADSHEET_ID is not set")

    service = _get_service()
    sheet_name = _get_sheet_name()
    sheet_id, created = _ensure_sheet(service, sheet_name)
    if sheet_id is None:
        sheet_id = _get_sheet_id(service, sheet_name)
        created = False

    values_resp = (
        service.spreadsheets()
        .values()
        .get(spreadsheetId=SPREADSHEET_ID, range=f"'{sheet_name}'!A:Q")
        .execute()
    )
    values = values_resp.get("values", [])

    if not values or not values[0] or values[0][0] != HEADERS[0]:
        service.spreadsheets().values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=f"'{sheet_name}'!A1:Q1",
            valueInputOption="RAW",
            body={"values": [HEADERS]},
        ).execute()

    existing_rows: dict[str, int] = {}
    for idx, row in enumerate(values, start=1):
        if idx == 1:
            continue
        if row and row[0]:
            deal_id = _extract_deal_id(str(row[0]))
            if deal_id:
                existing_rows[deal_id] = idx

    update_data = []
    append_values = []
    for deal_id, row_values in rows:
        if not row_values or not deal_id:
            continue
        row_index = existing_rows.get(deal_id)
        if row_index:
            update_data.append(
                {
                    "range": f"'{sheet_name}'!A{row_index}:Q{row_index}",
                    "values": [row_values],
                }
            )
        else:
            append_values.append(row_values)

    if update_data:
        body = {"valueInputOption": "USER_ENTERED", "data": update_data}
        service.spreadsheets().values().batchUpdate(
            spreadsheetId=SPREADSHEET_ID, body=body
        ).execute()

    if append_values:
        body = {"values": append_values}
        service.spreadsheets().values().append(
            spreadsheetId=SPREADSHEET_ID,
            range=f"'{sheet_name}'!A:Q",
            valueInputOption="USER_ENTERED",
            insertDataOption="INSERT_ROWS",
            body=body,
        ).execute()

    if current_deal_ids is None:
        current_deal_ids = set()

    missing_rows = [
        row_index
        for deal_id, row_index in existing_rows.items()
        if deal_id not in current_deal_ids
    ]

    if missing_rows:
        missing_updates = []
        for row_index in missing_rows:
            missing_updates.append(
                {
                    "range": f"'{sheet_name}'!B{row_index}:B{row_index}",
                    "values": [["?"]],
                }
            )
        service.spreadsheets().values().batchUpdate(
            spreadsheetId=SPREADSHEET_ID,
            body={"valueInputOption": "USER_ENTERED", "data": missing_updates},
        ).execute()

    updated = (
        service.spreadsheets()
        .values()
        .get(spreadsheetId=SPREADSHEET_ID, range=f"'{sheet_name}'!A:Q")
        .execute()
    )
    last_row = len(updated.get("values", []))
    if last_row < 2:
        return {"updated": len(update_data), "added": len(append_values)}

    format_requests = [
        {
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 0,
                    "endRowIndex": 1,
                    "startColumnIndex": 0,
                    "endColumnIndex": 17,
                },
                "cell": {
                    "userEnteredFormat": {
                        "textFormat": {"bold": True},
                        "backgroundColor": {"red": 0.776, "green": 0.878, "blue": 0.706},
                    }
                },
                "fields": "userEnteredFormat.textFormat.bold,userEnteredFormat.backgroundColor",
            }
        },
        {
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 1,
                    "startColumnIndex": 0,
                    "endColumnIndex": 17,
                },
                "cell": {"userEnteredFormat": {"textFormat": {"bold": False}}},
                "fields": "userEnteredFormat.textFormat.bold",
            }
        },
        {
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 0,
                    "startColumnIndex": 0,
                    "endColumnIndex": 17,
                },
                "cell": {"userEnteredFormat": {"horizontalAlignment": "CENTER"}},
                "fields": "userEnteredFormat.horizontalAlignment",
            }
        },
        {
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 1,
                    "startColumnIndex": 1,
                    "endColumnIndex": 2,
                },
                "cell": {
                    "userEnteredFormat": {
                        "numberFormat": {"type": "DATE", "pattern": "dd.MM.yyyy"}
                    }
                },
                "fields": "userEnteredFormat.numberFormat",
            }
        },
        {
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 1,
                    "startColumnIndex": 2,
                    "endColumnIndex": 3,
                },
                "cell": {
                    "userEnteredFormat": {
                        "numberFormat": {"type": "NUMBER", "pattern": "#,##0.00 ₽"}
                    }
                },
                "fields": "userEnteredFormat.numberFormat",
            }
        },
        {
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 1,
                    "startColumnIndex": 3,
                    "endColumnIndex": 4,
                },
                "cell": {
                    "userEnteredFormat": {
                        "numberFormat": {"type": "NUMBER", "pattern": "#,##0.00 ₽"}
                    }
                },
                "fields": "userEnteredFormat.numberFormat",
            }
        },
        {
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 1,
                    "startColumnIndex": 15,
                    "endColumnIndex": 16,
                },
                "cell": {
                    "userEnteredFormat": {
                        "numberFormat": {"type": "NUMBER", "pattern": "#,##0.00 ₽"}
                    }
                },
                "fields": "userEnteredFormat.numberFormat",
            }
        },
        {
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 1,
                    "startColumnIndex": 16,
                    "endColumnIndex": 17,
                },
                "cell": {
                    "userEnteredFormat": {
                        "numberFormat": {"type": "NUMBER", "pattern": "#,##0.00 ₽"}
                    }
                },
                "fields": "userEnteredFormat.numberFormat",
            }
        },
        {
            "setDataValidation": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 1,
                    "startColumnIndex": 13,
                    "endColumnIndex": 15,
                },
                "rule": {
                    "condition": {"type": "BOOLEAN"},
                    "showCustomUi": True,
                    "strict": True,
                },
            }
        },
        {
            "addConditionalFormatRule": {
                "rule": {
                    "ranges": [
                        {
                            "sheetId": sheet_id,
                            "startRowIndex": 1,
                            "startColumnIndex": 11,
                            "endColumnIndex": 12,
                        }
                    ],
                    "booleanRule": {
                        "condition": {
                            "type": "BLANK",
                        },
                        "format": {
                            "backgroundColor": {"red": 1.0, "green": 1.0, "blue": 0.686}
                        },
                    },
                },
                "index": 0,
            }
        },
    ]

    sort_request = {
        "sortRange": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": 1,
                "endRowIndex": last_row,
                "startColumnIndex": 0,
                "endColumnIndex": 17,
            },
            "sortSpecs": [{"dimensionIndex": 1, "sortOrder": "ASCENDING"}],
        }
    }

    requests = []
    if created or apply_format:
        requests.extend(format_requests)
    if missing_rows:
        for row_index in missing_rows:
            requests.append(
                {
                    "repeatCell": {
                        "range": {
                            "sheetId": sheet_id,
                            "startRowIndex": row_index - 1,
                            "endRowIndex": row_index,
                            "startColumnIndex": 1,
                            "endColumnIndex": 2,
                        },
                        "cell": {
                            "userEnteredFormat": {
                                "textFormat": {
                                    "bold": True,
                                    "foregroundColor": {"red": 1.0, "green": 0.0, "blue": 0.0},
                                }
                            }
                        },
                        "fields": "userEnteredFormat.textFormat",
                    }
                }
            )
    requests.append(sort_request)

    service.spreadsheets().batchUpdate(
        spreadsheetId=SPREADSHEET_ID, body={"requests": requests}
    ).execute()

    return {"updated": len(update_data), "added": len(append_values)}
