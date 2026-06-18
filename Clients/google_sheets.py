from datetime import date
import os
from typing import Iterable
import re

import threading

from dotenv import load_dotenv
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

load_dotenv()

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

SERVICE_ACCOUNT_PATH = os.getenv(
    "SHEETS_SERVICE_ACCOUNT",
    "/etc/secrets/google_service_account.json",
)
SPREADSHEET_ID = os.getenv("SHEETS_SPREADSHEET_ID")
MANAGERS_SPREADSHEET_ID = os.getenv("SHEETS_MANAGERS_SPREADSHEET_ID")

_service_cache = None
_service_lock = threading.Lock()

MANAGER_HEADERS = [
    "id", "Дата", "Тариф", "Город", "Адрес",
    "Время начала", "Кол-во часов", "Кол-во чел.", "Формат", "Примечание",
    "Способ оплаты", "Контакт", "Ведущий", "Реквизит", "Информация",
]


def _get_sheet_name(target_date: date | None = None) -> str:
    return "Тестовый лист"


def _get_service():
    global _service_cache
    if _service_cache is None:
        with _service_lock:
            if _service_cache is None:
                creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_PATH, scopes=SCOPES)
                _service_cache = build("sheets", "v4", credentials=creds, cache_discovery=False)
    return _service_cache


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
    return _get_sheet_id_in(service, SPREADSHEET_ID, sheet_name)


def _get_sheet_id_in(service, spreadsheet_id: str, sheet_name: str) -> int:
    spreadsheet = (
        service.spreadsheets()
        .get(spreadsheetId=spreadsheet_id, fields="sheets.properties.title,sheets.properties.sheetId")
        .execute()
    )
    for sheet in spreadsheet.get("sheets", []):
        if sheet["properties"]["title"] == sheet_name:
            return sheet["properties"]["sheetId"]
    raise ValueError(f"Sheet {sheet_name} not found in {spreadsheet_id}")


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


def reset_deals_color(deal_ids: list[str]) -> int:
    if not deal_ids or not SPREADSHEET_ID:
        return 0

    service = _get_service()
    sheet_name = _get_sheet_name()

    try:
        sheet_id = _get_sheet_id(service, sheet_name)
    except ValueError:
        return 0

    values_resp = (
        service.spreadsheets()
        .values()
        .get(spreadsheetId=SPREADSHEET_ID, range=f"'{sheet_name}'!A:A")
        .execute()
    )
    values = values_resp.get("values", [])

    id_set = set(deal_ids)
    row_indices = []
    for idx, row in enumerate(values, start=1):
        if idx == 1:
            continue
        if row and row[0]:
            deal_id = _extract_deal_id(str(row[0]))
            if deal_id in id_set:
                row_indices.append(idx)

    if not row_indices:
        return 0

    requests = [
        {
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": row_index - 1,
                    "endRowIndex": row_index,
                    "startColumnIndex": 0,
                    "endColumnIndex": 17,
                },
                "cell": {
                    "userEnteredFormat": {
                        "backgroundColor": {"red": 1.0, "green": 1.0, "blue": 1.0}
                    }
                },
                "fields": "userEnteredFormat.backgroundColor",
            }
        }
        for row_index in row_indices
    ]

    service.spreadsheets().batchUpdate(
        spreadsheetId=SPREADSHEET_ID, body={"requests": requests}
    ).execute()

    return len(row_indices)


def mark_deals_red(deal_ids: list[str]) -> int:
    if not deal_ids or not SPREADSHEET_ID:
        return 0

    service = _get_service()
    sheet_name = _get_sheet_name()

    try:
        sheet_id = _get_sheet_id(service, sheet_name)
    except ValueError:
        return 0

    values_resp = (
        service.spreadsheets()
        .values()
        .get(spreadsheetId=SPREADSHEET_ID, range=f"'{sheet_name}'!A:A")
        .execute()
    )
    values = values_resp.get("values", [])

    id_set = set(deal_ids)
    row_indices = []
    for idx, row in enumerate(values, start=1):
        if idx == 1:
            continue
        if row and row[0]:
            deal_id = _extract_deal_id(str(row[0]))
            if deal_id in id_set:
                row_indices.append(idx)

    if not row_indices:
        return 0

    requests = [
        {
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": row_index - 1,
                    "endRowIndex": row_index,
                    "startColumnIndex": 0,
                    "endColumnIndex": 17,
                },
                "cell": {
                    "userEnteredFormat": {
                        "backgroundColor": {"red": 1.0, "green": 0.4, "blue": 0.4}
                    }
                },
                "fields": "userEnteredFormat.backgroundColor",
            }
        }
        for row_index in row_indices
    ]

    service.spreadsheets().batchUpdate(
        spreadsheetId=SPREADSHEET_ID, body={"requests": requests}
    ).execute()

    return len(row_indices)



_COL_LETTERS = list("ABCDEFGHIJKLMNOPQRSTUVWXYZ")


def _col(idx: int) -> str:
    return _COL_LETTERS[idx]


def _rule(sheet_id: int, col: int, formula: str, color: dict) -> dict:
    return {
        "addConditionalFormatRule": {
            "rule": {
                "ranges": [{"sheetId": sheet_id, "startRowIndex": 1, "startColumnIndex": col, "endColumnIndex": col + 1}],
                "booleanRule": {
                    "condition": {
                        "type": "CUSTOM_FORMULA",
                        "values": [{"userEnteredValue": formula}],
                    },
                    "format": {"backgroundColor": color},
                },
            },
            "index": 0,
        }
    }


_YELLOW = {"red": 1.0, "green": 0.898, "blue": 0.6}
_RED = {"red": 0.918, "green": 0.6, "blue": 0.6}


def _yellow_blank_rules(sheet_id: int, col_indices: list[int], end_row: int) -> list[dict]:
    return [
        _rule(sheet_id, col, f"=(LEN({_col(col)}2)=0)*(LEN($A2)>0)", _YELLOW)
        for col in col_indices
    ]


def _red_blank_rules(sheet_id: int, col_indices: list[int], end_row: int) -> list[dict]:
    return [
        _rule(sheet_id, col, f"=(LEN({_col(col)}2)=0)*(LEN($A2)>0)", _RED)
        for col in col_indices
    ]


def _red_false_rules(sheet_id: int, col_indices: list[int], end_row: int) -> list[dict]:
    return [
        _rule(sheet_id, col, f"=({_col(col)}2=FALSE)*(LEN($A2)>0)", _RED)
        for col in col_indices
    ]


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

    col_a = (
        service.spreadsheets()
        .values()
        .get(spreadsheetId=SPREADSHEET_ID, range=f"'{sheet_name}'!A:A")
        .execute()
    ).get("values", [])

    if not col_a or not col_a[0] or col_a[0][0] != HEADERS[0]:
        service.spreadsheets().values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=f"'{sheet_name}'!A1:Q1",
            valueInputOption="RAW",
            body={"values": [HEADERS]},
        ).execute()
        if not col_a:
            col_a = [[HEADERS[0]]]

    existing_rows: dict[str, int] = {}
    for idx, row in enumerate(col_a, start=1):
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

    missing_rows = (
        [
            row_index
            for deal_id, row_index in existing_rows.items()
            if deal_id not in current_deal_ids
        ]
        if current_deal_ids is not None
        else []
    )

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

    last_row = len(col_a) + len(append_values)
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
            "updateDimensionProperties": {
                "range": {"sheetId": sheet_id, "dimension": "ROWS", "startIndex": 0},
                "properties": {"pixelSize": 30},
                "fields": "pixelSize",
            }
        },
        {
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 1,
                    "startColumnIndex": 9,
                    "endColumnIndex": 10,
                },
                "cell": {"userEnteredFormat": {"horizontalAlignment": "LEFT"}},
                "fields": "userEnteredFormat.horizontalAlignment",
            }
        },
        *[
            {
                "updateDimensionProperties": {
                    "range": {"sheetId": sheet_id, "dimension": "COLUMNS", "startIndex": col, "endIndex": col + 1},
                    "properties": {"pixelSize": px},
                    "fields": "pixelSize",
                }
            }
            for col, px in [(4, 180), (5, 235), (10, 150), (12, 130), (15, 130)]
        ],
        *_yellow_blank_rules(sheet_id, [1, 2, 4, 5, 6, 7, 8, 10, 12, 15], last_row),
        *_red_blank_rules(sheet_id, [11], last_row),
        *_red_false_rules(sheet_id, [13, 14], last_row),
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

    if created or apply_format:
        sheet_data = service.spreadsheets().get(
            spreadsheetId=SPREADSHEET_ID,
            fields="sheets(properties.sheetId,conditionalFormats)",
        ).execute()
        delete_requests = []
        for s in sheet_data.get("sheets", []):
            if s["properties"]["sheetId"] == sheet_id:
                n = len(s.get("conditionalFormats", []))
                for i in range(n - 1, -1, -1):
                    delete_requests.append({"deleteConditionalFormatRule": {"sheetId": sheet_id, "index": i}})
                break
        if delete_requests:
            service.spreadsheets().batchUpdate(
                spreadsheetId=SPREADSHEET_ID, body={"requests": delete_requests}
            ).execute()

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


# ── Managers table ────────────────────────────────────────────────────────────

def _mgr_range(sheet_id: int, start_col: int, end_col: int, start_row: int = 0, end_row: int | None = None) -> dict:
    r = {"sheetId": sheet_id, "startRowIndex": start_row, "startColumnIndex": start_col, "endColumnIndex": end_col}
    if end_row is not None:
        r["endRowIndex"] = end_row
    return r


def mark_manager_deals_red(deal_ids: list[str]) -> int:
    if not deal_ids or not MANAGERS_SPREADSHEET_ID:
        return 0
    service = _get_service()
    sheet_name = _get_sheet_name()
    try:
        sheet_id = _get_sheet_id_in(service, MANAGERS_SPREADSHEET_ID, sheet_name)
    except ValueError:
        return 0
    values = service.spreadsheets().values().get(spreadsheetId=MANAGERS_SPREADSHEET_ID, range=f"'{sheet_name}'!A:A").execute().get("values", [])
    id_set = set(deal_ids)
    row_indices = [idx for idx, row in enumerate(values, 1) if idx > 1 and row and _extract_deal_id(str(row[0])) in id_set]
    if not row_indices:
        return 0
    requests = [{"repeatCell": {"range": _mgr_range(sheet_id, 0, 15, ri - 1, ri), "cell": {"userEnteredFormat": {"backgroundColor": {"red": 1.0, "green": 0.4, "blue": 0.4}}}, "fields": "userEnteredFormat.backgroundColor"}} for ri in row_indices]
    service.spreadsheets().batchUpdate(spreadsheetId=MANAGERS_SPREADSHEET_ID, body={"requests": requests}).execute()
    return len(row_indices)


def reset_manager_deals_color(deal_ids: list[str]) -> int:
    if not deal_ids or not MANAGERS_SPREADSHEET_ID:
        return 0
    service = _get_service()
    sheet_name = _get_sheet_name()
    try:
        sheet_id = _get_sheet_id_in(service, MANAGERS_SPREADSHEET_ID, sheet_name)
    except ValueError:
        return 0
    values = service.spreadsheets().values().get(spreadsheetId=MANAGERS_SPREADSHEET_ID, range=f"'{sheet_name}'!A:A").execute().get("values", [])
    id_set = set(deal_ids)
    row_indices = [idx for idx, row in enumerate(values, 1) if idx > 1 and row and _extract_deal_id(str(row[0])) in id_set]
    if not row_indices:
        return 0
    requests = [{"repeatCell": {"range": _mgr_range(sheet_id, 0, 15, ri - 1, ri), "cell": {"userEnteredFormat": {"backgroundColor": {"red": 1.0, "green": 1.0, "blue": 1.0}}}, "fields": "userEnteredFormat.backgroundColor"}} for ri in row_indices]
    service.spreadsheets().batchUpdate(spreadsheetId=MANAGERS_SPREADSHEET_ID, body={"requests": requests}).execute()
    return len(row_indices)


def extend_manager_checkboxes(last_row: int) -> None:
    if not MANAGERS_SPREADSHEET_ID or last_row < 2:
        return
    service = _get_service()
    sheet_name = _get_sheet_name()
    try:
        sheet_id = _get_sheet_id(service, sheet_name)
    except ValueError:
        return
    service.spreadsheets().batchUpdate(
        spreadsheetId=MANAGERS_SPREADSHEET_ID,
        body={"requests": [{"setDataValidation": {"range": _mgr_range(sheet_id, 13, 15, 1, last_row), "rule": {"condition": {"type": "BOOLEAN"}, "showCustomUi": True, "strict": True}}}]},
    ).execute()


def _ensure_sheet_in(service, spreadsheet_id: str, sheet_name: str) -> tuple[int, bool]:
    spreadsheet = service.spreadsheets().get(spreadsheetId=spreadsheet_id, fields="sheets.properties.title,sheets.properties.sheetId").execute()
    sheets = spreadsheet.get("sheets", [])
    existing = {s["properties"]["title"]: s["properties"]["sheetId"] for s in sheets}
    if sheet_name in existing:
        return existing[sheet_name], False
    body = {"requests": [{"addSheet": {"properties": {"title": sheet_name}}}]}
    response = service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()
    return response["replies"][0]["addSheet"]["properties"]["sheetId"], True


def upsert_manager_deals(
    rows: Iterable[tuple[str, list[str]]],
    apply_format: bool = False,
    current_deal_ids: set[str] | None = None,
) -> dict[str, int]:
    if not MANAGERS_SPREADSHEET_ID:
        raise ValueError("SHEETS_MANAGERS_SPREADSHEET_ID is not set")

    service = _get_service()
    sheet_name = _get_sheet_name()
    sheet_id, created = _ensure_sheet_in(service, MANAGERS_SPREADSHEET_ID, sheet_name)

    ncols = len(MANAGER_HEADERS)
    col_range = chr(ord("A") + ncols - 1)

    col_a = service.spreadsheets().values().get(
        spreadsheetId=MANAGERS_SPREADSHEET_ID, range=f"'{sheet_name}'!A:A"
    ).execute().get("values", [])

    if not col_a or not col_a[0] or col_a[0][0] != MANAGER_HEADERS[0]:
        service.spreadsheets().values().update(
            spreadsheetId=MANAGERS_SPREADSHEET_ID,
            range=f"'{sheet_name}'!A1:{col_range}1",
            valueInputOption="RAW",
            body={"values": [MANAGER_HEADERS]},
        ).execute()
        if not col_a:
            col_a = [[MANAGER_HEADERS[0]]]

    existing_rows: dict[str, int] = {}
    for idx, row in enumerate(col_a, start=1):
        if idx == 1:
            continue
        if row and row[0]:
            did = _extract_deal_id(str(row[0]))
            if did:
                existing_rows[did] = idx

    rows = list(rows)
    update_data, append_values = [], []
    for deal_id, row_values in rows:
        if not row_values or not deal_id:
            continue
        ri = existing_rows.get(deal_id)
        if ri:
            update_data.append({"range": f"'{sheet_name}'!A{ri}:{col_range}{ri}", "values": [row_values]})
        else:
            append_values.append(row_values)

    if update_data:
        service.spreadsheets().values().batchUpdate(spreadsheetId=MANAGERS_SPREADSHEET_ID, body={"valueInputOption": "USER_ENTERED", "data": update_data}).execute()
    if append_values:
        service.spreadsheets().values().append(spreadsheetId=MANAGERS_SPREADSHEET_ID, range=f"'{sheet_name}'!A:{col_range}", valueInputOption="USER_ENTERED", insertDataOption="INSERT_ROWS", body={"values": append_values}).execute()

    missing_rows = ([ri for did, ri in existing_rows.items() if did not in current_deal_ids] if current_deal_ids is not None else [])
    if missing_rows:
        service.spreadsheets().values().batchUpdate(
            spreadsheetId=MANAGERS_SPREADSHEET_ID,
            body={"valueInputOption": "USER_ENTERED", "data": [{"range": f"'{sheet_name}'!B{ri}:B{ri}", "values": [["?"]]} for ri in missing_rows]},
        ).execute()

    last_row = len(col_a) + len(append_values)

    if last_row < 2:
        return {"updated": len(update_data), "added": len(append_values)}

    if created or apply_format:
        sheet_data = service.spreadsheets().get(spreadsheetId=MANAGERS_SPREADSHEET_ID, fields="sheets(properties.sheetId,conditionalFormats)").execute()
        delete_reqs = []
        for s in sheet_data.get("sheets", []):
            if s["properties"]["sheetId"] == sheet_id:
                n = len(s.get("conditionalFormats", []))
                delete_reqs = [{"deleteConditionalFormatRule": {"sheetId": sheet_id, "index": i}} for i in range(n - 1, -1, -1)]
                break
        if delete_reqs:
            service.spreadsheets().batchUpdate(spreadsheetId=MANAGERS_SPREADSHEET_ID, body={"requests": delete_reqs}).execute()

    format_reqs = [
        {"repeatCell": {"range": _mgr_range(sheet_id, 0, ncols, 0, 1), "cell": {"userEnteredFormat": {"textFormat": {"bold": True}, "backgroundColor": {"red": 0.776, "green": 0.878, "blue": 0.706}}}, "fields": "userEnteredFormat.textFormat.bold,userEnteredFormat.backgroundColor"}},
        {"repeatCell": {"range": _mgr_range(sheet_id, 0, ncols, 1), "cell": {"userEnteredFormat": {"textFormat": {"bold": False}}}, "fields": "userEnteredFormat.textFormat.bold"}},
        {"repeatCell": {"range": _mgr_range(sheet_id, 0, ncols), "cell": {"userEnteredFormat": {"horizontalAlignment": "CENTER"}}, "fields": "userEnteredFormat.horizontalAlignment"}},
        {"repeatCell": {"range": _mgr_range(sheet_id, 4, 5, 1), "cell": {"userEnteredFormat": {"horizontalAlignment": "LEFT"}}, "fields": "userEnteredFormat.horizontalAlignment"}},
        {"repeatCell": {"range": _mgr_range(sheet_id, 9, 10, 1), "cell": {"userEnteredFormat": {"horizontalAlignment": "LEFT"}}, "fields": "userEnteredFormat.horizontalAlignment"}},
        {"repeatCell": {"range": _mgr_range(sheet_id, 1, 2, 1), "cell": {"userEnteredFormat": {"numberFormat": {"type": "DATE", "pattern": "dd.MM.yyyy"}}}, "fields": "userEnteredFormat.numberFormat"}},
        {"updateDimensionProperties": {"range": {"sheetId": sheet_id, "dimension": "ROWS", "startIndex": 0}, "properties": {"pixelSize": 30}, "fields": "pixelSize"}},
        *[{"updateDimensionProperties": {"range": {"sheetId": sheet_id, "dimension": "COLUMNS", "startIndex": col, "endIndex": col + 1}, "properties": {"pixelSize": px}, "fields": "pixelSize"}} for col, px in [(2, 150), (4, 200), (9, 220), (11, 150), (12, 150)]],
        {"setDataValidation": {"range": _mgr_range(sheet_id, 13, 15, 1, last_row), "rule": {"condition": {"type": "BOOLEAN"}, "showCustomUi": True, "strict": True}}},
        *_yellow_blank_rules(sheet_id, [1, 2, 3, 4, 5, 6, 7, 10, 11], last_row),
        *_red_blank_rules(sheet_id, [12], last_row),
        *_red_false_rules(sheet_id, [13, 14], last_row),
    ]

    sort_req = {"sortRange": {"range": _mgr_range(sheet_id, 0, ncols, 1, last_row), "sortSpecs": [{"dimensionIndex": 1, "sortOrder": "ASCENDING"}]}}

    requests = []
    if created or apply_format:
        requests.extend(format_reqs)
    if missing_rows:
        requests.extend([{"repeatCell": {"range": _mgr_range(sheet_id, 1, 2, ri - 1, ri), "cell": {"userEnteredFormat": {"textFormat": {"bold": True, "foregroundColor": {"red": 1.0, "green": 0.0, "blue": 0.0}}}}, "fields": "userEnteredFormat.textFormat"}} for ri in missing_rows])
    requests.append(sort_req)

    service.spreadsheets().batchUpdate(spreadsheetId=MANAGERS_SPREADSHEET_ID, body={"requests": requests}).execute()
    return {"updated": len(update_data), "added": len(append_values)}
