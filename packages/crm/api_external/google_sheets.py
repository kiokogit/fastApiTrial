import logging
from collections.abc import Sequence

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

import util

logger = logging.getLogger()

# If modifying these scopes, delete the file token.json.
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# The ID and range of a sample spreadsheet.
SPREADSHEET_ID = "12CptU9SaNlihqr0PRrvdMqdy7KVKlhgTDwohHErMzEI"


def get_column_key(n) -> str:
    a, b = n // 26, n % 26
    if n == 0:
        return "A"
    if n == 1:
        return "A"

    if n <= 26:
        return chr(ord("A") + b - 1)
    else:
        return chr(ord("A") + a) + chr(ord("A") + b)


def get_creds():
    credentials = service_account.Credentials.from_service_account_file(
        str(util.project_root() / "local/credentials.json")
    )

    return credentials.with_scopes(SCOPES)


creds = get_creds()
service = build("sheets", "v4", credentials=creds)


def clear_spreadsheet(sheet_id, clear_header, n_cols):
    creds = get_creds()

    service = build("sheets", "v4", credentials=creds)

    start = 1 if clear_header else 2
    # clear sheet by inserting empty values
    values = [[""] * 10000] * n_cols
    body = {"values": values, "majorDimension": "COLUMNS"}
    range = f"Sheet1!A{start}:{get_column_key(n_cols)}{len(values[0])}"

    result = (
        service.spreadsheets()
        .values()
        .update(spreadsheetId=sheet_id, range=range, valueInputOption="RAW", body=body)
        .execute()
    )
    logger.debug("{} cells updated.".format(result.get("updatedCells")))


def write_columns_range(spreadsheet_id, data_by_columns: dict | list, col_offset=0, row_offset=0):
    creds = get_creds()
    service = build("sheets", "v4", credentials=creds)

    clear_spreadsheet(spreadsheet_id, clear_header=True, n_cols=len(data_by_columns))

    start_col = ord("A") + col_offset
    col_values = data_by_columns.values() if isinstance(data_by_columns, dict) else data_by_columns
    end_row = max(len(v) for v in col_values) + 1 + row_offset

    range_name = f"Sheet1!{chr(start_col)}{1 + row_offset}:{get_column_key(len(data_by_columns))}{end_row}"

    values = (
        [[key] + vals for key, vals in data_by_columns.items()]
        if isinstance(data_by_columns, dict)
        else data_by_columns
    )
    body = {"values": values, "majorDimension": "COLUMNS"}

    result = (
        service.spreadsheets()
        .values()
        .update(spreadsheetId=spreadsheet_id, range=range_name, valueInputOption="RAW", body=body)
        .execute()
    )
    logger.debug("{} cells updated.".format(result.get("updatedCells")))


def read_columns_range(spreadsheet_id, range: str, axis):
    """
    Creates the batch_update the user has access to.
    Load pre-authorized user credentials from the environment.
    TODO(developer) - See https://developers.google.com/identity
    for guides on implementing OAuth2 for the application.\n"
    """
    if not axis in ("ROWS", "COLUMNS"):
        raise ValueError("invalid majorDimension parameter supplied")

    try:
        result = (
            service.spreadsheets()
            .values()
            .get(spreadsheetId=spreadsheet_id, range=range, majorDimension=axis)
            .execute()
        )
        rows = result.get("values", [])
        print(f"{len(rows)} rows retrieved")
        return result
    except HttpError as error:
        print(f"An error occurred: {error}")
        return error


def write_column(sheet_id, col: chr, inp_values: Sequence["str"], keep_header=True):
    creds = get_creds()

    try:
        if len(inp_values) == 0:
            return

        service = build("sheets", "v4", credentials=creds)

        clear_spreadsheet(sheet_id, clear_header=not keep_header, n_cols=10)

        start = 2 if keep_header else 1
        values = [list(inp_values)]
        body = {"values": values, "majorDimension": "COLUMNS"}
        range = f"Sheet1!{col}{start}:{get_column_key(n_cols)}{len(values[0])+1}"

        result = (
            service.spreadsheets()
            .values()
            .update(spreadsheetId=sheet_id, range=range, valueInputOption="RAW", body=body)
            .execute()
        )
        logger.debug("{} cells updated.".format(result.get("updatedCells")))

    except HttpError as err:
        print(err)


if __name__ == "__main__":
    print(get_column_key(0), get_column_key(1), get_column_key(26), get_column_key(27), get_column_key(28))
    # columns = {
    #     'url': ['project1', 'project2', 'project3'],
    #     'grade': ['A', 'B']
    # }
    #
    # sheet_id = '1SZeIPZCeHU3zLQEnqcDmG0QEtBYifrviq0iKnYhiQc4'
    # write_columns_range(sheet_id, range_name=range, data_by_columns=columns)
