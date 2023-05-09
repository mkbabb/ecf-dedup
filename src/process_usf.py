import pathlib
from typing import Any
import pandas as pd
from googleapiutils2 import Sheets, get_oauth2_creds

SHEET_URL = "https://docs.google.com/spreadsheets/d/1F8GNe4VwSc8kuFE0mdGGtZfiFpLcMG26LNUTlkOHTH8/edit#gid=0"

HEADER = [
    "State",
    "High Cost",
    "Low Income",
    "Schools & Libraries",
    "Rural Healthcare",
    "Total Subsidies",
    "% of Total",
    "Contributions",
    "% of Total",
    "Delta",
]

HEADER_MARKER = "% of Total"

TABLE = "1.9"


def find_first(df: pd.DataFrame, value: str) -> int | None:
    for ix, row in df.iterrows():
        if any(value in str(cell) for cell in row):
            return ix
    return None


def process_dir(dir_path: pathlib.Path):
    dfs = []

    for file in dir_path.glob("**/*.xlsx"):
        if "Section 1" not in file.name:
            continue
        year = int(file.parent.name)

        print(f"Processing {file}")

        sheet_name = None
        for t_sheet_name in pd.ExcelFile(file).sheet_names:
            if TABLE in t_sheet_name:
                sheet_name = t_sheet_name
                break
        else:
            print(f"Could not find sheet {TABLE} in {file.name}")
            continue

        df = pd.read_excel(file, sheet_name=sheet_name, header=None)

        # remove empty rows:
        df = df.replace(r"^\s*$", pd.NA, regex=True)

        # remove normalize header:
        header_ix = find_first(df, HEADER_MARKER)
        df = df.iloc[header_ix + 1 :].reset_index(drop=True)

        # remove empty bottom rows after total:
        total_ix = find_first(df, "Total")
        df = df.iloc[:total_ix].reset_index(drop=True)
        df = df.dropna(axis=1, how="all")

        df = df.iloc[:, : len(HEADER)]
        df.columns = HEADER

        df["Year"] = year
        df["State Import"] = df["Delta"] / df["Contributions"]

        # sort df by state:
        df = df.sort_values(["State"], ascending=[True]).reset_index(drop=True)

        # verify the first state is Alabama, and the last state is Wyoming:
        assert df["State"].iloc[0] == "Alabama"
        assert df["State"].iloc[-1] == "Wyoming"

        dfs.append(df)

    df = pd.concat(dfs).reset_index(drop=True)
    df = df.sort_values(["Year"], ascending=[False]).reset_index(drop=True)

    return df


if __name__ == "__main__":
    creds = get_oauth2_creds(client_config=pathlib.Path("auth/creds.json"))
    sheets = Sheets(creds)

    dir_path = pathlib.Path("./data/usf")
    df = process_dir(dir_path=dir_path)

    filepath = pathlib.Path("data/USF Data.csv")

    df.to_csv(filepath, index=False)

    sheets.update(
        spreadsheet_id=SHEET_URL, range_name="Sheet1", values=sheets.from_frame(df)
    )
