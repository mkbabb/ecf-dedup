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
    "% of Total Subsidies",
    "Contributions",
    "% of Total Contributions",
    "Delta",
]
DOLLAR_HEADER = [
    "High Cost",
    "Low Income",
    "Schools & Libraries",
    "Rural Healthcare",
    "Total Subsidies",
    "Contributions",
    "Delta",
]

SUBSIDIES_HEADER = [
    "High Cost",
    "Low Income",
    "Schools & Libraries",
    "Rural Healthcare",
]

HEADER_MARKER = "% of Total"

TABLE = "1.9"

YEARS_IN_THOUSANDS = [2022, 2021, 2018, 2017, 2016, 2015]


def dollar_to_float(value: Any) -> float:
    try:
        return float(value.replace("$", "").replace(",", ""))
    except:
        return 0.0


def find_first(df: pd.DataFrame, value: str) -> int | None:
    for ix, row in df.iterrows():
        if any(value.lower() in str(cell).lower() for cell in row):
            return ix
    return None


def process_dir(dir_path: pathlib.Path):
    dfs = []

    for file in dir_path.glob("**/*.xlsx"):
        if "Section 1" not in file.name or "~$" in file.name:
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

        df[DOLLAR_HEADER] = df[DOLLAR_HEADER].astype(float)

        if year in YEARS_IN_THOUSANDS:
            df[DOLLAR_HEADER] *= 1000.0
            if year == 2017:
                df[["High Cost", "Low Income"]] /= 1000.0
            if year == 2018:
                df["Schools & Libraries"] /= 1000.0

        df["Year"] = year
        df["State Import"] = df["Delta"] / df["Contributions"]

        df["Total Subsidies"] = df[SUBSIDIES_HEADER].sum(axis=1)

        df["% of Total Subsidies"] = df["Total Subsidies"] / df["Total Subsidies"].sum()

        df["% of Total Contributions"] = df["Contributions"] / df["Contributions"].sum()

        df["Delta"] = df["Total Subsidies"] - df["Contributions"]

        df["State Import"] = df["Delta"] / df["Contributions"]
        df["Net Retention"] = 1 + df["State Import"]

        # sort df by state:
        df = df.sort_values(["State"], ascending=[True]).reset_index(drop=True)

        # verify the first state is Alabama, and the last state is Wyoming:
        assert df["State"].iloc[0] == "Alabama"
        assert df["State"].iloc[-1] == "Wyoming"

        dfs.append(df)

    df = pd.concat(dfs).reset_index(drop=True)
    df = df.sort_values(["Year"], ascending=[False]).reset_index(drop=True)

    return df


def join_ecf(usf_df: pd.DataFrame, ecf_filepath: pathlib.Path):
    ecf_df = pd.read_csv(ecf_filepath)

    ecf_df = ecf_df[ecf_df["Funding Request Status"] == "Funded"]

    ecf_df = (
        ecf_df.groupby(["Billed Entity State"])
        .agg({"Line Total Cost": "sum"})
        .reset_index()
    )
    ecf_df["Year"] = 2022

    usf_df = usf_df.merge(
        ecf_df,
        how="left",
        left_on=["State Abbreviation", "Year"],
        right_on=["Billed Entity State", "Year"],
    )
    usf_df = usf_df.drop(columns=["Billed Entity State"])

    return usf_df


def join_acp(usf_df: pd.DataFrame, acp_filepath: pathlib.Path):
    acp_df = pd.read_csv(acp_filepath)

    # agg the acp_df by Data Month, which is Month-Year, by year, and then by State
    acp_df["Data Month"] = pd.to_datetime(acp_df["Data Month"], format="%b-%y")
    acp_df["Year"] = acp_df["Data Month"].dt.year

    acp_df[" Total Support "] = acp_df[" Total Support "].apply(dollar_to_float)
    acp_df = (
        acp_df.groupby(["Year", "State"]).agg({" Total Support ": "sum"}).reset_index()
    )

    # merge the acp_df with the usf_df on Year and State (which is an abbreviation)
    usf_df = usf_df.merge(
        acp_df,
        how="left",
        left_on=["Year", "State Abbreviation"],
        right_on=["Year", "State"],
    )

    usf_df = usf_df.drop(columns=["State_y"])
    usf_df = usf_df.rename(columns={"State_x": "State"})

    return usf_df


def join_state_names(usf_df: pd.DataFrame, us_states_names_path: pathlib.Path):

    us_states_names_df = pd.read_csv(us_states_names_path)
    us_states_names_df = us_states_names_df.rename(
        columns={"Name": "State Name", "Abbreviation": "State Abbreviation"}
    )
    return usf_df.merge(
        us_states_names_df, how="left", left_on="State", right_on="State Name"
    )


if __name__ == "__main__":
    creds = get_oauth2_creds(client_config=pathlib.Path("auth/creds.json"))
    sheets = Sheets(creds)

    dir_path = pathlib.Path("./data/usf")
    df = process_dir(dir_path=dir_path)

    us_states_names_path = pathlib.Path("data/us-states-names.csv")
    df = join_state_names(usf_df=df, us_states_names_path=us_states_names_path)

    ecf_filepath = pathlib.Path("data/ECF Deduped.csv")
    df = join_ecf(usf_df=df, ecf_filepath=ecf_filepath)

    acp_filepath = pathlib.Path(
        "data/ACP-Households-and-Claims-by-County-January-August-2022.xlsx - Sheet 1.csv"
    )
    df = join_acp(usf_df=df, acp_filepath=acp_filepath)

    sheets.clear(spreadsheet_id=SHEET_URL, range_name="Sheet1")
    sheets.update(
        spreadsheet_id=SHEET_URL, range_name="Sheet1", values=sheets.from_frame(df)
    )
    sheets.resize_columns(spreadsheet_id=SHEET_URL, sheet_name="Sheet1", width=120)
