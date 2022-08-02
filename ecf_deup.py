import pathlib
from datetime import datetime
from typing import Optional

import geopandas as gpd
import pandas as pd
import requests


def current_date():
    return datetime.today().strftime("%Y-%m-%d")


def svf_join(ecf_df: pd.DataFrame):
    svf_filepath = "data/2022 SVF from 2021 SVF from Oct 2019 Claim Data - SVF2022.csv"
    svf_df = pd.read_csv(svf_filepath)
    svf_df = svf_df.groupby(["District Entity Number"]).first().reset_index()
    svf_df["LEA Number"] = svf_df["State School ID"].str.slice(0, 3)

    regions_path = (
        "data/LEA-to-Region-Mapping-May2022 - LEA-to-Region-Mapping-May2022.csv"
    )
    regions_df = pd.read_csv(regions_path)
    regions_df["LEA No."] = regions_df["LEA No."].str.zfill(3)

    ecf_df = ecf_df[ecf_df["Billed Entity State"] == "NC"]
    ecf_df = ecf_df[
        ecf_df["Billed Entity Number (BEN)"].isin(svf_df["District Entity Number"])
    ]

    ecf_df = pd.merge(
        ecf_df,
        svf_df[["District Entity Number", "State School ID", "LEA Number"]],
        left_on="Billed Entity Number (BEN)",
        right_on="District Entity Number",
        how="inner",
    )

    ecf_df = pd.merge(
        ecf_df,
        regions_df[["LEA No.", "SBE Region", "SBE Region Names"]],
        how="left",
        left_on="LEA Number",
        right_on="LEA No.",
    ).drop(["LEA No."], axis=1)

    return ecf_df


def map_bens(ecf_df: pd.DataFrame):
    supp_path = "data/E-Rate_Supplemental_Entity_Information.csv"
    supp_df = pd.read_csv(supp_path)

    # no_lea_ixs = supp_df["State Local Education Agency (LEA) Code"].isnull()
    # lea = supp_df[~no_lea_ixs]

    # def map_pen(pen: str):
    #     if not pd.isna(pen):
    #         row = lea[lea["Entity Number"] == pen]
    #         if len(row) > 0:
    #             return row["State Local Education Agency (LEA) Code"].iloc[0]
    #     return pd.NA

    # supp_df.loc[no_lea_ixs, "State Local Education Agency (LEA) Code"] = supp_df.loc[
    #     no_lea_ixs, "Parent Entity Number"
    # ].apply(map_pen)

    supp_df = (
        supp_df.groupby("Entity Number", as_index=False).last().reset_index(drop=True)
    )

    ecf_df = pd.merge(
        ecf_df,
        supp_df,
        left_on="Billed Entity Number (BEN)",
        right_on="Entity Number",
        how="left",
    )

    return ecf_df


def spatial_join(df: pd.DataFrame):
    gdf = gpd.GeoDataFrame(
        df, geometry=gpd.points_from_xy(df.Longitude, df.Latitude), crs="EPSG:4269"
    )

    school_districts_path = "~/Documents/My Tableau Repository/Datasources/EDGESCHOOLDISTRICT_TL21_SY2021/schooldistrict_sy2021_tl21.shp"
    school_districts_gdf = gpd.read_file(school_districts_path)

    gdf = gdf.sjoin(school_districts_gdf, how="left")

    df = pd.DataFrame(gdf.drop(columns="geometry"))

    return gdf


def dedup(ecf_df: pd.DataFrame):
    frns = ecf_df.groupby(["Funding Request Number (FRN)", "FRN Line Item ID"])

    drop_ixs = []

    for frn, group_df in frns:
        statuses = group_df["Funding Request Status"]
        statuses_list = list(statuses)

        if "Pending" in statuses_list and len(statuses_list) > 1:
            pending_ixs = statuses[statuses == "Pending"].index
            drop_ixs.extend(pending_ixs)

    ecf_df = ecf_df.drop(drop_ixs, axis=0)

    def split_firms(x: str):
        items = x.split("},")

        names, numbers = [], []

        for i in items:
            i = i.replace("{", "").replace("}", "")
            name, number = i.split("|")

            names.append(name)
            numbers.append(number)

        return ", ".join(names), ", ".join(numbers)

    firm_ixs = ~ecf_df["Consulting Firm"].isnull()
    firms = ecf_df.loc[firm_ixs, "Consulting Firm"]

    (
        ecf_df.loc[firm_ixs, "Consulting Firm Names"],
        ecf_df.loc[firm_ixs, "Consulting Firm Numbers"],
    ) = zip(*firms.apply(split_firms))

    return ecf_df


def get_ecf_data():
    url = "https://opendata.usac.org/api/views/i5j4-3rvr/rows.csv?accessType=DOWNLOAD"
    r = requests.get(url)

    ecf_filepath = f"data/ECF Data - {current_date()}.csv"
    ecf_filepath = pathlib.Path(ecf_filepath)

    if not ecf_filepath.exists():
        with open(ecf_filepath, "wb") as file:
            file.write(r.content)

    return ecf_filepath


def main(ecf_filepath: Optional[str] = None):
    if ecf_filepath is None:
        ecf_filepath = get_ecf_data()

    ecf_filepath = pathlib.Path(ecf_filepath)
    out_filepath = ecf_filepath.with_name(f"{ecf_filepath.stem} - Deduped.csv")

    ecf_df = pd.read_csv(ecf_filepath)

    ecf_df = map_bens(ecf_df)
    ecf_df = spatial_join(ecf_df)
    # NC only
    ecf_df = svf_join(ecf_df)
    ecf_df = dedup(ecf_df)

    ecf_df.to_csv(out_filepath, index=False)


if __name__ == "__main__":
    # ecf_filepath = "data/ECF_NC_Only.csv"
    ecf_filepath = "data/Emergency_Connectivity_Fund_FCC_Form_471 - July 30 - USA.csv"

    main()
