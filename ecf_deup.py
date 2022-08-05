import pathlib
from datetime import datetime
from typing import Literal, Optional

import geopandas as gpd
import pandas as pd
import requests


def current_date():
    return datetime.today().strftime("%Y-%m-%d")


def merge_n_drop(*args, keep_cols: Literal["left", "right", "both"] = "left", **kwargs):
    to_set = lambda x: {x} if isinstance(x, str) else set(x)

    suffixes = kwargs.get("suffixes", ("_x", "_y"))

    left, right, *args = args
    left_columns, right_columns = set(left.columns), set(right.columns)

    merged = pd.merge(left, right, *args, **kwargs)

    if keep_cols != "both":
        dropped = to_set(kwargs.get(f"{keep_cols}_on", kwargs.get("on", {})))

        same = left_columns.intersection(right_columns)

        same_dropped = same.intersection(dropped)
        dropped = dropped.difference(same)

        same = same.difference(same_dropped)

        rename_cols = left_columns.intersection(same)
        rename_suffix, drop_suffix = suffixes

        if keep_cols == "right":
            drop_suffix, rename_suffix = suffixes
            rename_cols = right_columns.intersection(same)

        rename_mapper = {f"{i}{rename_suffix}": i for i in rename_cols}
        dropped |= {f"{i}{drop_suffix}" for i in same_dropped}

        merged.drop(dropped, axis=1, inplace=True)
        merged.rename(rename_mapper, inplace=True)

    return merged


def map_bens(ecf_df: pd.DataFrame, supp_path: str):
    children_cols = "Total Number of Full-Time Students	Total Number of Part-Time Students	Peak Number of Part-Time Students	Number of NSLP Students".split(
        "	"
    )

    supp_df = pd.read_csv(supp_path)

    # Mapping the parent entity's LEA number, if it exists, back to all of its children.
    parent_ixs = supp_df["Parent Entity Number"].isnull()
    children = supp_df[~parent_ixs]

    hey = children.groupby("Parent Entity Number")[children_cols].sum().reset_index()

    supp_df = merge_n_drop(
        hey,
        supp_df,
        left_on="Parent Entity Number",
        right_on="Entity Number",
        how="right",
    )

    supp_df = (
        supp_df.groupby("Entity Number", as_index=False).last().reset_index(drop=True)
    )

    ecf_df = merge_n_drop(
        ecf_df,
        supp_df,
        left_on="Billed Entity Number (BEN)",
        right_on="Entity Number",
        how="left",
    )

    nc = supp_df[supp_df["Physical State"] == "NC"]

    print(nc[nc["Entity Number"] == 17000198]["Total Number of Full-Time Students"])

    heyy = supp_df["Total Number of Full-Time Students"].sum()

    return ecf_df


def spatial_join(df: pd.DataFrame, school_districts_path: str):
    """Most reliable method to map a school-like entity to a given district;
    100% of the supplemental E-rate data contain coordinates for the included schools."""
    gdf = gpd.GeoDataFrame(
        df, geometry=gpd.points_from_xy(df.Longitude, df.Latitude), crs="EPSG:4269"
    )
    school_districts_gdf = gpd.read_file(school_districts_path)

    gdf = gdf.sjoin(school_districts_gdf, how="left")

    df = pd.DataFrame(gdf.drop(columns="geometry"))

    return gdf


def dedup(ecf_df: pd.DataFrame):
    """Removes duplicated rows based on each pair of (FRN, FRN Line Item).
    If an entry contains a status of 'Pending' in addition to any other status,
    the 'Pending' entry is removed; this row is duplicated in regards to
    calculating the Line Total Cost."""
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


def get_ecf_data(ecf_filepath: Optional[str] = None):
    """Automatically downloads the latest ECF data.
    If it's already been downloaded for the day, we use that version instead."""

    if ecf_filepath is None:
        ecf_filepath = f"data/ECF Data - {current_date()}.csv"
        ecf_filepath = pathlib.Path(ecf_filepath)

        if not ecf_filepath.exists():
            url = "https://opendata.usac.org/api/views/i5j4-3rvr/rows.csv?accessType=DOWNLOAD"
            r = requests.get(url)
            with open(ecf_filepath, "wb") as file:
                file.write(r.content)

    return pd.read_csv(ecf_filepath)


def process_ecf_data(
    ecf_df: pd.DataFrame,
    supp_path: str,
    school_districts_path: str,
    out_filepath: str = f"data/ECF Deduped - {current_date()}.csv",
):
    ecf_df = map_bens(ecf_df, supp_path=supp_path)
    ecf_df = spatial_join(ecf_df, school_districts_path=school_districts_path)
    ecf_df = dedup(ecf_df)

    ecf_df.to_csv(out_filepath, index=False)


if __name__ == "__main__":
    supp_path = "data/E-Rate_Supplemental_Entity_Information.csv"
    school_districts_path = "~/Documents/My Tableau Repository/Datasources/EDGESCHOOLDISTRICT_TL21_SY2021/schooldistrict_sy2021_tl21.shp"

    ecf_df = get_ecf_data()

    process_ecf_data(
        ecf_df=ecf_df, supp_path=supp_path, school_districts_path=school_districts_path
    )
