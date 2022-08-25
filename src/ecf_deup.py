from typing import Optional

import geopandas as gpd
import pandas as pd

from src.utils import GET_if_not_exists, merge_n_drop

ECF_URL = "https://opendata.usac.org/api/views/i5j4-3rvr/rows.csv?accessType=DOWNLOAD"
ERATE_SUPP_URL = (
    "https://opendata.usac.org/api/views/7i5i-83qf/rows.csv?accessType=DOWNLOAD"
)
SCHOOL_DISTRICTS_URL = (
    "https://nces.ed.gov/programs/edge/data/EDGESCHOOLDISTRICT_TL21_SY2021.zip"
)


def get_ecf_data(ecf_filepath: Optional[str] = None):
    ecf_filepath, _ = GET_if_not_exists(
        url=ECF_URL, filepath=ecf_filepath, days_until_stale=7, suffix=".csv"
    )
    return pd.read_csv(ecf_filepath)


def get_supp_data(supp_path: Optional[str] = None):
    supp_path, downloaded = GET_if_not_exists(
        url=ERATE_SUPP_URL, filepath=supp_path, days_until_stale=7, suffix=".csv"
    )
    supp_df = pd.read_csv(supp_path)

    if not downloaded:
        return supp_df

    sum_children_cols = "Total Number of Full-Time Students	Total Number of Part-Time Students	Peak Number of Part-Time Students	Number of NSLP Students".split(
        "	"
    )

    parent_ixs = supp_df["Parent Entity Number"].isnull()
    children = supp_df[~parent_ixs]

    # Sums together all columns within sum_children_cols, grouped by parents
    sum_children = (
        children.groupby("Parent Entity Number")[sum_children_cols].sum().reset_index()
    )

    # And then maps it back to the parents.
    supp_df = merge_n_drop(
        sum_children,
        supp_df,
        left_on="Parent Entity Number",
        right_on="Entity Number",
        how="right",
        dup_cols_to_keep="left",
    )

    supp_df = (
        supp_df.groupby("Entity Number", as_index=False).last().reset_index(drop=True)
    )

    supp_df.to_csv(supp_path, index=False)
    return supp_df


def get_school_districts_data(school_districts_path: Optional[str] = None):
    school_districts_path, _ = GET_if_not_exists(
        url=SCHOOL_DISTRICTS_URL, filepath=school_districts_path, suffix=".zip"
    )
    # File extension must exist, so we add a .zip.
    gdf = gpd.read_file(school_districts_path)
    return gdf


def map_bens(ecf_df: pd.DataFrame, supp_df: pd.DataFrame):
    ecf_df = merge_n_drop(
        ecf_df,
        supp_df,
        left_on="Billed Entity Number (BEN)",
        right_on="Entity Number",
        how="left",
    )

    return ecf_df


def spatial_join(df: pd.DataFrame, school_districts_gdf: gpd.GeoDataFrame):
    """Most reliable method to map a school-like entity to a given district;
    100% of the supplemental E-rate data contain coordinates for the included schools."""
    gdf = gpd.GeoDataFrame(
        df, geometry=gpd.points_from_xy(df.Longitude, df.Latitude), crs="EPSG:4269"
    )
    gdf = gdf.sjoin(school_districts_gdf, how="left")
    df = pd.DataFrame(gdf.drop(columns="geometry"))
    return df


def dedeup_frns(ecf_df: pd.DataFrame):
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


def process_ecf_data(
    ecf_df: pd.DataFrame,
    supp_path: Optional[str] = None,
    school_districts_path: Optional[str] = None,
    out_filepath: str = f"data/ECF Deduped.csv",
):
    supp_df = get_supp_data(supp_path=supp_path)
    ecf_df = map_bens(ecf_df, supp_df=supp_df)

    school_districts_gdf = get_school_districts_data(school_districts_path)
    ecf_df = spatial_join(ecf_df, school_districts_gdf=school_districts_gdf)

    ecf_df = dedeup_frns(ecf_df)

    ecf_df.to_csv(out_filepath, index=False)


if __name__ == "__main__":
    ecf_df = get_ecf_data()
    process_ecf_data(ecf_df=ecf_df)
