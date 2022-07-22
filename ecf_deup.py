import pandas as pd


def map_bens(usac_df: pd.DataFrame, state: str = "NC"):
    svf_filepath = "data/2022 SVF from 2021 SVF from Oct 2019 Claim Data - SVF2022.csv"
    svf_df = pd.read_csv(svf_filepath)
    svf_df = svf_df.groupby(["District Entity Number"]).first().reset_index()
    svf_df["LEA Number"] = svf_df["State School ID"].str.slice(0, 3)

    regions_path = (
        "data/LEA-to-Region-Mapping-May2022 - LEA-to-Region-Mapping-May2022.csv"
    )
    regions_df = pd.read_csv(regions_path)
    regions_df["LEA No."] = regions_df["LEA No."].str.zfill(3)

    usac_df = usac_df[usac_df["Billed Entity State"] == state]
    usac_df = usac_df[
        usac_df["Billed Entity Number (BEN)"].isin(svf_df["District Entity Number"])
    ]

    usac_df = pd.merge(
        usac_df,
        svf_df[["District Entity Number", "State School ID", "LEA Number"]],
        left_on="Billed Entity Number (BEN)",
        right_on="District Entity Number",
        how="inner",
    )

    usac_df = pd.merge(
        usac_df,
        regions_df[["LEA No.", "SBE Region", "SBE Region Names"]],
        how="left",
        left_on="LEA Number",
        right_on="LEA No.",
    ).drop(["LEA No."], axis=1)

    return usac_df


def dedup(usac_df: pd.DataFrame):
    frns = usac_df.groupby(["Funding Request Number (FRN)", "FRN Line Item ID"])

    drop_ixs = []

    for frn, group_df in frns:
        statuses = group_df["Funding Request Status"]
        statuses_list = list(statuses)

        if "Pending" in statuses_list and len(statuses_list) > 1:
            pending_ixs = statuses[statuses == "Pending"].index
            drop_ixs.extend(pending_ixs)

    usac_df = usac_df.drop(drop_ixs, axis=0)

    n = 0

    def split_firms(x: str):
        nonlocal n
        n += 1

        if pd.isna(x):
            return x, x

        items = x.split("},")

        names, numbers = [], []

        for i in items:
            i = i.replace("{", "").replace("}", "")
            name, number = i.split("|")

            names.append(name)
            numbers.append(number)

        return ", ".join(names), ", ".join(numbers)

    (
        usac_df["Consulting Firm Names"],
        usac_df["Consulting Firm Numbers"],
    ) = zip(*usac_df["Consulting Firm"].apply(split_firms, 1))

    return usac_df


usac_filepath = "data/ECF_NC_Only.csv"

usac_df = pd.read_csv(usac_filepath)

usac_df = map_bens(usac_df)

usac_df = dedup(usac_df)

usac_df.to_csv("data/ECF Dedup.csv", index=False)
