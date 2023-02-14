import pandas as pd

claims_path = "data/ACP-Claims-by-County-January-December-2022.csv"
households_path = "data/ACP-Households-by-County-January-December-2022.csv"

claims_df = pd.read_csv(claims_path)
households_df = pd.read_csv(households_path)

on = "Data Month,State,State Name,County Name,State FIPS,County FIPS".split(",")

df = pd.merge(claims_df, households_df, left_on=on, right_on=on, how="inner")

df.to_csv(
    "data/ACP-Claims-and-Households-by-County-January-December-2022.csv", index=False
)
