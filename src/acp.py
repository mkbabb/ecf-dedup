import pandas as pd


claims_columns = "Data Month,State,State Name,County Name,State FIPS,County FIPS,Total Claimed Subscribers,Total Claimed Devices,Service Support,Device Support,Total Support".split(
    ","
)
households_columns = "Data Month,State,State Name,County Name,State FIPS,County FIPS,Net New Enrollments Alternative Verification Process,Net New Enrollments Verified by School,Net New Enrollments Lifeline,Net New Enrollments National Verifier Application,Net New Enrollments total,Total Alternative Verification Process,Total Verified by School,Total Lifeline,Total National Verifier Application,Total Subscribers".split(
    ","
)

# Normalize ACP data to include households and counties in one file

# 2023 data's header is formatted differently


# claims_path = "data/acp/ACP-Claims-by-County-January-August-2023.csv"
# households_path = "data/acp/ACP-Households-by-County-January-August-2023.csv"

claims_path = "data/acp/ACP-Claims-by-County-January-December-2022.csv"
households_path = "data/acp/ACP-Households-by-County-January-December-2022.csv"

claims_df = pd.read_csv(claims_path)
households_df = pd.read_csv(households_path)

claims_df.columns = claims_columns
households_df.columns = households_columns


on = "Data Month,State,State Name,County Name,State FIPS,County FIPS".split(",")

df = pd.merge(claims_df, households_df, left_on=on, right_on=on, how="inner")

df.to_csv(
    "data/acp/ACP-Claims-and-Households-by-County-January-December-2022.csv",
    index=False,
)

# stack years of normalized data

df_2022 = pd.read_csv(
    "data/acp/ACP-Claims-and-Households-by-County-January-December-2022.csv"
)
df_2023 = pd.read_csv(
    "data/acp/ACP-Claims-and-Households-by-County-January-August-2023.csv"
)


df = pd.concat([df_2022, df_2023])

df.to_csv("data/acp/ACP-Claims-and-Households-by-County.csv", index=False)
