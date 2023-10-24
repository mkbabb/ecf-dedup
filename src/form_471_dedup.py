import pathlib

import pandas as pd

BEN = "Billed Entity Number"
FUNDING_YEAR = "Funding Year"
CAT1_DISCOUNT = "Category One Discount Rate"

path = pathlib.Path("data/E-Rate_Request_for_Discount_on_Services__Basic_Information__FCC_Form_471_and_Related_Information__20231023.csv")

app_df = pd.read_csv(path)

key = [BEN, FUNDING_YEAR, CAT1_DISCOUNT]

bens = app_df.sort_values(key, ascending=[True, False, False]).drop_duplicates(
    BEN, keep="first"
)

bens.to_csv(path.with_stem(f"{path.stem} - Deduped"), index=False)
