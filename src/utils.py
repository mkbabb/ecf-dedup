import datetime
import hashlib
import os
import pathlib
from typing import Literal, Optional

import pandas as pd
import requests

OUT_DIR = "./data/"


def merge_n_drop(
    *args,
    how: str = "inner",
    validate: Optional[str] = None,
    keep_dup_cols: Literal["left", "right", "both"] = "left",
    ensure_m1: bool = True,
    **kwargs,
):
    """'Better' way to merge two dataframes - at least more intuitive.
    If the keep_dup_cols value is not 'both', this function will ensure
    that there are no duplicated columns in the resultant merged dataframe.

    For example:

        df1.columns = ["join_col",         "hey", "ok"]
        df2.columns = ["another_join_col", "wow", "ok"]

        merged = pd.merge(
            df1,
            df2,
            left_on="join_col",
            right_on="another_join_col",
            how="left",
        )

    Pandas' default functionality will yield these columns:

        merged.columns = ["join_col", "another_join_col", "hey", "wow", "ok_x", "ok_y"]

    Typically, one of the two joining columns is superfluous.
    Further, any exactly duplicated columns are also likely superfluous (the "ok" column need not be included twice).

    merge_n_drop will instead, depending on the value of keep_dup_cols, only keep overlapping columns from one dataframe.
    In the above example, if we instead:

        merged = merge_n_drop(
            df1,
            df2,
            left_on="join_col",
            right_on="another_join_col",
            how="left",
            keep_dup_cols="left"
        )

    Will yield:

        merged.columns = ["join_col", "hey", "wow", "ok"]

    Thus minimizing the number of overlapping and logically duplicated columns.

    ensure_m1 does exactly what is says: ensures that there's a many-to-one
    relationship when executing the final join.
    """
    left, right, *args = args

    if validate is None and ensure_m1:
        validate = (
            "m:1"
            if (how == "left" or how == "inner")
            else "1:m"
            if how == "right"
            else ""
        )

    def _merge():
        return pd.merge(left, right, *args, how=how, validate=validate, **kwargs)

    if keep_dup_cols == "both":
        return _merge()
    else:
        left_columns, right_columns = set(left.columns), set(right.columns)

        to_set = lambda x: {x} if isinstance(x, str) else set(x)

        suffixes = kwargs.get("suffixes", ("_x", "_y"))
        join_cols = to_set(kwargs.get(f"{keep_dup_cols}_on", kwargs.get("on", {})))

        same = left_columns.intersection(right_columns)

        same_n_join_cols = same.intersection(join_cols)
        join_cols = join_cols.difference(same)

        same = same.difference(same_n_join_cols)

        drop_suffix = None

        # We only keep overlapping columns
        # from one explicitly specified dataframe,
        # optimizing the join
        if keep_dup_cols == "left":
            rename_suffix, drop_suffix = suffixes
            cols = [i for i in right.columns if i not in same]
            right = right[cols]
        elif keep_dup_cols == "right":
            drop_suffix, rename_suffix = suffixes
            cols = [i for i in left.columns if i not in same]
            left = left[cols]

        # If a column is a join column AND and overlapping column we can't
        # filter it in the above; we're going to end up with a duplicated set of columns.
        # We rename, and thus keep, the columns in the keep_dup_cols dataframe
        # and drop the columns in the opposing keep_dup_cols dataframe.
        join_cols |= {f"{i}{drop_suffix}" for i in same_n_join_cols}
        rename_mapper = {f"{i}{rename_suffix}": i for i in same_n_join_cols}

        merged = _merge()

        merged.drop(join_cols, axis=1, inplace=True)
        merged.rename(rename_mapper, axis=1, inplace=True)

        return merged


def make_hashed_filename(s: str, out_dir: str):
    h = hashlib.new("sha256")
    h.update(s.encode())
    return pathlib.Path(out_dir).joinpath(h.hexdigest())


def GET_if_not_exists(
    url: str,
    filepath: Optional[str] = None,
    out_dir: Optional[str] = OUT_DIR,
    days_until_stale: Optional[int] = None,
) -> tuple[pathlib.Path, bool]:
    """Automatically downloads a bytes file from some URL.
    If it's already been downloaded within days_until_stale,
    we use that version instead.

    Return the output path and whether or not the file's been downloaded."""
    if filepath is None:
        filepath = make_hashed_filename(s=url, out_dir=out_dir)

    filepath = pathlib.Path(filepath)

    download = not filepath.exists()

    if not download and days_until_stale is not None:
        modified_time = os.path.getmtime(filepath)
        delta = datetime.datetime.today() - datetime.datetime.fromtimestamp(
            modified_time
        )
        download = delta.days > days_until_stale

    if download:
        r = requests.get(url)
        with open(filepath, "wb") as file:
            file.write(r.content)

    return filepath, download
