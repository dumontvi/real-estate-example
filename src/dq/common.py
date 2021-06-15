import pandas as pd


def null_check(df):
    return bool(df.isna()), df.isna().sum()  ## return is null bool, num of null per column


def nonzero_count_check(df):
    return len(df.index) > 0  # Check if count > 0


def unique_check(df, col):
    """
    Check if col is unique
    """
