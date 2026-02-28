import pandas as pd

def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """"Basic cleaning: drop duplicates, keep simple NA handling with flags.""""
    df = df.copy()
    df = df.drop_duplicates()

    # Add missing flags (industry practice)
    for col in df.columns:
        if df[col].isna().any():
            df[f"{col}_is_missing"] = df[col].isna().astype(int)

    # Minimal NA handling (keep rows, fill numeric with median)
    numeric_cols = df.select_dtypes(include=["number"]).columns
    for col in numeric_cols:
        if df[col].isna().any():
            df[col] = df[col].fillna(df[col].median())

    # Fill remaining object NAs
    object_cols = df.select_dtypes(include=["object"]).columns
    for col in object_cols:
        if df[col].isna().any():
            df[col] = df[col].fillna("UNKNOWN")

    return df
