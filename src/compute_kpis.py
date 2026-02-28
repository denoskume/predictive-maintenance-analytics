import pandas as pd

def failure_rate_by_machine(df: pd.DataFrame) -> pd.Series:
    return df.groupby("machine_id")["failure_within_24h"].mean().sort_values(ascending=False)

def failure_rate_by_type(df: pd.DataFrame) -> pd.Series:
    return df.groupby("machine_type")["failure_within_24h"].mean().sort_values(ascending=False)

def avg_repair_cost_by_machine(df: pd.DataFrame) -> pd.Series:
    return df.groupby("machine_id")["estimated_repair_cost"].mean().sort_values(ascending=False)
