import pandas as pd

def load_data(path: str = "data/raw/predictive_maintenance.csv") -> pd.DataFrame:
    """"Load raw predictive maintenance dataset (CSV).""""
    return pd.read_csv(path, parse_dates=["timestamp"])
