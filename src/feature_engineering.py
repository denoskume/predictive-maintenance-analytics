import pandas as pd

def build_features(df: pd.DataFrame) -> pd.DataFrame:
    """"Create core time + risk features (baseline industry).""""
    df = df.copy()

    df["hour"] = df["timestamp"].dt.hour
    df["day_of_week"] = df["timestamp"].dt.dayofweek

    # Simple thresholds (baseline). Later replace with quantiles / per-type thresholds.
    df["high_vibration"] = (df["vibration_rms"] > df["vibration_rms"].median()).astype(int)
    df["high_temperature"] = (df["temperature_motor"] > df["temperature_motor"].median()).astype(int)

    return df
