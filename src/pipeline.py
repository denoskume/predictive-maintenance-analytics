from src.load_data import load_data
from src.clean_data import clean_data
from src.feature_engineering import build_features
from src.compute_kpis import failure_rate_by_machine, failure_rate_by_type, avg_repair_cost_by_machine

def main():
    df = load_data()
    df = clean_data(df)
    df = build_features(df)

    print("Rows:", len(df))
    print("Machines:", df["machine_id"].nunique())

    print("\nTop failure rate (machines):")
    print(failure_rate_by_machine(df).head(10))

    print("\nFailure rate (types):")
    print(failure_rate_by_type(df))

    print("\nTop avg repair cost (machines):")
    print(avg_repair_cost_by_machine(df).head(10))

    df.to_csv("data/processed/processed_data.csv", index=False)
    print("\nSaved: data/processed/processed_data.csv")

if __name__ == "__main__":
    main()
