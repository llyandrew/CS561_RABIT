import duckdb
import time
import numpy as np
import pandas as pd

DB_FILE = "tpch.duckdb"
RUNS = 5

POINT_QUERY_CONFIGS = [
    {"name": "q_eq_20", "quantity_value": 20},
    {"name": "q_eq_24", "quantity_value": 24},
    {"name": "q_eq_30", "quantity_value": 30},
    {"name": "q_eq_40", "quantity_value": 40},
]


def load_data_once(con):

    df = con.execute("""
        SELECT l_extendedprice, l_discount, l_quantity
        FROM lineitem
    """).fetchdf()

    data = {
        "price": df["l_extendedprice"].to_numpy(),
        "discount": df["l_discount"].to_numpy(),
        "quantity": df["l_quantity"].to_numpy()
    }
    return data


def run_scan_point(con, quantity_value):

    sql = f"""
    SELECT SUM(l_extendedprice * l_discount)
    FROM lineitem
    WHERE l_quantity = {quantity_value}
    """

    start = time.time()
    result = con.execute(sql).fetchone()[0]
    end = time.time()

    return end - start, result


def run_bitmap_point_cached(data, quantity_value):

    price = data["price"]
    discount = data["discount"]
    quantity = data["quantity"]

    start = time.time()

    bitmap_quantity = (quantity == quantity_value)
    result = np.sum(price[bitmap_quantity] * discount[bitmap_quantity])

    end = time.time()

    return end - start, result


def benchmark_scan_point(con, config, runs=5):
    times = []
    results = []

    for _ in range(runs):
        t, r = run_scan_point(con, config["quantity_value"])
        times.append(t)
        results.append(r)

    return times, results


def benchmark_bitmap_point(data, config, runs=5):
    times = []
    results = []

    for _ in range(runs):
        t, r = run_bitmap_point_cached(data, config["quantity_value"])
        times.append(t)
        results.append(r)

    return times, results


def main():
    con = duckdb.connect(DB_FILE)

    print("Loading data once from DuckDB...")
    data = load_data_once(con)
    print("Data loaded.")

    rows = []

    for config in POINT_QUERY_CONFIGS:
        print(f"\nRunning point config: {config['name']}")

        scan_times, scan_results = benchmark_scan_point(con, config, runs=RUNS)
        bitmap_times, bitmap_results = benchmark_bitmap_point(data, config, runs=RUNS)

        print("  Scan times   :", scan_times)
        print("  Bitmap times :", bitmap_times)

        for i in range(RUNS):
            rows.append({
                "query_name": config["name"],
                "query_type": "point",
                "method": "scan",
                "quantity_value": config["quantity_value"],
                "run_id": i + 1,
                "time_sec": scan_times[i],
                "result": scan_results[i]
            })

            rows.append({
                "query_name": config["name"],
                "query_type": "point",
                "method": "bitmap",
                "quantity_value": config["quantity_value"],
                "run_id": i + 1,
                "time_sec": bitmap_times[i],
                "result": bitmap_results[i]
            })

    result_df = pd.DataFrame(rows)
    result_df.to_csv("point_results.csv", index=False)

    summary_df = (
        result_df.groupby(["query_name", "query_type", "method"], as_index=False)["time_sec"]
        .mean()
        .rename(columns={"time_sec": "avg_time_sec"})
    )
    summary_df.to_csv("point_summary_results.csv", index=False)

    print("\nSaved results to point_results.csv")
    print("Saved summary to point_summary_results.csv")
    print("\nAverage times:")
    print(summary_df)


if __name__ == "__main__":
    main()
