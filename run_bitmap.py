import duckdb
import time
import numpy as np
import pandas as pd
import os

DB_FILE = "tpch.duckdb"
RUNS = 5

# 4 組 selectivity
QUERY_CONFIGS = [
    {"name": "very_narrow", "discount_low": 0.05, "discount_high": 0.051, "quantity_threshold": 24},
    {"name": "narrow",      "discount_low": 0.05, "discount_high": 0.055, "quantity_threshold": 24},
    {"name": "medium",      "discount_low": 0.05, "discount_high": 0.07,  "quantity_threshold": 24},
    {"name": "wide",        "discount_low": 0.01, "discount_high": 0.09,  "quantity_threshold": 24},
]


def load_data_once(con):
    """
    只在程式一開始載入一次資料
    """
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


def run_scan(con, discount_low, discount_high, quantity_threshold):
    """
    直接讓 DuckDB 跑 SQL scan
    """
    sql = f"""
    SELECT SUM(l_extendedprice * l_discount)
    FROM lineitem
    WHERE l_discount BETWEEN {discount_low} AND {discount_high}
      AND l_quantity < {quantity_threshold}
    """

    start = time.time()
    result = con.execute(sql).fetchone()[0]
    end = time.time()

    return end - start, result


def run_bitmap_cached(data, discount_low, discount_high, quantity_threshold):
    """
    不再從 DuckDB 抓資料
    直接用已經載入好的 numpy array 做 bitmap/filter
    """
    price = data["price"]
    discount = data["discount"]
    quantity = data["quantity"]

    start = time.time()

    bitmap_discount = (discount >= discount_low) & (discount <= discount_high)
    bitmap_quantity = (quantity < quantity_threshold)
    final_bitmap = bitmap_discount & bitmap_quantity

    result = np.sum(price[final_bitmap] * discount[final_bitmap])

    end = time.time()

    return end - start, result


def benchmark_scan(con, config, runs=5):
    times = []
    results = []

    for _ in range(runs):
        t, r = run_scan(
            con,
            config["discount_low"],
            config["discount_high"],
            config["quantity_threshold"]
        )
        times.append(t)
        results.append(r)

    return times, results


def benchmark_bitmap(data, config, runs=5):
    times = []
    results = []

    for _ in range(runs):
        t, r = run_bitmap_cached(
            data,
            config["discount_low"],
            config["discount_high"],
            config["quantity_threshold"]
        )
        times.append(t)
        results.append(r)

    return times, results


def main():
    con = duckdb.connect(DB_FILE)

    # 只載入一次
    print("Loading data once from DuckDB...")
    data = load_data_once(con)
    print("Data loaded.")

    rows = []

    for config in QUERY_CONFIGS:
        print(f"\nRunning config: {config['name']}")

        scan_times, scan_results = benchmark_scan(con, config, runs=RUNS)
        bitmap_times, bitmap_results = benchmark_bitmap(data, config, runs=RUNS)

        print("  Scan times   :", scan_times)
        print("  Bitmap times :", bitmap_times)

        for i in range(RUNS):
            rows.append({
                "query_name": config["name"],
                "method": "scan",
                "discount_low": config["discount_low"],
                "discount_high": config["discount_high"],
                "quantity_threshold": config["quantity_threshold"],
                "run_id": i + 1,
                "time_sec": scan_times[i],
                "result": scan_results[i]
            })

            rows.append({
                "query_name": config["name"],
                "method": "bitmap",
                "discount_low": config["discount_low"],
                "discount_high": config["discount_high"],
                "quantity_threshold": config["quantity_threshold"],
                "run_id": i + 1,
                "time_sec": bitmap_times[i],
                "result": bitmap_results[i]
            })

    result_df = pd.DataFrame(rows)
    result_df.to_csv("results.csv", index=False)

    summary_df = (
        result_df.groupby(["query_name", "method"], as_index=False)["time_sec"]
        .mean()
        .rename(columns={"time_sec": "avg_time_sec"})
    )
    summary_df.to_csv("summary_results.csv", index=False)

    print("\nSaved results to results.csv")
    print("Saved summary to summary_results.csv")
    print("\nAverage times:")
    print(summary_df)


if __name__ == "__main__":
    main()