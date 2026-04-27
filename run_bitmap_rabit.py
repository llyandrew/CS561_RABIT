import duckdb
import time
import numpy as np
import pandas as pd

DB_FILE = "workloads.duckdb"
RUNS = 3

#queries_layout_selectivity_explain.sql

WORKLOADS = [

    # random
    {"query_name": "random_0_1_pct", "table": "t_layout_random", "low": 100000, "high": 100999},
    {"query_name": "random_1_pct",   "table": "t_layout_random", "low": 100000, "high": 109999},
    {"query_name": "random_5_pct",   "table": "t_layout_random", "low": 100000, "high": 149999},
    {"query_name": "random_10_pct",  "table": "t_layout_random", "low": 100000, "high": 199999},
    {"query_name": "random_20_pct",  "table": "t_layout_random", "low": 100000, "high": 299999},
    {"query_name": "random_30_pct",  "table": "t_layout_random", "low": 100000, "high": 399999},
    {"query_name": "random_40_pct",  "table": "t_layout_random", "low": 100000, "high": 499999},
    {"query_name": "random_50_pct",  "table": "t_layout_random", "low": 100000, "high": 599999},
    {"query_name": "random_60_pct",  "table": "t_layout_random", "low": 100000, "high": 699999},
    {"query_name": "random_70_pct",  "table": "t_layout_random", "low": 100000, "high": 799999},
    {"query_name": "random_80_pct",  "table": "t_layout_random", "low": 100000, "high": 899999},
    {"query_name": "random_90_pct",  "table": "t_layout_random", "low": 0,      "high": 899999},

    # sorted
    {"query_name": "sorted_0_1_pct", "table": "t_layout_sorted", "low": 100000, "high": 100999},
    {"query_name": "sorted_1_pct",   "table": "t_layout_sorted", "low": 100000, "high": 109999},
    {"query_name": "sorted_5_pct",   "table": "t_layout_sorted", "low": 100000, "high": 149999},
    {"query_name": "sorted_10_pct",  "table": "t_layout_sorted", "low": 100000, "high": 199999},
    {"query_name": "sorted_20_pct",  "table": "t_layout_sorted", "low": 100000, "high": 299999},
    {"query_name": "sorted_30_pct",  "table": "t_layout_sorted", "low": 100000, "high": 399999},
    {"query_name": "sorted_40_pct",  "table": "t_layout_sorted", "low": 100000, "high": 499999},
    {"query_name": "sorted_50_pct",  "table": "t_layout_sorted", "low": 100000, "high": 599999},
    {"query_name": "sorted_60_pct",  "table": "t_layout_sorted", "low": 100000, "high": 699999},
    {"query_name": "sorted_70_pct",  "table": "t_layout_sorted", "low": 100000, "high": 799999},
    {"query_name": "sorted_80_pct",  "table": "t_layout_sorted", "low": 100000, "high": 899999},
    {"query_name": "sorted_90_pct",  "table": "t_layout_sorted", "low": 0,      "high": 899999},

    # block-sorted
    {"query_name": "block_sorted_0_1_pct", "table": "t_layout_block_sorted", "low": 100000, "high": 100999},
    {"query_name": "block_sorted_1_pct",   "table": "t_layout_block_sorted", "low": 100000, "high": 109999},
    {"query_name": "block_sorted_5_pct",   "table": "t_layout_block_sorted", "low": 100000, "high": 149999},
    {"query_name": "block_sorted_10_pct",  "table": "t_layout_block_sorted", "low": 100000, "high": 199999},
    {"query_name": "block_sorted_20_pct",  "table": "t_layout_block_sorted", "low": 100000, "high": 299999},
    {"query_name": "block_sorted_30_pct",  "table": "t_layout_block_sorted", "low": 100000, "high": 399999},
    {"query_name": "block_sorted_40_pct",  "table": "t_layout_block_sorted", "low": 100000, "high": 499999},
    {"query_name": "block_sorted_50_pct",  "table": "t_layout_block_sorted", "low": 100000, "high": 599999},
    {"query_name": "block_sorted_60_pct",  "table": "t_layout_block_sorted", "low": 100000, "high": 699999},
    {"query_name": "block_sorted_70_pct",  "table": "t_layout_block_sorted", "low": 100000, "high": 799999},
    {"query_name": "block_sorted_80_pct",  "table": "t_layout_block_sorted", "low": 100000, "high": 899999},
    {"query_name": "block_sorted_90_pct",  "table": "t_layout_block_sorted", "low": 0,      "high": 899999},
]


def load_table_once(con, table_name: str):
    arrs = con.execute(f"""
        SELECT a
        FROM {table_name}
    """).fetchnumpy()

    return {
        "a": arrs["a"]
    }


def run_scan(con, table_name: str, low: int, high: int):
    sql = f"""
    SELECT count(*)
    FROM {table_name}
    WHERE a BETWEEN {low} AND {high}
    """

    start = time.time()
    result = con.execute(sql).fetchone()[0]
    end = time.time()

    return end - start, result


def run_bitmap_cached(data, low: int, high: int):
    a = data["a"]

    start = time.time()

    bitmap = (a >= low) & (a <= high)
    result = int(np.sum(bitmap))

    end = time.time()
    return end - start, result


def build_rabit_index_for_workload(a_values, workloads):
    needed_thresholds = set()

    for q in workloads:
        low = q["low"]
        high = q["high"]

        needed_thresholds.add(high)
        if low > 0:
            needed_thresholds.add(low - 1)

    needed_thresholds = sorted(needed_thresholds)

    prefix_bitmaps = {}
    for t in needed_thresholds:
        prefix_bitmaps[t] = (a_values <= t)

    return {
        "thresholds": needed_thresholds,
        "prefix_bitmaps": prefix_bitmaps
    }


#unique_vals = [10, 20, 30, 40]
#target = 25 then get back 30
def find_ge(unique_vals, target):
    idx = np.searchsorted(unique_vals, target, side="left")
    if idx >= len(unique_vals):
        return unique_vals[-1]
    return unique_vals[idx]

#target = 25 then get back 20
def find_lt(unique_vals, target):
    idx = np.searchsorted(unique_vals, target, side="left") - 1
    if idx < 0:
        return None
    return unique_vals[idx]


def rabit_range_bitmap(rabit_index, low, high):
    prefix_bitmaps = rabit_index["prefix_bitmaps"]
    #it store like: a <= 100999 a <= 109999

    high_bitmap = prefix_bitmaps[high]

    if low <= 0:
        return high_bitmap.copy()
    else:
        low_bitmap = prefix_bitmaps[low - 1]
        return high_bitmap & (~low_bitmap)


def run_rabit_cached(data, rabit_index, low: int, high: int):
    start = time.time()

    bitmap = rabit_range_bitmap(rabit_index, low, high)
    result = int(np.sum(bitmap))

    end = time.time()
    return end - start, result


def benchmark(func, *args, runs=3):
    times = []
    results = []

    for _ in range(runs):
        t, r = func(*args)
        times.append(t)
        results.append(r)

    return times, results


def main():
    con = duckdb.connect(DB_FILE)

    table_cache = {}
    rabit_cache = {}

    all_rows = []

    for q in WORKLOADS:
        table_name = q["table"]

        if table_name not in table_cache:
            print(f"Loading table: {table_name}")
            data = load_table_once(con, table_name)
            table_cache[table_name] = data
            table_workloads = [w for w in WORKLOADS if w["table"] == table_name]
            rabit_cache[table_name] = build_rabit_index_for_workload(data["a"], table_workloads)

        data = table_cache[table_name]
        rabit_index = rabit_cache[table_name]

        print(f"\nRunning {q['query_name']} on {table_name} [{q['low']}, {q['high']}]")

        scan_times, scan_results = benchmark(
            run_scan, con, table_name, q["low"], q["high"], runs=RUNS
        )
        bitmap_times, bitmap_results = benchmark(
            run_bitmap_cached, data, q["low"], q["high"], runs=RUNS
        )
        rabit_times, rabit_results = benchmark(
            run_rabit_cached, data, rabit_index, q["low"], q["high"], runs=RUNS
        )

        print("  Scan   :", scan_times)
        print("  Bitmap :", bitmap_times)
        print("  RABIT  :", rabit_times)

        for i in range(RUNS):
            all_rows.append({
                "query_name": q["query_name"],
                "table_name": table_name,
                "low": q["low"],
                "high": q["high"],
                "method": "scan",
                "run_id": i + 1,
                "time_sec": scan_times[i],
                "result": scan_results[i]
            })
            all_rows.append({
                "query_name": q["query_name"],
                "table_name": table_name,
                "low": q["low"],
                "high": q["high"],
                "method": "bitmap",
                "run_id": i + 1,
                "time_sec": bitmap_times[i],
                "result": bitmap_results[i]
            })
            all_rows.append({
                "query_name": q["query_name"],
                "table_name": table_name,
                "low": q["low"],
                "high": q["high"],
                "method": "rabit_like",
                "run_id": i + 1,
                "time_sec": rabit_times[i],
                "result": rabit_results[i]
            })

    result_df = pd.DataFrame(all_rows)
    result_df.to_csv("layout_selectivity_results.csv", index=False)

    summary_df = (
        result_df.groupby(["query_name", "table_name", "method"], as_index=False)["time_sec"]
        .mean()
        .rename(columns={"time_sec": "avg_time_sec"})
    )
    summary_df.to_csv("layout_selectivity_summary.csv", index=False)

    print("\nSaved:")
    print("- layout_selectivity_results.csv")
    print("- layout_selectivity_summary.csv")


if __name__ == "__main__":
    main()