import duckdb
import time
import numpy as np
import pandas as pd

DB_FILE = "tpch.duckdb"
RUNS = 5

QUERY_CONFIGS = [
    {"name": "very_narrow", "discount_low": 0.05, "discount_high": 0.051, "quantity_threshold": 24},
    {"name": "narrow",      "discount_low": 0.05, "discount_high": 0.055, "quantity_threshold": 24},
    {"name": "medium",      "discount_low": 0.05, "discount_high": 0.07,  "quantity_threshold": 24},
    {"name": "wide",        "discount_low": 0.01, "discount_high": 0.09,  "quantity_threshold": 24},
]


#Load data
def load_data_once(con):
    df = con.execute("""
        SELECT l_extendedprice, l_discount, l_quantity
        FROM lineitem
    """).fetchdf()

    return {
        "price": df["l_extendedprice"].to_numpy(),
        "discount": df["l_discount"].to_numpy(),
        "quantity": df["l_quantity"].to_numpy()
    }



#scan
def run_scan(con, discount_low, discount_high, quantity_threshold):
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



#bitmap (on the fly)
def run_bitmap_cached(data, discount_low, discount_high, quantity_threshold):
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


#RABIT
def rabit_range_bitmap_safe(discount, low, high):
    return (discount >= low) & (discount <= high)


def run_rabit_cached(data, discount_low, discount_high, quantity_threshold):
    price = data["price"]
    discount = data["discount"]
    quantity = data["quantity"]

    start = time.time()

    bitmap_discount = rabit_range_bitmap_safe(discount, discount_low, discount_high)
    bitmap_quantity = (quantity < quantity_threshold)
    final_bitmap = bitmap_discount & bitmap_quantity

    result = np.sum(price[final_bitmap] * discount[final_bitmap])

    end = time.time()
    return end - start, result


#CUBIT (test now)
def compress_bitmap(bitmap):
    compressed = []
    count = 1

    for i in range(1, len(bitmap)):
        if bitmap[i] == bitmap[i - 1]:
            count += 1
        else:
            compressed.append((bitmap[i - 1], count))
            count = 1

    compressed.append((bitmap[-1], count))
    return compressed


def sum_with_compressed_bitmap(compressed, price, discount):
    total = 0
    idx = 0

    for val, count in compressed:
        if val:
            segment_price = price[idx: idx + count]
            segment_discount = discount[idx: idx + count]
            total += np.sum(segment_price * segment_discount)

        idx += count

    return total


def run_cubit_like(data, discount_low, discount_high, quantity_threshold):
    price = data["price"]
    discount = data["discount"]
    quantity = data["quantity"]

    start = time.time()

    bitmap_discount = (discount >= discount_low) & (discount <= discount_high)
    bitmap_quantity = (quantity < quantity_threshold)
    final_bitmap = bitmap_discount & bitmap_quantity

    compressed = compress_bitmap(final_bitmap)

    result = sum_with_compressed_bitmap(compressed, price, discount)

    end = time.time()
    return end - start, result


def benchmark(func, *args, runs=5):
    times = []
    results = []

    for _ in range(runs):
        t, r = func(*args)
        times.append(t)
        results.append(r)

    return times, results


def main():
    con = duckdb.connect(DB_FILE)

    print("Loading data...")
    data = load_data_once(con)

    rows = []

    for config in QUERY_CONFIGS:
        print(f"\nRunning: {config['name']}")

        scan_times, scan_results = benchmark(
            run_scan, con,
            config["discount_low"],
            config["discount_high"],
            config["quantity_threshold"],
            runs=RUNS
        )

        bitmap_times, bitmap_results = benchmark(
            run_bitmap_cached, data,
            config["discount_low"],
            config["discount_high"],
            config["quantity_threshold"],
            runs=RUNS
        )

        rabit_times, rabit_results = benchmark(
            run_rabit_cached, data,
            config["discount_low"],
            config["discount_high"],
            config["quantity_threshold"],
            runs=RUNS
        )

        cubit_times, cubit_results = benchmark(
            run_cubit_like, data,
            config["discount_low"],
            config["discount_high"],
            config["quantity_threshold"],
            runs=RUNS
        )

        print("  Scan  :", scan_times)
        print("  Bitmap:", bitmap_times)
        print("  Rabit :", rabit_times)
        print("  Cubit :", cubit_times)

        for i in range(RUNS):
            rows.extend([
                {"query_name": config["name"], "method": "scan", "run": i, "time": scan_times[i], "result": scan_results[i]},
                {"query_name": config["name"], "method": "bitmap", "run": i, "time": bitmap_times[i], "result": bitmap_results[i]},
                {"query_name": config["name"], "method": "rabit_like", "run": i, "time": rabit_times[i], "result": rabit_results[i]},
                {"query_name": config["name"], "method": "cubit_like", "run": i, "time": cubit_times[i], "result": cubit_results[i]},
            ])

    df = pd.DataFrame(rows)
    df.to_csv("final_results.csv", index=False)

    print("\nSaved to final_results.csv")


if __name__ == "__main__":
    main()
