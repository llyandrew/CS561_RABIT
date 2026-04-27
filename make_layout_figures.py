import pandas as pd
import matplotlib.pyplot as plt
import re
import os

RESULTS_FILE = "layout_selectivity_results.csv"
SUMMARY_FILE = "layout_selectivity_summary.csv"

FIG_DIR = "figures"
os.makedirs(FIG_DIR, exist_ok=True)

CSV_DIR = "csv"
os.makedirs(CSV_DIR, exist_ok=True)

results_df = pd.read_csv(RESULTS_FILE)
summary_df = pd.read_csv(SUMMARY_FILE)

layout_map = {
    "t_layout_random": "Random",
    "t_layout_sorted": "Sorted",
    "t_layout_block_sorted": "Block-Sorted"
}

method_order = ["scan", "bitmap", "rabit_like"]
selectivity_order = [0.1, 1, 5, 10, 20, 30, 40, 50, 60, 70, 80, 90]


def extract_selectivity(query_name: str):
    m = re.search(r'_(0_1|1|5|10|20|30|40|50|60|70|80|90)_pct$', query_name)
    if not m:
        return None
    return float(m.group(1).replace("_", "."))


# preprocess
results_df["layout"] = results_df["table_name"].map(layout_map)
summary_df["layout"] = summary_df["table_name"].map(layout_map)

results_df["selectivity"] = results_df["query_name"].apply(extract_selectivity)
summary_df["selectivity"] = summary_df["query_name"].apply(extract_selectivity)

results_df = results_df.dropna(subset=["selectivity"])
summary_df = summary_df.dropna(subset=["selectivity"])

results_df = results_df.sort_values(["layout", "selectivity"])
summary_df = summary_df.sort_values(["layout", "selectivity"])

# correctness check
correctness = (
    results_df.groupby(["layout", "query_name", "run_id"])["result"]
    .nunique()
    .reset_index(name="num_distinct_results")
)

if (correctness["num_distinct_results"] == 1).all():
    print("Correctness check passed: all methods return the same result per query/run.")
else:
    print("Warning: some queries have mismatched results across methods.")
    print(correctness[correctness["num_distinct_results"] != 1])

correctness.to_csv(os.path.join(CSV_DIR, "layout_correctness_check.csv"), index=False)

# save report table
table_df = (
    summary_df.pivot_table(
        index=["layout", "selectivity"],
        columns="method",
        values="avg_time_sec",
        aggfunc="mean"
    )
    .reset_index()
)

# flatten column names just in case
table_df.columns = [c if isinstance(c, str) else c[1] if c[1] else c[0] for c in table_df.columns]

cols = ["layout", "selectivity"] + [c for c in method_order if c in table_df.columns]
table_df = table_df[cols]

table_df = table_df.sort_values(["layout", "selectivity"])
table_df_rounded = table_df.copy()

for c in method_order:
    if c in table_df_rounded.columns:
        table_df_rounded[c] = table_df_rounded[c].round(6)

table_df_rounded.to_csv(os.path.join(FIG_DIR, "table_layout_selectivity.csv"), index=False)

with open(os.path.join(CSV_DIR, "table_layout_selectivity.md"), "w") as f:
    f.write(table_df_rounded.to_markdown(index=False))

print("\nSaved tables:")
print("- table_layout_selectivity.csv")
print("- table_layout_selectivity.md")

# Figure by layout
for layout in ["Random", "Sorted", "Block-Sorted"]:
    subset = summary_df[summary_df["layout"] == layout]

    pivot = subset.pivot_table(
        index="selectivity",
        columns="method",
        values="avg_time_sec",
        aggfunc="mean"
    )
    pivot = pivot.reindex(selectivity_order)

    plt.figure(figsize=(8, 5))
    x = pivot.index.astype(float)

    plotted = False
    for method in method_order:
        if method in pivot.columns:
            plt.plot(x, pivot[method], marker="o", label=method)
            plotted = True

    plt.xlabel("Selectivity (%)")
    plt.ylabel("Average Time (second)")
    plt.title(f"{layout} Layout: Scan vs Bitmap vs RABIT")
    plt.xticks(selectivity_order, [f"{v}%" for v in selectivity_order])

    if plotted:
        plt.legend()

    plt.tight_layout()
    filename = os.path.join(FIG_DIR, f"figure_{layout.lower().replace('-', '_')}_layout.png")
    plt.savefig(filename, dpi=200)
    plt.close()
    print(f"Saved: {filename}")

# Figure by method
for method in method_order:
    subset = summary_df[summary_df["method"] == method]

    pivot = subset.pivot_table(
        index="selectivity",
        columns="layout",
        values="avg_time_sec",
        aggfunc="mean"
    )
    pivot = pivot.reindex(selectivity_order)

    plt.figure(figsize=(8, 5))
    x = pivot.index.astype(float)

    plotted = False
    for layout in ["Random", "Sorted", "Block-Sorted"]:
        if layout in pivot.columns:
            plt.plot(x, pivot[layout], marker="o", label=layout)
            plotted = True

    plt.xlabel("Selectivity (%)")
    plt.ylabel("Average Time (second)")
    plt.title(f"{method} Across Layouts")
    plt.xticks(selectivity_order, [f"{v}%" for v in selectivity_order])

    if plotted:
        plt.legend()

    plt.tight_layout()
    filename = os.path.join(FIG_DIR, f"figure_{method}_layout_compare.png")
    plt.savefig(filename, dpi=200)
    plt.close()
    print(f"Saved: {filename}")

# Speedup figure
speedup_df = summary_df.pivot_table(
    index=["layout", "selectivity"],
    columns="method",
    values="avg_time_sec",
    aggfunc="mean"
).reset_index()

speedup_df.columns = [c if isinstance(c, str) else c[1] if c[1] else c[0] for c in speedup_df.columns]

print("\nSpeedup columns:", speedup_df.columns.tolist())

speedup_df["bitmap_over_rabit_speedup"] = speedup_df["bitmap"] / speedup_df["rabit_like"]
speedup_df["bitmap_over_rabit_speedup"] = speedup_df["bitmap_over_rabit_speedup"].round(4)
speedup_df.to_csv(os.path.join(CSV_DIR, "table_bitmap_rabit_speedup.csv"), index=False)

plt.figure(figsize=(8, 5))
for layout in ["Random", "Sorted", "Block-Sorted"]:
    subset = speedup_df[speedup_df["layout"] == layout].copy()
    subset = subset.sort_values("selectivity")

    plt.plot(
        subset["selectivity"].astype(float),
        subset["bitmap_over_rabit_speedup"],
        marker="o",
        label=layout
    )

plt.xlabel("Selectivity (%)")
plt.ylabel("Bitmap / RABIT speedup")
plt.title("Speedup of RABIT over Bitmap")
plt.xticks(selectivity_order, [f"{v}%" for v in selectivity_order])
plt.legend()
plt.tight_layout()
plt.savefig(os.path.join(FIG_DIR, "figure_bitmap_vs_rabit_speedup.png"), dpi=200)
plt.close()
print("Saved: figure_bitmap_vs_rabit_speedup.png")

# summary stats
mean_by_layout_method = (
    summary_df.groupby(["layout", "method"])["avg_time_sec"]
    .mean()
    .reset_index()
)

print("\nAverage time by layout")
print(mean_by_layout_method.to_string(index=False))

mean_by_layout_method.to_csv(os.path.join(CSV_DIR, "table_mean_by_layout_method.csv"), index=False)
print("\nSaved:")
print("- layout_correctness_check.csv")
print("- table_mean_by_layout_method.csv")
print("- figure_random_layout.png")
print("- figure_sorted_layout.png")
print("- figure_block_sorted_layout.png")
print("- figure_scan_layout_compare.png")
print("- figure_bitmap_layout_compare.png")
print("- figure_rabit_like_layout_compare.png")
print("- figure_bitmap_vs_rabit_speedup.png")