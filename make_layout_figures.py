import pandas as pd
import matplotlib.pyplot as plt
import re

# =========================================================
RESULTS_FILE = "layout_selectivity_results.csv"
SUMMARY_FILE = "layout_selectivity_summary.csv"

# =========================================================
# Read data
# =========================================================
results_df = pd.read_csv(RESULTS_FILE)
summary_df = pd.read_csv(SUMMARY_FILE)

# =========================================================
# Helper mappings
# =========================================================
layout_map = {
    "t_layout_random": "Random",
    "t_layout_sorted": "Sorted",
    "t_layout_block_sorted": "Block-Sorted"
}

method_order = ["scan", "bitmap", "rabit_like"]
selectivity_order = ["0.1%", "1%", "5%", "10%", "20%", "50%", "90%"]


def extract_selectivity(query_name: str) -> str:
    m = re.search(r'_(0_1|1|5|10|20|50|90)_pct$', query_name)
    if not m:
        return query_name
    return m.group(1).replace("_", ".") + "%"


# =========================================================
# Preprocess
# =========================================================
results_df["layout"] = results_df["table_name"].map(layout_map)
summary_df["layout"] = summary_df["table_name"].map(layout_map)

results_df["selectivity"] = results_df["query_name"].apply(extract_selectivity)
summary_df["selectivity"] = summary_df["query_name"].apply(extract_selectivity)

results_df["selectivity"] = pd.Categorical(
    results_df["selectivity"], categories=selectivity_order, ordered=True
)
summary_df["selectivity"] = pd.Categorical(
    summary_df["selectivity"], categories=selectivity_order, ordered=True
)

# =========================================================
# 1. Correctness check
# Same query should produce the same result for all methods
# =========================================================
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

# Save correctness report
correctness.to_csv("layout_correctness_check.csv", index=False)

# =========================================================
# 2. Save report tables
# =========================================================
table_df = (
    summary_df.pivot_table(
        index=["layout", "selectivity"],
        columns="method",
        values="avg_time_sec"
    )
    .reset_index()
)

# Reorder columns if present
cols = ["layout", "selectivity"] + [c for c in method_order if c in table_df.columns]
table_df = table_df[cols]

table_df = table_df.sort_values(["layout", "selectivity"])
table_df_rounded = table_df.copy()
for c in method_order:
    if c in table_df_rounded.columns:
        table_df_rounded[c] = table_df_rounded[c].round(6)

table_df_rounded.to_csv("table_layout_selectivity.csv", index=False)

with open("table_layout_selectivity.md", "w") as f:
    f.write(table_df_rounded.to_markdown(index=False))

print("\nSaved tables:")
print("- table_layout_selectivity.csv")
print("- table_layout_selectivity.md")

# =========================================================
# 3. Figure by layout: one figure per layout
# =========================================================
for layout in ["Random", "Sorted", "Block-Sorted"]:
    subset = summary_df[summary_df["layout"] == layout]

    pivot = subset.pivot(index="selectivity", columns="method", values="avg_time_sec")
    pivot = pivot.reindex(selectivity_order)

    plt.figure(figsize=(8, 5))
    for method in method_order:
        if method in pivot.columns:
            plt.plot(pivot.index, pivot[method], marker="o", label=method)

    plt.xlabel("Selectivity")
    plt.ylabel("Average Time (second)")
    plt.title(f"{layout} Layout: Scan vs Bitmap vs RABIT")
    plt.legend()
    plt.tight_layout()
    filename = f"figure_{layout.lower().replace('-', '_')}_layout.png"
    plt.savefig(filename, dpi=200)
    plt.close()
    print(f"Saved: {filename}")

# =========================================================
# 4. Figure by method: compare layouts
# One figure per method
# =========================================================
for method in method_order:
    subset = summary_df[summary_df["method"] == method]

    pivot = subset.pivot(index="selectivity", columns="layout", values="avg_time_sec")
    pivot = pivot.reindex(selectivity_order)

    plt.figure(figsize=(8, 5))
    for layout in ["Random", "Sorted", "Block-Sorted"]:
        if layout in pivot.columns:
            plt.plot(pivot.index, pivot[layout], marker="o", label=layout)

    plt.xlabel("Selectivity")
    plt.ylabel("Average Time (second)")
    plt.title(f"{method}Layouts")
    plt.legend()
    plt.tight_layout()
    filename = f"figure_{method}_layout_compare.png"
    plt.savefig(filename, dpi=200)
    plt.close()
    print(f"Saved: {filename}")

# =========================================================
# 5. Speedup figure: bitmap / rabit_like
# >1 means RABIT-like is faster
# =========================================================
speedup_df = (
    summary_df.pivot_table(
        index=["layout", "selectivity"],
        columns="method",
        values="avg_time_sec"
    )
    .reset_index()
)

speedup_df["bitmap_over_rabit_speedup"] = speedup_df["bitmap"] / speedup_df["rabit_like"]
speedup_df["bitmap_over_rabit_speedup"] = speedup_df["bitmap_over_rabit_speedup"].round(4)
speedup_df.to_csv("table_bitmap_rabit_speedup.csv", index=False)

plt.figure(figsize=(8, 5))
for layout in ["Random", "Sorted", "Block-Sorted"]:
    subset = speedup_df[speedup_df["layout"] == layout].copy()
    subset["selectivity"] = pd.Categorical(
        subset["selectivity"], categories=selectivity_order, ordered=True
    )
    subset = subset.sort_values("selectivity")
    plt.plot(
        subset["selectivity"],
        subset["bitmap_over_rabit_speedup"],
        marker="o",
        label=layout
    )

plt.xlabel("Selectivity")
plt.ylabel("Bitmap / RABIT speedup")
plt.title("speedup of RABIT over bitmap")
plt.legend()
plt.tight_layout()
plt.savefig("figure_bitmap_vs_rabit_speedup.png", dpi=200)
plt.close()
print("Saved: figure_bitmap_vs_rabit_speedup.png")

# =========================================================
# 6. Print concise summary statistics
# =========================================================
mean_by_layout_method = (
    summary_df.groupby(["layout", "method"])["avg_time_sec"]
    .mean()
    .reset_index()
)

print("\naverage time by layout")
print(mean_by_layout_method.to_string(index=False))

# save
mean_by_layout_method.to_csv("table_mean_by_layout_method.csv", index=False)

print("\nSaved:")
print("- layout_correctness_check.csv")
print("- table_mean_by_layout_method.csv")
print("- figure_random_layout.png")
print("- figure_sorted_layout.png")
print("- figure_block_sorted_layout.png")
print("- figure_scan_layout_compare.png")
print("- figure_bitmap_layout_compare.png")
print("- figure_rabit_layout_compare.png")
print("- figure_bitmap_vs_rabit_speedup.png")