import duckdb

DB_FILE = "workloads.duckdb"

sql_files = [
    "sql/create_base_uniform.sql",
    "sql/create_layout_random.sql",
    "sql/create_layout_sorted.sql",
    "sql/create_layout_block_sorted.sql",
]

con = duckdb.connect(DB_FILE)

for path in sql_files:
    print(f"Running {path} ...")
    with open(path, "r") as f:
        sql = f.read()
    con.execute(sql)

print("\nTables created:")
print(con.execute("SHOW TABLES").fetchall())

con.close()
print("\nDone.")