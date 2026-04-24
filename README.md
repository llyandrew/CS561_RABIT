SCC venv too old:

update python in venv
python -m venv venv310
source venv310/bin/activate

module avail python
module load python3/3.10.12
pip install duckdb pandas numpy matplotlib

RUN:
python run_bitmap_rabit.py
python make_layout_figures.py

(chack duckdb can run)
import duckdb
con = duckdb.connect("test.duckdb")
print(con.execute("SELECT 42").fetchall())
