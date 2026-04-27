# CS561 RABIT Experiment

## Overview

The goal is to compare three query evaluation methods under the same workload:

- **Scan**: DuckDB native SQL execution
- **Bitmap**: on-the-fly bitmap filtering using NumPy
- **RABIT**: prefix-bitmap range evaluation

The focus of experiments is to study how these methods behave under different of these:

- **layouts**: random / sorted / block-sorted
- **selectivities**: from very selective to broad range queries


## Methods

### 1. Scan
DuckDB executes the SQL query:

```sql
SELECT count(*)
FROM <table>
WHERE a BETWEEN low AND high;  
```
 
#### 2. Bitmap
The table column a is loaded into NumPy once:

```bash
(a >= low) & (a <= high)
```

This represents an on-the-fly bitmap filter.


#### 3. RABIT
Use prefix bitmaps of the form:
- a <= t 
- A range query: low <= a <= high
- (a <= high) AND NOT (a <= low - 1)

This represents an on-the-fly bitmap filter.



## Workload

### 1. Tables used

These correspond to the layout/selectivity workload: 
- t_layout_random
- t_layout_sorted
- t_layout_block_sorted

#### 2. Query form

All queries follow this:
```sql
SELECT count(*)
FROM <table>
WHERE a BETWEEN low AND high; 
```

#### 3. Workload configuration

- 0.1%
- 1%
- 5%
- 10%
- 20%
- 30%
- 40%
- 50%
- 60%
- 70%
- 80%
- 90%

Each selectivity is evaluated on this three layouts:
- Random
- Sorted
- Block-Sorted

So the experiment compares:

- same predicate type
- same selectivity
- same table size
- different layout / access path behavior


## Main scripts

#### build_workloads.py
Creates the shared DuckDB database:
t_layout_random     t_layout_sorted     t_layout_block_sorted

#### run_bitmap_rabit.py
Runs the layout-selectivity experiment and compares:
- scan
- bitmap
- rabit_like

For each query, it records:
- method
- runtime
- result count

Output files:
- layout_selectivity_results.csv
- layout_selectivity_summary.csv

#### make_layout_figures.py
Then line plots by layout and method


## API and libraries

#### DuckDB API
connect to the workload database and load table data
run baseline SQL queries

- duckdb.connect(...)
- con.execute(...)
- .fetchone()
- .fetchnumpy()

#### NumPy

use for: bitmap construction, prefix bitmap construction

- (a >= low) & (a <= high)
- (a_values <= t)
- np.sum(...)
- np.searchsorted(...)


#### Pandas

use for: storing experiment results, aggregating average runtime

#### Matplotlib

use for:plotting runtime comparisons, generate figures


## How to run

#### run
```bash
source venv310/bin/activate
python build_workloads.py
python run_bitmap_rabit.py
python make_layout_figures.py
```

python run_bitmap_rabit.py: This generates:
layout_selectivity_results.csv
layout_selectivity_summary.csv


#### run on SCC
SCC venv too old, update python in venv :

```bash
python -m venv venv310 
source venv310/bin/activate 

module avail python 
module load python3/3.10.12 
pip install duckdb pandas numpy matplotlib
```

run:
```bash
python run_bitmap_rabit.py 
python make_layout_figures.py 
```

check duckdb can run
```bash
import duckdb 
con = duckdb.connect("test.duckdb") 
print(con.execute("SELECT 42").fetchall()) 
```


## Notes

The main goal is to find that:
how layout affects performance
how selectivity affects performance
whether prefix-based RABIT evaluation improves over on-the-fly bitmap filtering











