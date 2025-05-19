# 📊 BEP Performance Evaluation: SQLite vs MongoDB

This notebook benchmarks query execution time across SQLite and MongoDB using the OpenFDA dataset.

---

## 📦 1. Setup

```python
import time
import sqlite3
from pymongo import MongoClient
import pandas as pd
import matplotlib.pyplot as plt

# SQLite
sqlite_path = "path/to/your.db"
sqlite_conn = sqlite3.connect(sqlite_path)
sqlite_cursor = sqlite_conn.cursor()

# MongoDB
mongo_client = MongoClient("mongodb://localhost:27017")
mongo_db = mongo_client["openfda_converted"]
mongo_collection = mongo_db["full_reports"]

# Helper function to time a query
def time_query(func, n_runs=5):
    durations = []
    for _ in range(n_runs):
        start = time.perf_counter()
        func()
        durations.append(time.perf_counter() - start)
    return durations
```

---

## 🔍 2. Query Performance Comparison Strategy

### Query Categories:
- **Simple**: Single-table lookups, counts, or filters
- **Medium**: Multi-table joins or filtered aggregates
- **Complex**: Nested logic, joins with filtering, or document traversals

Each query will be:
- Executed multiple times (default: 5 runs)
- Reported as a list of runtimes
- Summarized using mean and standard deviation

---

## 🧪 3. Simple Queries

### 🟢 3.1 Count All Reports

#### SQLite
```python
def sqlite_query_count_reports():
    sqlite_cursor.execute("SELECT COUNT(*) FROM report").fetchone()
# durations = time_query(sqlite_query_count_reports)
```

#### MongoDB
```python
def mongo_query_count_reports():
    mongo_collection.count_documents({})
# durations = time_query(mongo_query_count_reports)
```

---

## 🧩 4. Medium Queries

_Add structure here for medium-complexity queries._

---

## 🧠 5. Complex Queries

_Add structure here for high-complexity logic._

---

## 📈 6. Summary of Results

_You can collect timing data into a Pandas DataFrame and plot it like this:_

```python
# Example placeholder structure
results = pd.DataFrame([
    {"query": "count_reports", "db": "SQLite", "mean": 0.012, "std": 0.002},
    {"query": "count_reports", "db": "MongoDB", "mean": 0.018, "std": 0.003},
])
results.pivot("query", "db", "mean").plot(kind="bar", ylabel="Time (s)", title="Query Runtime Comparison")
plt.show()
```