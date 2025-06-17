# BEP — Structured vs Semi-Structured Data (OpenFDA)

## 📁 src/ Folder
This folder contains all core logic for interacting with and transforming the OpenFDA dataset. It is organized into three functional submodules:

### src/db_sql/
Logic for handling the SQLite relational database:
- `create_final_sql_schema_split_openfda_indexed.py` — initializes the SQLite database.  
  *(The database should get initialized inside the `sql/` folder.)*
- `insert_final_refactored_openfda.py` — pipeline for inserting reports into the database.

### src/db_mongo/
Code for MongoDB ingestion (semi-structured baseline):
- `insert_pipeline_mongo_limited.py` — transforms and loads JSON reports into the `full_reports` collection with type handling.

### src/parser/
Utility for parsing large OpenFDA JSON files efficiently:
- `iterate_reports.py` — streaming parser using `ijson` to yield one report at a time.

> `.gitkeep` and `__init__.py` files are included for structural and packaging consistency.

---

## 📓 notebooks/ Folder
This folder contains all Jupyter notebooks used throughout the project.

### Key Notebook:
- `final_performance_evaluation.ipynb`  
  Used in the thesis to benchmark query performance across both database systems. It summarizes and visualizes runtime metrics from a standardized set of queries.

### Supporting Notebooks:
- `OpenFDA_Structure_Analysis.ipynb`  
  Used for exploratory data analysis (EDA) and understanding the structure and irregularities of the OpenFDA dataset.

Other notebooks were created during early stages of development and validation and are not directly referenced in the final report.

---

## 📂 data/ and sql/ Folders

These directories are used to store input data and generated database artifacts.

### data/raw/source_data/
This is the designated location for storing the raw OpenFDA JSON files used during both MongoDB and SQLite ingestion. The scripts expect the data to be placed in this exact subfolder.

### sql/
Output location for the generated SQLite database (e.g., `openfda_final.db`) after executing the schema creation and ingestion steps.

Both folders are initially empty and populated during runtime.

---

## ▶️ Execution Order

This section outlines the recommended order of execution to fully initialize both databases and perform query performance evaluation.

### 🗃️ SQLite Relational Database

1. **Place Data**  
   Save the raw OpenFDA JSON files into the `data/raw/source_data/` directory.

2. **Create Schema**  
   Run the schema creation script to initialize the database structure:
   <!-- ```bash
   python src/db_sql/create_final_sql_schema_split_openfda_indexed.py
   ``` -->

3. **Insert Data**  
   Populate the database with data from the JSON source:
   <!-- ```bash
   python src/db_sql/insert_final_refactored_openfda.py --json_path data/raw/source_data/
   ``` -->

---

### 🍃 MongoDB (Semi-Structured Baseline)

1. **Start MongoDB**  
   Ensure MongoDB is running locally or adjust the connection settings in the script accordingly.

2. **Insert Data**  
   Run the MongoDB insertion pipeline:
   <!-- ```bash
   python src/db_mongo/insert_pipeline_mongo_limited.py --json_path data/raw/source_data/
   ``` -->

---

### 📊 Performance Evaluation

Once both databases are populated, you can run the performance benchmarking notebook:

```bash
jupyter notebook notebooks/final_performance_evaluation.ipynb
```

This notebook compares query runtimes, result consistency, and complexity across the two systems.


## Oversized Document Skipped

- `safetyreportid`: 20937
- Size: ~25.2MB
- MongoDB BSON limit: 16MB
- Skipped during insert, ID saved to: `reports/evaluation_results/oversized_reports_skipped.json`




