

# 💊 Structuring OpenFDA Adverse Event Data: SQLite vs MongoDB

This repository contains my Bachelor End Project (BEP) at Eindhoven University of Technology. The project explores how to convert semi-structured JSON data from the OpenFDA Adverse Events dataset into structured formats using both a **relational (SQLite)** and a **NoSQL (MongoDB)** database.

The goal is to evaluate the benefits and trade-offs of each approach in terms of structure expressiveness, query performance, and usability.

---

## 📁 Repository Structure
├── data/
│ ├── raw/
│ │ ├── source_data/ # Full raw OpenFDA JSON files (3-part set)
│ │ ├── sqlite/ # SQLite DB files
│ │ └── mongodb/ # MongoDB dump (optional)
│ ├── processed/ # Cleaned / normalized intermediate data
│ └── sample/ # Small JSON samples for testing and schema inference
│
├── notebooks/ # Jupyter Notebooks for exploration and pipeline validation
├── src/
│ ├── parser/ # JSON streaming and transformation logic
│ ├── db_sql/ # SQLite schema, insert & query scripts
│ └── db_mongo/ # MongoDB insert & query scripts (insert_pipline.py lives here)
│
├── sql/ # SQL schema and evaluation queries
├── tests/ # Unit and integration tests
├── scratch/ # Experimental utilities and debug tools
├── reports/ # Final results, plots, and evaluation figures
├── mongo/ # Design notes and structure ideas for MongoDB


## 🚀 Running the MongoDB Insert Script

The script to load the full OpenFDA dataset into MongoDB is located at:
src/db_mongo/insert_pipline.py

### ✅ Usage (from project root)

```bash
python -m src.db_mongo.insert_pipline

This will:
Connect to the default MongoDB URI (localhost:27017)
Insert all reports from the data/raw/source_data/ folder
Skip any duplicate records based on safetyreportid
Print progress and insertion time

🛠️ Optional CLI Parameters
You can customize it using the following flags:

bash
Kopiuj
Edytuj
python -m src.db_mongo.insert_pipline \
  --db openfda_alt \
  --collection my_collection \
  --json_path data/sample/OpenFDA_sample_combined.json \
  --mongo_uri mongodb://localhost:27017