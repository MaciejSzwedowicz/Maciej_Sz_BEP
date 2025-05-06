

# 💊 Structuring OpenFDA Adverse Event Data: SQLite vs MongoDB

This repository contains my Bachelor End Project (BEP) at Eindhoven University of Technology. The project explores how to convert semi-structured JSON data from the OpenFDA Adverse Events dataset into structured formats using both a **relational (SQLite)** and a **NoSQL (MongoDB)** database.

The goal is to evaluate the benefits and trade-offs of each approach in terms of structure expressiveness, query performance, and usability.

---

## 📁 Repository Structure
├── data/ # Datasets
│ ├── raw/ # Full raw OpenFDA JSON + database folders
│ │ ├── mongodb/
│ │ └── sqlite/
│ ├── processed/ # Cleaned / normalized intermediate data
│ └── sample/ # Small JSON samples (included in repo)
│
├── notebooks/ # Exploratory and processing notebooks
├── src/ # Source code
│ ├── parser/ # JSON parsing and transformation scripts
│ ├── db_sql/ # SQLite schema, insert & query scripts
│ └── db_mongo/ # MongoDB insert & query scripts
│
├── reports/ # Final report files and visual outputs
├── scratch/ # Experimental or backup scripts
├── tests/ # Unit and integration tests
├── sql/ # SQL schema and example queries
├── mongo/ # Notes or templates for MongoDB design


## 🚀 Running the MongoDB Insert Script

The script to load the full OpenFDA dataset into MongoDB is located at:
src/db_mongo/insert_pipline.py

### ✅ Usage (from project root)

```bash
python -m src.db_mongo.insert_pipline

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