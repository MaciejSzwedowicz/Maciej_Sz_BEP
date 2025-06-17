

# 💊 Structuring OpenFDA Adverse Event Data: SQLite vs MongoDB

This repository contains my Bachelor End Project (BEP) at Eindhoven University of Technology. The project explores how to convert semi-structured JSON data from the OpenFDA Adverse Events dataset into structured formats using both a **relational (SQLite)** and a **NoSQL (MongoDB)** database.

The goal is to evaluate the benefits and trade-offs of each approach in terms of structure expressiveness, query performance, and usability.

---




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


## Oversized Document Skipped

- `safetyreportid`: 20937
- Size: ~25.2MB
- MongoDB BSON limit: 16MB
- Skipped during insert, ID saved to: `reports/evaluation_results/oversized_reports_skipped.json`
