import sys
import os
import glob
import ijson
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from pymongo import MongoClient, errors
from src.parser.iterate_reports import iterate_reports_ijson

# === CONFIGURATION ===
DB_NAME = "openfda_sample"
COLLECTION_NAME = "sample_reports"
JSON_PATH = "data\sample\OpenFDA_sample_combined.json"  # Adjust path if needed
MONGO_URI = "mongodb://localhost:27017/"

# === CONNECT TO MONGO ===
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

# === OPTIONAL: Create unique index on 'safetyreportid' ===
try:
    collection.create_index("safetyreportid", unique=True)
except errors.OperationFailure as e:
    print(f"Index creation skipped or failed: {e}")

# === INSERT REPORTS USING GENERATOR ===
successful = 0
try:
    for report in iterate_reports_ijson(JSON_PATH):
        try:
            collection.insert_one(report)
            successful += 1
        except errors.DuplicateKeyError:
            continue  # Skip if already inserted
        except Exception as e:
            print(f"Failed to insert report: {e}")
except Exception as e:
    print(f"Error while iterating reports: {e}")
    sys.exit(1)

print(f"Inserted {successful} new reports into MongoDB.")
