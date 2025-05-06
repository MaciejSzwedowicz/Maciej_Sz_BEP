from pymongo import MongoClient, errors
from src.parser.iterate_reports import iterate_reports_ijson
import logging
from time import time
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))


# === CONFIGURATION ===
DB_NAME = "openfda"
COLLECTION_NAME = "full_reports"
MONGO_URI = "mongodb://localhost:27017/"
JSON_PATH = "data/raw/source_data"  # Folder containing multiple JSON files


def insert_reports_one_by_one(db_name, collection_name, report_generator, mongo_uri=MONGO_URI):
    client = MongoClient(mongo_uri)
    db = client[db_name]
    collection = db[collection_name]

    # Create index if not exists
    try:
        collection.create_index("safetyreportid", unique=True)
    except errors.OperationFailure as e:
        logging.warning(f"Index creation skipped or failed: {e}")

    total_inserted = 0
    duplicate_count = 0
    start_time = time()

    for i, report in enumerate(report_generator, 1):
        try:
            collection.insert_one(report)
            total_inserted += 1
            if i % 100 == 0:
                logging.info(f"Inserted {total_inserted} reports so far...")
        except errors.DuplicateKeyError:
            duplicate_count += 1
            if duplicate_count % 50 == 0:
                logging.warning(f"Duplicate report skipped (example): {report.get('safetyreportid')}")
        except Exception as e:
            logging.error(f"Error inserting report {i}: {e}")

    elapsed = time() - start_time
    logging.info(f"Done. Inserted {total_inserted} documents in {elapsed:.2f} seconds.")
    logging.info(f"Skipped {duplicate_count} duplicates.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    gen = iterate_reports_ijson(JSON_PATH)
    insert_reports_one_by_one(DB_NAME, COLLECTION_NAME, gen)