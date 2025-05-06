import argparse
import logging
import os
import sys
from time import time
from pymongo import MongoClient, errors

# Add src to path if not running with -m
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))
from parser.iterate_reports import iterate_reports_ijson


def insert_reports_one_by_one(db_name, collection_name, json_path, mongo_uri="mongodb://localhost:27017"):
    client = MongoClient(mongo_uri)
    db = client[db_name]
    collection = db[collection_name]

    try:
        collection.create_index("safetyreportid", unique=True)
    except errors.OperationFailure as e:
        logging.warning(f"Index creation skipped or failed: {e}")

    gen = iterate_reports_ijson(json_path)
    total_inserted = 0
    duplicate_count = 0
    start_time = time()

    for i, report in enumerate(gen, 1):
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
    parser = argparse.ArgumentParser(description="Insert OpenFDA reports into MongoDB.")
    parser.add_argument("--db", default="openfda", help="MongoDB database name")
    parser.add_argument("--collection", default="full_reports", help="MongoDB collection name")
    parser.add_argument("--json_path", default="data/raw/source_data", help="Path to JSON file or directory")
    parser.add_argument("--mongo_uri", default="mongodb://localhost:27017", help="MongoDB connection URI")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    insert_reports_one_by_one(args.db, args.collection, args.json_path, args.mongo_uri)
