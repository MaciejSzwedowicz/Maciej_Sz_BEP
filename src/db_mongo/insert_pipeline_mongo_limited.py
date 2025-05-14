
""" 
MongoDB insertion pipeline with:
- Fully dynamic field conversion (based on CSV)
- Limit support for fast dev iterations
- case_event_date extraction and drug date normalization
"""

import argparse
import os
import sys
import logging
import re
import json
from datetime import datetime
from pymongo import MongoClient, errors


sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from src.parser.iterate_reports import iterate_reports_ijson

def safe_int(val):
    try: return int(val)
    except: return val

def safe_float(val):
    try: return float(val)
    except: return val

def normalize_date_iso(date_str):
    if not isinstance(date_str, str) or not date_str.isdigit():
        return None
    try:
        if len(date_str) == 8:
            return datetime.strptime(date_str, "%Y%m%d")
        elif len(date_str) == 6:
            return datetime.strptime(date_str + "01", "%Y%m%d")
        elif len(date_str) == 4:
            return datetime.strptime(date_str + "0101", "%Y%m%d")
    except ValueError:
        return None
    return None

def extract_case_event_date(text):
    if isinstance(text, str):
        match = re.search(r"CASE EVENT DATE:\s*(\d{8})", text)
        if match:
            return normalize_date_iso(match.group(1))
    return None


def set_nested_safe(obj, keys, converter):
    try:
        for k in keys[:-1]:
            obj = obj.get(k, {}) if isinstance(obj, dict) else {}
        if isinstance(obj, dict) and keys[-1] in obj:
            original = obj[keys[-1]]
            converted = converter(original)
            obj[keys[-1]] = converted
            logging.debug(f"✅ Converted {'.'.join(keys)}: {original} → {converted}")
        else:
            logging.debug(f"⚠️ Key {keys[-1]} not in object for {'.'.join(keys)}")
    except Exception as e:
        logging.warning(f"Failed to convert {'.'.join(keys)}: {e}")

def transform_report(report):
    # Convert safetyreportid explicitly
    report["safetyreportid"] = safe_int(report.get("safetyreportid"))

    # -------- Top-level or nested fields (non-list paths) --------
    for path, func in [
        (['patient', 'patientagegroup'], safe_int),
        (['patient', 'patientonsetage'], safe_int),
        (['patient', 'patientonsetageunit'], safe_int),
        (['patient', 'patientsex'], safe_int),
        (['patient', 'patientweight'], safe_float),
        (['safetyreportversion'], safe_int),
        (['receivedateformat'], safe_int),
        (['receiptdateformat'], safe_int),
        (['transmissiondateformat'], safe_int),
        (['reporttype'], safe_int),
        (['fulfillexpeditecriteria'], safe_int),
        (['serious'], safe_int),
        (['seriousnessdeath'], safe_int),
        (['seriousnesslifethreatening'], safe_int),
        (['seriousnesshospitalization'], safe_int),
        (['seriousnessdisabling'], safe_int),
        (['seriousnesscongenitalanomali'], safe_int),
        (['seriousnessother'], safe_int),
        (['duplicate'], safe_int),
        (['primarysource', 'qualification'], safe_int),
        (['sender', 'sendertype'], safe_int),
        (['receiver', 'receivertype'], safe_int),
    ]:
        set_nested_safe(report, path, func)

    # -------- Dates (normalize) --------
    for date_path in [
        ['receivedate'], ['receiptdate'], ['transmissiondate']
    ]:
        set_nested_safe(report, date_path, normalize_date_iso)

    # -------- Extract case_event_date_extracted --------
    summary = report.get("patient", {}).get("summary", {})
    if isinstance(summary, dict):
        narrative = summary.get("narrativeincludeclinical")
        extracted = extract_case_event_date(narrative)
        if extracted:
            summary["case_event_date_extracted"] = extracted
            logging.debug(f"✅ Extracted case_event_date: {extracted}")

    # -------- patient.drug (list of dicts) --------
    for drug in report.get("patient", {}).get("drug", []):
        for path, func in [
            (["drugcharacterization"], safe_int),
            (["drugauthorizationnumb"], safe_int),
            (["drugadministrationroute"], safe_int),
            (["actiondrug"], safe_int),
            (["drugadditional"], safe_int),
            (["drugintervaldosagedefinition"], safe_int),
            (["drugcumulativedosagenumb"], safe_float),
            (["drugcumulativedosageunit"], safe_int),
            (["drugenddateformat"], safe_int),
            (["drugintervaldosageunitnumb"], safe_float),
            (["drugrecurreadministration"], safe_int),
            (["drugseparatedosagenumb"], safe_float),
            (["drugstartdateformat"], safe_int),
            (["drugstructuredosagenumb"], safe_float),
            (["drugstructuredosageunit"], safe_int),
            (["drugtreatmentduration"], safe_float),
            (["drugtreatmentdurationunit"], safe_int),
            (["drugstartdate"], normalize_date_iso),
            (["drugenddate"], normalize_date_iso),
        ]:
            set_nested_safe(drug, path, func)

    # -------- patient.reaction (list of dicts) --------
    for reaction in report.get("patient", {}).get("reaction", []):
        set_nested_safe(reaction, ["reactionmeddraversionpt"], safe_float)
        set_nested_safe(reaction, ["reactionoutcome"], safe_int)

    return report


def insert_reports(db, collection_name, reports, limit=None):
    collection = db[collection_name]
    inserted = 0
    for i, report in enumerate(reports):
        if limit and inserted >= limit:
            break
        report = transform_report(report)
        rid = report.get("safetyreportid")
        if not rid:
            logging.warning("Skipping report with missing ID.")
            continue
        try:
            collection.replace_one({"safetyreportid": rid}, report, upsert=True)
            inserted += 1
            if inserted % 1000 == 0:
                logging.info(f"Inserted {inserted} reports so far...")
        except errors.DocumentTooLarge:
            logging.warning(f"⚠️ Skipped oversized report {rid}")
            os.makedirs("reports/evaluation_results", exist_ok=True)
            with open("reports/evaluation_results/oversized_reports_skipped.json", "a") as f:
                f.write(json.dumps({"safetyreportid": rid}) + "\n")
            continue
        except errors.PyMongoError as e:
            logging.error(f"❌ Failed to insert report {rid}: {{e}}")

    logging.info(f"✅ Inserted or updated {{inserted}} reports.")

def main(uri, db_name, collection_name, json_path, limit):
    client = MongoClient(uri)
    db = client[db_name]
    logging.info(f"Connected to MongoDB database: {{db_name}}, collection: {{collection_name}}")
    reports = iterate_reports_ijson(json_path)
    insert_reports(db, collection_name, reports, limit=limit)
    client.close()
    logging.info("MongoDB connection closed.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--uri", default="mongodb://localhost:27017", help="MongoDB URI")
    parser.add_argument("--db", default="openfda_converted", help="Target MongoDB database name")
    parser.add_argument("--collection", default="full_reports", help="Target collection name")
    parser.add_argument("--json_path", default="data/raw/source_data", help="Path to JSON directory")
    parser.add_argument("--limit", type=int, default=None, help="Maximum number of reports to insert")
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging") # added for debugging
    args = parser.parse_args()

    # logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    main(args.uri, args.db, args.collection, args.json_path, args.limit)
