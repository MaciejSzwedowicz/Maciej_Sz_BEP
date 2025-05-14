
""" 
Updated MongoDB pipeline:
- Converts numeric-looking strings
- Normalizes date fields
- Extracts case_event_date_extracted
- Inserts into `openfda_converted.full_reports`
"""

import argparse
import os
import sys
import logging
import re
from datetime import datetime
from pymongo import MongoClient, errors
import json

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from src.parser.iterate_reports import iterate_reports_ijson

# Type-safe conversion helpers
def safe_int(val):
    try: return int(val)
    except: return val

def safe_float(val):
    try: return float(val)
    except: return val

def normalize_date(date_str, fmt_code):
    if not date_str or not fmt_code:
        return date_str
    try:
        if fmt_code == "102" and len(date_str) == 8:
            return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"
        elif fmt_code == "610" and len(date_str) == 6:
            return f"{date_str[:4]}-{date_str[4:6]}-01"
        elif fmt_code == "602" and len(date_str) == 4:
            return f"{date_str}-01-01"
    except:
        return date_str
    return date_str

def extract_case_event_date(text):
    match = re.search(r'CASE EVENT DATE[:\s]*?(\d{8})', str(text))
    if match:
        try:
            raw_date = match.group(1)
            datetime.strptime(raw_date, "%Y%m%d")
            return f"{raw_date[:4]}-{raw_date[4:6]}-{raw_date[6:]}"
        except ValueError:
            return None
    return None


def transform_report(report):
    def set_nested_safe(d, keys, converter):
        obj = d
        for k in keys[:-1]:
            obj = obj.get(k, {})
            if not isinstance(obj, dict):
                return
        if keys[-1] in obj:
            obj[keys[-1]] = converter(obj[keys[-1]])

    # Normalize drug date fields
    if isinstance(report.get("patient"), dict):
        for drug in report["patient"].get("drug", []):
            if isinstance(drug, dict):
                drug["drugstartdate"] = normalize_date(drug.get("drugstartdate"), drug.get("drugstartdateformat"))
                drug["drugenddate"] = normalize_date(drug.get("drugenddate"), drug.get("drugenddateformat"))

    # Extract narrative date
    try:
        summary = report.get("patient", {}).get("summary", {})
        if isinstance(summary, dict):
            narrative = summary.get("narrativeincludeclinical", "")
            extracted_date = extract_case_event_date(narrative)
            if extracted_date:
                summary["case_event_date_extracted"] = extracted_date
    except Exception as e:
        logging.warning(f"Could not extract date from narrative for report {report.get('safetyreportid')}: {e}")

    # Top-level integer fields
    if "safetyreportversion" in report:
        report["safetyreportversion"] = safe_int(report["safetyreportversion"])
    if "receivedateformat" in report:
        report["receivedateformat"] = safe_int(report["receivedateformat"])
    if "receiptdateformat" in report:
        report["receiptdateformat"] = safe_int(report["receiptdateformat"])
    if "transmissiondateformat" in report:
        report["transmissiondateformat"] = safe_int(report["transmissiondateformat"])
    if "reporttype" in report:
        report["reporttype"] = safe_int(report["reporttype"])
    if "fulfillexpeditecriteria" in report:
        report["fulfillexpeditecriteria"] = safe_int(report["fulfillexpeditecriteria"])
    if "serious" in report:
        report["serious"] = safe_int(report["serious"])
    if "seriousnessdeath" in report:
        report["seriousnessdeath"] = safe_int(report["seriousnessdeath"])
    if "seriousnesslifethreatening" in report:
        report["seriousnesslifethreatening"] = safe_int(report["seriousnesslifethreatening"])
    if "seriousnesshospitalization" in report:
        report["seriousnesshospitalization"] = safe_int(report["seriousnesshospitalization"])
    if "seriousnessdisabling" in report:
        report["seriousnessdisabling"] = safe_int(report["seriousnessdisabling"])
    if "seriousnesscongenitalanomali" in report:
        report["seriousnesscongenitalanomali"] = safe_int(report["seriousnesscongenitalanomali"])
    if "seriousnessother" in report:
        report["seriousnessother"] = safe_int(report["seriousnessother"])
    if "sender_sendertype" in report:
        report["sender_sendertype"] = safe_int(report["sender_sendertype"])
    if "receiver_receivertype" in report:
        report["receiver_receivertype"] = safe_int(report["receiver_receivertype"])
    if "primarysource_qualification" in report:
        report["primarysource_qualification"] = safe_int(report["primarysource_qualification"])
    if "duplicate" in report:
        report["duplicate"] = safe_int(report["duplicate"])

    # Nested integer fields
    set_nested_safe(report, ['patient', 'patientagegroup'], safe_int)
    set_nested_safe(report, ['patient', 'patientonsetage'], safe_int)
    set_nested_safe(report, ['patient', 'patientonsetageunit'], safe_int)
    set_nested_safe(report, ['patient', 'patientsex'], safe_int)
    set_nested_safe(report, ['reaction', 'reactionoutcome'], safe_int)
    set_nested_safe(report, ['drug', 'drugcharacterization'], safe_int)
    set_nested_safe(report, ['drug', 'drugauthorizationnumb'], safe_int)
    set_nested_safe(report, ['drug', 'drugadministrationroute'], safe_int)
    set_nested_safe(report, ['drug', 'drugenddateformat'], safe_int)
    set_nested_safe(report, ['drug', 'drugstartdateformat'], safe_int)
    set_nested_safe(report, ['drug_optional', 'actiondrug'], safe_int)
    set_nested_safe(report, ['drug_optional', 'drugadditional'], safe_int)
    set_nested_safe(report, ['drug_optional', 'drugintervaldosagedefinition'], safe_int)
    set_nested_safe(report, ['drug_optional', 'drugcumulativedosageunit'], safe_int)
    set_nested_safe(report, ['drug_optional', 'drugrecurreadministration'], safe_int)
    set_nested_safe(report, ['drug_optional', 'drugstructuredosageunit'], safe_int)
    set_nested_safe(report, ['drug_optional', 'drugtreatmentdurationunit'], safe_int)

    # Nested float fields
    set_nested_safe(report, ['patient', 'patientweight'], safe_float)
    set_nested_safe(report, ['reaction', 'reactionmeddraversionpt'], safe_float)
    set_nested_safe(report, ['drug_optional', 'drugcumulativedosagenumb'], safe_float)
    set_nested_safe(report, ['drug_optional', 'drugintervaldosageunitnumb'], safe_float)
    set_nested_safe(report, ['drug_optional', 'drugseparatedosagenumb'], safe_float)
    set_nested_safe(report, ['drug_optional', 'drugstructuredosagenumb'], safe_float)
    set_nested_safe(report, ['drug_optional', 'drugtreatmentduration'], safe_float)

    return report

def insert_reports(db, collection_name, reports):
    collection = db[collection_name]
    inserted = 0
    for report in reports:
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
            logging.error(f"❌ Failed to insert report {rid}: {e}")
    logging.info(f"✅ Inserted or updated {inserted} reports.")


def main(uri, db_name, collection_name, json_path):
    client = MongoClient(uri)
    db = client[db_name]
    logging.info(f"Connected to MongoDB database: {db_name}, collection: {collection_name}")
    reports = iterate_reports_ijson(json_path)
    insert_reports(db, collection_name, reports)
    client.close()
    logging.info("MongoDB connection closed.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--uri", default="mongodb://localhost:27017", help="MongoDB URI")
    parser.add_argument("--db", default="openfda_converted", help="Target MongoDB database name")
    parser.add_argument("--collection", default="full_reports", help="Target MongoDB collection name")
    parser.add_argument("--json_path", default="data/raw/source_data", help="Path to JSON directory")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    main(args.uri, args.db, args.collection, args.json_path)

