# Redesigned insertion script for new SQL schema (with full report structure)
import argparse
import logging
import os
import sqlite3
import sys
import re
from datetime import datetime
from collections import defaultdict

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from src.parser.iterate_reports import iterate_reports_ijson

# Reuse type-safe conversion helpers from original script
def safe_int(val):
    try: return int(val)
    except: return None

def safe_float(val):
    try: return float(val)
    except: return None

def normalize_date(date_str, fmt_code):
    if not date_str or not fmt_code:
        return None
    try:
        if fmt_code == "102" and len(date_str) == 8:
            return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"
        elif fmt_code == "610" and len(date_str) == 6:
            return f"{date_str[:4]}-{date_str[4:6]}-01"
        elif fmt_code == "602" and len(date_str) == 4:
            return f"{date_str}-01-01"
    except:
        return None
    return None

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

def safe_get(obj, key, default=None):
    return obj.get(key) if isinstance(obj, dict) else default

def insert_with_fields(cursor, table, fields, values):
    placeholders = ", ".join([f":{field}" for field in fields])
    columns = ", ".join(fields)
    sql = f"INSERT OR IGNORE INTO {table} ({columns}) VALUES ({placeholders})"
    cursor.execute(sql, values)

def normalize_scalar(val):
    if isinstance(val, str):
        return val.strip().lower()
    return str(val).strip().lower() if val is not None else ""

def normalize_field_for_key(val):
    if isinstance(val, list):
        return ", ".join(sorted(normalize_scalar(v) for v in val))
    return normalize_scalar(val)

# In-memory drug catalog deduplication
class DrugRegistry:
    def __init__(self):
        self.key_to_id = {}
        self.next_id = 1

    def get_or_create(self, cursor, drug):
        key = (
            normalize_scalar(drug.get("medicinalproduct")),
            tuple(sorted([
                normalize_scalar(a.get("activesubstancename"))
                for a in (drug.get("activesubstance") if isinstance(drug.get("activesubstance"), list) else [drug.get("activesubstance")])
                if a
            ])),
            normalize_scalar(drug.get("openfda", {}).get("application_number"))
        )
        if key in self.key_to_id:
            return self.key_to_id[key]

        drug_id = self.next_id
        self.next_id += 1
        self.key_to_id[key] = drug_id

        insert_with_fields(cursor, "drug_catalog", ["drug_id", "medicinalproduct"], {
            "drug_id": drug_id,
            "medicinalproduct": drug.get("medicinalproduct")
        })

        actives = drug.get("activesubstance")
        if isinstance(actives, dict):
            name = actives.get("activesubstancename")
            if name:
                insert_with_fields(cursor, "drug_activesubstance",
                    ["drug_id", "activesubstancename"], {"drug_id": drug_id, "activesubstancename": name})
        elif isinstance(actives, list):
            for a in actives:
                name = safe_get(a, "activesubstancename")
                if name:
                    insert_with_fields(cursor, "drug_activesubstance",
                        ["drug_id", "activesubstancename"], {"drug_id": drug_id, "activesubstancename": name})

        openfda = drug.get("openfda", {})
        if isinstance(openfda, dict):
            openfda_fields = [
                "application_number", "brand_name", "generic_name", "manufacturer_name",
                "nui", "package_ndc", "pharm_class_cs", "pharm_class_epc", "pharm_class_moa",
                "pharm_class_pe", "product_ndc", "product_type", "route", "rxcui",
                "spl_id", "spl_set_id", "substance_name", "unii"
            ]
            field_data = {
                f: ", ".join(openfda.get(f, [])) if isinstance(openfda.get(f), list) else openfda.get(f)
                for f in openfda_fields
            }
            if any(v for v in field_data.values()):
                field_data["drug_id"] = drug_id
                insert_with_fields(cursor, "drug_openfda", list(field_data.keys()), field_data)

        return drug_id

def main(db_path, json_path, limit):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    registry = DrugRegistry()
    inserted = 0

    for report in iterate_reports_ijson(json_path):
        try:
            rid = safe_int(report.get("safetyreportid"))
            if not rid:
                continue

            # Insert report
            insert_with_fields(cursor, "report", [
                "safetyreportid", "safetyreportversion", "receivedateformat", "receivedate", "receiptdateformat",
                "receiptdate", "transmissiondateformat", "transmissiondate", "companynumb", "reporttype",
                "fulfillexpeditecriteria", "serious", "seriousnessdeath", "seriousnesslifethreatening",
                "seriousnesshospitalization", "seriousnessdisabling", "seriousnesscongenitalanomali",
                "seriousnessother", "primarysourcecountry", "sendertype", "senderorganization",
                "receivertype", "receiverorganization", "primarysource_qualification",
                "primarysource_reportercountry", "occurcountry", "duplicate", "patientsex"
            ], {
                "safetyreportid": rid,
                "safetyreportversion": safe_int(report.get("safetyreportversion")),
                "receivedateformat": safe_int(report.get("receivedateformat")),
                "receivedate": normalize_date(report.get("receivedate"), report.get("receivedateformat")),
                "receiptdateformat": safe_int(report.get("receiptdateformat")),
                "receiptdate": normalize_date(report.get("receiptdate"), report.get("receiptdateformat")),
                "transmissiondateformat": safe_int(report.get("transmissiondateformat")),
                "transmissiondate": normalize_date(report.get("transmissiondate"), report.get("transmissiondateformat")),
                "companynumb": report.get("companynumb"),
                "reporttype": safe_int(report.get("reporttype")),
                "fulfillexpeditecriteria": safe_int(report.get("fulfillexpeditecriteria")),
                "serious": safe_int(report.get("serious")),
                "seriousnessdeath": safe_int(report.get("seriousnessdeath")),
                "seriousnesslifethreatening": safe_int(report.get("seriousnesslifethreatening")),
                "seriousnesshospitalization": safe_int(report.get("seriousnesshospitalization")),
                "seriousnessdisabling": safe_int(report.get("seriousnessdisabling")),
                "seriousnesscongenitalanomali": safe_int(report.get("seriousnesscongenitalanomali")),
                "seriousnessother": safe_int(report.get("seriousnessother")),
                "primarysourcecountry": report.get("primarysourcecountry"),
                "sendertype": safe_int(safe_get(report.get("sender"), "sendertype")),
                "senderorganization": safe_get(report.get("sender"), "senderorganization"),
                "receivertype": safe_int(safe_get(report.get("receiver"), "receivertype")),
                "receiverorganization": safe_get(report.get("receiver"), "receiverorganization"),
                "primarysource_qualification": safe_int(safe_get(report.get("primarysource"), "qualification")),
                "primarysource_reportercountry": safe_get(report.get("primarysource"), "reportercountry"),
                "occurcountry": report.get("occurcountry"),
                "duplicate": safe_int(report.get("duplicate")),
                "patientsex": safe_int(safe_get(report.get("patient", {}), "patientsex"))
            })

            # Optional: authoritynumb
            if report.get("authoritynumb"):
                insert_with_fields(cursor, "report_authority", ["safetyreportid", "authoritynumb"], {
                    "safetyreportid": rid,
                    "authoritynumb": report["authoritynumb"]
                })

            # Reaction(s)
            for reaction in safe_get(report.get("patient", {}), "reaction", []):
                insert_with_fields(cursor, "reaction", ["safetyreportid", "reactionmeddrapt", "reactionmeddraversionpt", "reactionoutcome"], {
                    "safetyreportid": rid,
                    "reactionmeddrapt": reaction.get("reactionmeddrapt"),
                    "reactionmeddraversionpt": safe_float(reaction.get("reactionmeddraversionpt")),
                    "reactionoutcome": safe_int(reaction.get("reactionoutcome"))
                })

            # Report duplicate
            for dup in report.get("reportduplicate", []):
                insert_with_fields(cursor, "report_duplicate", ["safetyreportid", "duplicatenumb", "duplicatesource"], {
                    "safetyreportid": rid,
                    "duplicatenumb": dup.get("duplicatenumb"),
                    "duplicatesource": dup.get("duplicatesource")
                })

            # Summary
            narrative = safe_get(report.get("summary"), "narrativeincludeclinical")
            if narrative:
                insert_with_fields(cursor, "summary", ["safetyreportid", "narrativeincludeclinical", "case_event_date_extracted"], {
                    "safetyreportid": rid,
                    "narrativeincludeclinical": narrative,
                    "case_event_date_extracted": extract_case_event_date(narrative)
                })

            # Literature references
            primarysource = report.get("primarysource")
            if isinstance(primarysource, dict):
                for ref in safe_get(safe_get(report.get("primarysource"), "literaturereference"), None, []):
                    insert_with_fields(cursor, "primarysource_literature_reference", ["safetyreportid", "literature_reference"], {
                        "safetyreportid": rid,
                        "literature_reference": ref
                    })

            # Patient sub-tables
            patient = safe_get(report, "patient", {})
            if "patientonsetage" in patient and "patientonsetageunit" in patient:
                insert_with_fields(cursor, "patient_age", ["safetyreportid", "patientonsetage", "patientonsetageunit"], {
                    "safetyreportid": rid,
                    "patientonsetage": safe_float(patient.get("patientonsetage")),
                    "patientonsetageunit": safe_int(patient.get("patientonsetageunit"))
                })
            if "patientagegroup" in patient:
                insert_with_fields(cursor, "patient_agegroup", ["safetyreportid", "patientagegroup"], {
                    "safetyreportid": rid,
                    "patientagegroup": safe_int(patient.get("patientagegroup"))
                })
            if "patientweight" in patient:
                insert_with_fields(cursor, "patient_weight", ["safetyreportid", "patientweight"], {
                    "safetyreportid": rid,
                    "patientweight": safe_float(patient.get("patientweight"))
                })

            # Drug block (unchanged)
            for i, drug in enumerate(patient.get("drug", [])):
                if not isinstance(drug, dict): continue
                drug_id = registry.get_or_create(cursor, drug)
                base = {
                    "safetyreportid": rid,
                    "drug_instance_index": i,
                    "drug_id": drug_id,
                    "drugauthorizationnumb": drug.get("drugauthorizationnumb"),
                    "drugcharacterization": safe_int(drug.get("drugcharacterization")),
                    "drugstartdate": normalize_date(drug.get("drugstartdate"), drug.get("drugstartdateformat")),
                    "drugenddate": normalize_date(drug.get("drugenddate"), drug.get("drugenddateformat")),
                    "drugindication": drug.get("drugindication"),
                    "actiondrug": safe_int(drug.get("actiondrug")),
                    "drugadministrationroute": safe_int(drug.get("drugadministrationroute")),
                    "drugdosagetext": drug.get("drugdosagetext"),
                    "drugstructuredosagenumb": safe_float(drug.get("drugstructuredosagenumb")),
                    "drugstructuredosageunit": safe_int(drug.get("drugstructuredosageunit")),
                    "drugseparatedosagenumb": safe_float(drug.get("drugseparatedosagenumb")),
                    "drugintervaldosagedefinition": safe_int(drug.get("drugintervaldosagedefinition")),
                    "drugintervaldosageunitnumb": safe_float(drug.get("drugintervaldosageunitnumb")),
                    "drugseparatedosageunit": drug.get("drugseparatedosageunit"),
                    "drugcumulativedosagenumb": safe_float(drug.get("drugcumulativedosagenumb")),
                    "drugcumulativedosageunit": safe_int(drug.get("drugcumulativedosageunit")),
                    "drugbatchnumb": drug.get("drugbatchnumb"),
                    "drugtreatmentduration": safe_float(drug.get("drugtreatmentduration")),
                    "drugtreatmentdurationunit": safe_int(drug.get("drugtreatmentdurationunit")),
                    "drugadditional": safe_int(drug.get("drugadditional"))
                }
                insert_with_fields(cursor, "patient_drug_history", list(base.keys()), base)

            inserted += 1
            if limit and inserted >= limit:
                break
            if inserted % 100 == 0:
                logging.info(f"Inserted data from {inserted} reports...")
        except Exception as e:
            logging.error(f"❌ Error on report {report.get('safetyreportid')}: {e}")

    conn.commit()
    conn.close()
    logging.info(f"✅ Finished. Full data from {inserted} reports inserted.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default="sql/openfda_base_redesigned.db")
    parser.add_argument("--json_path", default="data/raw/source_data")
    parser.add_argument("--limit", type=int, default=100, help="Max number of reports to insert")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    main(args.db, args.json_path, args.limit)
