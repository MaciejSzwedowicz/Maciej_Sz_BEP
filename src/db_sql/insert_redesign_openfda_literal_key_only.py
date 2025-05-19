
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

def insert_with_fields(conn, table, fields, values):
    cursor = conn.cursor()
    placeholders = ", ".join([f":{field}" for field in fields])
    columns = ", ".join(fields)
    sql = f"INSERT OR IGNORE INTO {table} ({columns}) VALUES ({placeholders})"
    cursor.execute(sql, values)
    # if cursor.rowcount == 0:
    #     continue
        # logging.warning(f"⚠️ Ignored insert into {table}, likely due to duplicate or missing key. Data: {values}")
        



def insert_report_related(conn, report):
    rid = safe_int(report.get("safetyreportid"))
    sender = safe_get(report, "sender", {})
    receiver = safe_get(report, "receiver", {})
    primarysource = safe_get(report, "primarysource", {})
    patient = safe_get(report, "patient", {})

    report_data = {
        "safetyreportid": rid,
        "safetyreportversion": safe_int(report.get("safetyreportversion")),
        "receivedateformat": safe_int(report.get("receivedateformat")),
        "receivedate": normalize_date(report.get("receivedate"),report.get("receivedateformat")),
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
        "sendertype": safe_int(sender.get("sendertype")) if isinstance(sender, dict) else None,
        "senderorganization": sender.get("senderorganization") if isinstance(sender, dict) else None,
        "receivertype": safe_int(receiver.get("receivertype")) if isinstance(receiver, dict) else None,
        "receiverorganization": receiver.get("receiverorganization") if isinstance(receiver, dict) else None,
        "primarysource_qualification": safe_int(primarysource.get("qualification")) if isinstance(primarysource, dict) else None,
        "primarysource_reportercountry": primarysource.get("reportercountry") if isinstance(primarysource, dict) else None,
        "occurcountry": report.get("occurcountry"),
        "patientsex": safe_int(patient.get("patientsex")),
        "duplicate": safe_int(report.get("duplicate"))
    }
    insert_with_fields(conn, "report", list(report_data.keys()), report_data)

    literature = primarysource.get("literaturereference")
    if isinstance(literature, str):
        insert_with_fields(conn, "primarysource_literature_reference",
                       ["safetyreportid", "literature_reference"],
                       {"safetyreportid": rid, "literature_reference": literature})
    elif isinstance(literature, list):
        for ref in literature:
            insert_with_fields(conn, "primarysource_literature_reference",
                           ["safetyreportid", "literature_reference"],
                           {"safetyreportid": rid, "literature_reference": ref})
            
    if report.get("authoritynumb"):
                insert_with_fields(conn, "report_authority", ["safetyreportid", "authoritynumb"], {
                    "safetyreportid": rid,
                    "authoritynumb": report["authoritynumb"]
                })

def insert_patient_age(conn, report):
    patient = safe_get(report, "patient", {})
    if not isinstance(patient, dict): return
    data = {
        "safetyreportid": safe_int(report.get("safetyreportid")),
        "patientonsetage": safe_int(patient.get("patientonsetage")),
        "patientonsetageunit": safe_int(patient.get("patientonsetageunit")),
    }
    if "patientonsetage" in patient:
        insert_with_fields(conn, "patient_age", list(data.keys()), data)

def insert_patient_agegroup(conn, report):
    patient = safe_get(report, "patient", {})
    if not isinstance(patient, dict): return
    data = {
        "safetyreportid": safe_int(report.get("safetyreportid")),
        "patientagegroup": safe_int(patient.get("patientagegroup"))
    }
    if "patientagegroup" in patient:
        insert_with_fields(conn, "patient_age_group", list(data.keys()), data)

def insert_patient_weight(conn, report):
    patient = safe_get(report, "patient", {})
    if not isinstance(patient, dict): return
    data = {
        "safetyreportid": safe_int(report.get("safetyreportid")),
        "patientweight": safe_float(patient.get("patientweight"))
    }
    if "patientweight" in patient:
        insert_with_fields(conn, "patient_weight", list(data.keys()), data)


def insert_summary(conn, report):
    patient = report.get("patient", {})
    summary = patient.get("summary") if isinstance(patient, dict) else None

    if not isinstance(summary, dict):
        # logging.warning(f"⚠️ Skipping summary for report {report.get('safetyreportid')} — missing or malformed.")
        return

    narrative = summary.get("narrativeincludeclinical", "")
    extracted = extract_case_event_date(narrative)
    data = {
        "safetyreportid": safe_int(report.get("safetyreportid")),
        "narrativeincludeclinical": narrative,
        "case_event_date_extracted": extracted
    }
    insert_with_fields(conn, "summary", list(data.keys()), data)

def insert_reactions(conn, report):
    patient = safe_get(report, "patient", {})
    if not isinstance(patient, dict): return
    reactions = patient.get("reaction", [])
    if not isinstance(reactions, list): return
    for reaction in reactions:
        if not isinstance(reaction, dict): continue
        data = {
            "safetyreportid": safe_int(report.get("safetyreportid")),
            "reactionmeddrapt": reaction.get("reactionmeddrapt"),
            "reactionmeddraversionpt": safe_float(reaction.get("reactionmeddraversionpt")),
            "reactionoutcome": safe_int(reaction.get("reactionoutcome"))
        }
        insert_with_fields(conn, "reaction", list(data.keys()), data)

def insert_reportduplicates(conn, report):
    duplicates = report.get("reportduplicate", [])
    if not isinstance(duplicates, list): return
    for dup in duplicates:
        if not isinstance(dup, dict): continue
        data = {
            "safetyreportid": safe_int(report.get("safetyreportid")),
            "duplicatesource": dup.get("duplicatesource"),
            "duplicatenumb": dup.get("duplicatenumb")
        }
        insert_with_fields(conn, "report_duplicate", list(data.keys()), data)


# In-memory drug catalog deduplication
class DrugRegistry:
    def __init__(self):
        self.name_to_id = {}
        self.next_id = 1
        self.openfda_variants = defaultdict(set)
        self.variant_indices = defaultdict(int)

    def _serialize_openfda(self, openfda):
        return tuple(sorted((k, tuple(v) if isinstance(v, list) else v) for k, v in openfda.items() if v))


    def get_or_create(self, conn, drug, safetyreportid=None, drug_instance_index=None):
        if not drug.get("medicinalproduct"):
            # logging.warning(f"⚠️ Skipping drug with missing medicinalproduct: {drug}")
            return None

        key = drug.get("medicinalproduct")
        if key in self.name_to_id:
            return self.name_to_id[key]

        drug_id = self.next_id
        self.next_id += 1
        self.name_to_id[key] = drug_id

        insert_with_fields(conn, "drug_catalog", ["drug_id", "medicinalproduct"], {
            "drug_id": drug_id,
            "medicinalproduct": drug.get("medicinalproduct")
        })

        # Insert activesubstance(s)
        actives = drug.get("activesubstance")
        seen = set()
        if isinstance(actives, dict):
            name = actives.get("activesubstancename")
            if name:
                insert_with_fields(conn, "drug_activesubstance", ["drug_id", "activesubstancename"], {
                    "drug_id": drug_id, "activesubstancename": name})
        elif isinstance(actives, list):
            for a in actives:
                name = a.get("activesubstancename") if isinstance(a, dict) else None
                if name and name not in seen:
                    seen.add(name)
                    insert_with_fields(conn, "drug_activesubstance", ["drug_id", "activesubstancename"], {
                        "drug_id": drug_id, "activesubstancename": name})

        # Insert openfda metadata
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
            variant_index = None
        if isinstance(openfda, dict):
            variant_key = self._serialize_openfda(openfda)
            if variant_key not in self.openfda_variants[drug_id]:
                self.openfda_variants[drug_id].add(variant_key)
                self.variant_indices[drug_id] += 1
                variant_index = self.variant_indices[drug_id]
                field_data = {
                    f: ", ".join(openfda.get(f, [])) if isinstance(openfda.get(f), list) else openfda.get(f)
                    for f in [
                        "application_number", "brand_name", "generic_name", "manufacturer_name",
                        "nui", "package_ndc", "pharm_class_cs", "pharm_class_epc", "pharm_class_moa",
                        "pharm_class_pe", "product_ndc", "product_type", "route", "rxcui",
                        "spl_id", "spl_set_id", "substance_name", "unii"
                    ]
                }
                field_data["drug_id"] = drug_id
                field_data["variant_index"] = variant_index
                insert_with_fields(conn, "drug_openfda_variant", list(field_data.keys()), field_data)
            else:
                for idx, key in enumerate(self.openfda_variants[drug_id]):
                    if key == variant_key:
                        variant_index = idx
                        break
        if safetyreportid is not None and drug_instance_index is not None and variant_index is not None:
            insert_with_fields(conn, "drug_openfda_link",
                               ["safetyreportid", "drug_instance_index", "drug_id", "variant_index"],
                               {
                                   "safetyreportid": safetyreportid,
                                   "drug_instance_index": drug_instance_index,
                                   "drug_id": drug_id,
                                   "variant_index": variant_index
                               })

        return drug_id

def insert_drugs(conn, report, registry):
    
    patient = safe_get(report, "patient", {})
    rid = safe_int(report.get("safetyreportid"))
    for i, drug in enumerate(patient.get("drug", [])):
                if not isinstance(drug, dict): continue
                drug_id = registry.get_or_create(conn, drug, rid, i)
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
                insert_with_fields(conn, "patient_drug_history", list(base.keys()), base)

def main(db_path, json_path, limit):
    conn = sqlite3.connect(db_path)
    print(f"🔗 Connected to DB at: {db_path}")
    registry = DrugRegistry()
    inserted = 0
    for report in iterate_reports_ijson(json_path):
        try:
            insert_report_related(conn, report)
            insert_patient_age(conn, report)
            insert_patient_agegroup(conn, report)
            insert_patient_weight(conn, report)
            insert_summary(conn, report)
            insert_reactions(conn, report)
            insert_reportduplicates(conn, report)
            insert_drugs(conn, report, registry)


            inserted += 1
            if limit and inserted >= limit:
                break
            if inserted % 100 == 0:
                logging.info(f"Inserted {inserted} reports...")
        except Exception as e:
            logging.error(f"❌ Error on report {report.get('safetyreportid')}: {e}")
    conn.commit()
    conn.close()
    logging.info(f"✅ Finished. Inserted {inserted} reports.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default="sql/openfda_final_v2.db")
    parser.add_argument("--json_path", default="data/raw/source_data")
    parser.add_argument("--limit", type=int, default= 1000, help="Max number of reports to insert")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    main(args.db, args.json_path, args.limit)
