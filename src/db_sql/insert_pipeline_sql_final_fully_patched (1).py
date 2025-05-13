
import argparse
import logging
import os
import sqlite3
import sys
import re
from datetime import datetime

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
    if cursor.rowcount == 0:
        logging.warning(f"⚠️ Ignored insert into {table}, likely due to duplicate or missing key. Data: {values}")


def insert_report_related(conn, report):
    rid = safe_int(report.get("safetyreportid"))
    sender = safe_get(report, "sender", {})
    receiver = safe_get(report, "receiver", {})
    primarysource = safe_get(report, "primarysource", {})

    report_data = {
        "safetyreportid": rid,
        "safetyreportversion": safe_int(report.get("safetyreportversion")),
        "receivedateformat": safe_int(report.get("receivedateformat")),
        "receivedate": report.get("receivedate"),
        "receiptdateformat": safe_int(report.get("receiptdateformat")),
        "receiptdate": report.get("receiptdate"),
        "transmissiondateformat": safe_int(report.get("transmissiondateformat")),
        "transmissiondate": report.get("transmissiondate"),
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
        "authoritynumb": report.get("authoritynumb"),
        "occurcountry": report.get("occurcountry"),
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




def insert_patient_optional(conn, report):
    patient = safe_get(report, "patient", {})
    if not isinstance(patient, dict): return
    data = {
        "safetyreportid": safe_int(report.get("safetyreportid")),
        "patientagegroup": safe_int(patient.get("patientagegroup")),
        "patientonsetage": safe_int(patient.get("patientonsetage")),
        "patientonsetageunit": safe_int(patient.get("patientonsetageunit")),
        "patientsex": safe_int(patient.get("patientsex")),
        "patientweight": safe_float(patient.get("patientweight"))
    }
    insert_with_fields(conn, "patient_optional", list(data.keys()), data)

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

def insert_drugs(conn, report):
    patient = safe_get(report, "patient", {})
    if not isinstance(patient, dict): return
    for drug in patient.get("drug", []):
        if not isinstance(drug, dict): continue
        base = {
            "safetyreportid": safe_int(report.get("safetyreportid")),
            "medicinalproduct": drug.get("medicinalproduct"),
            "drugcharacterization": safe_int(drug.get("drugcharacterization")),
            "drugauthorizationnumb": drug.get("drugauthorizationnumb"),
            "drugdosagetext": drug.get("drugdosagetext"),
            "drugstartdate": normalize_date(drug.get("drugstartdate"), drug.get("drugstartdateformat")),
            "drugstartdateformat": safe_int(drug.get("drugstartdateformat")),
            "drugenddate": normalize_date(drug.get("drugenddate"), drug.get("drugenddateformat")),
            "drugenddateformat": safe_int(drug.get("drugenddateformat")),
            "drugadministrationroute": safe_int(drug.get("drugadministrationroute")),
            "drugdosageform": drug.get("drugdosageform"),
            "drugindication": drug.get("drugindication")
        }
        insert_with_fields(conn, "drug", list(base.keys()), base)
        cursor = conn.cursor()
        cursor.execute("SELECT last_insert_rowid()")
        drug_id = cursor.fetchone()[0]

        optional = {
            "drug_id": drug_id,
            "actiondrug": safe_int(drug.get("actiondrug")),
            "drugadditional": safe_int(drug.get("drugadditional")),
            "drugbatchnumb": drug.get("drugbatchnumb"),
            "drugcumulativedosagenumb": safe_float(drug.get("drugcumulativedosagenumb")),
            "drugcumulativedosageunit": safe_int(drug.get("drugcumulativedosageunit")),
            "drugintervaldosagedefinition": safe_int(drug.get("drugintervaldosagedefinition")),
            "drugintervaldosageunitnumb": safe_float(drug.get("drugintervaldosageunitnumb")),
            "drugrecurreadministration": safe_int(drug.get("drugrecurreadministration")),
            "drugseparatedosagenumb": safe_float(drug.get("drugseparatedosagenumb")),
            "drugstructuredosagenumb": safe_float(drug.get("drugstructuredosagenumb")),
            "drugstructuredosageunit": safe_int(drug.get("drugstructuredosageunit")),
            "drugtreatmentduration": safe_float(drug.get("drugtreatmentduration")),
            "drugtreatmentdurationunit": safe_int(drug.get("drugtreatmentdurationunit"))
        }
        insert_with_fields(conn, "drug_optional", list(optional.keys()), optional)

        actives = drug.get("activesubstance")
        if isinstance(actives, dict):
            name = actives.get("activesubstancename")
            if name:
                insert_with_fields(conn, "drug_activesubstance", ["drug_id", "activesubstancename"], {
                    "drug_id": drug_id,
                    "activesubstancename": name
                })

        openfda = drug.get("openfda", {})
        if isinstance(openfda, dict):
            openfda_fields = [
                "application_number", "brand_name", "generic_name", "manufacturer_name",
                "nui", "package_ndc", "pharm_class_cs", "pharm_class_epc", "pharm_class_moa",
                "pharm_class_pe", "product_ndc", "product_type", "route", "rxcui",
                "spl_id", "spl_set_id", "substance_name", "unii"
            ]
            openfda_data = {
                "drug_id": drug_id,
                **{f: ", ".join(openfda.get(f, [])) if isinstance(openfda.get(f), list) else openfda.get(f) for f in openfda_fields}
            }
            insert_with_fields(conn, "drug_openfda", list(openfda_data.keys()), openfda_data)


def main(db_path, json_path):
    conn = sqlite3.connect(db_path)
    print(f"🔗 Connected to DB at: {db_path}")
    inserted = 0
    for report in iterate_reports_ijson(json_path):
        try:
            insert_report_related(conn, report)
            insert_patient_optional(conn, report)
            insert_summary(conn, report)
            insert_reactions(conn, report)
            insert_reportduplicates(conn, report)
            insert_drugs(conn, report)
            inserted += 1
            if inserted % 100 == 0:
                logging.info(f"Inserted {inserted} reports...")
        except Exception as e:
            logging.error(f"❌ Error on report {report.get('safetyreportid')}: {e}")
    conn.commit()
    conn.close()
    logging.info(f"✅ Finished. Inserted {inserted} reports.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default="sql/openfda_base_updated.db")
    parser.add_argument("--json_path", default="data/raw/source_data")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    main(args.db, args.json_path)
