"""
Safe SQLite insertion pipeline for OpenFDA, with type checks on nested structures.
"""
import argparse
import logging
import os
import sqlite3
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from src.parser.iterate_reports import iterate_reports_ijson


def insert_with_fields(conn, table, fields, values):
    cursor = conn.cursor()
    placeholders = ", ".join([f":{field}" for field in fields])
    columns = ", ".join(fields)
    sql = f"INSERT OR IGNORE INTO {table} ({columns}) VALUES ({placeholders})"
    
    cursor.execute(sql, values)
    
    # DEBUG: Check if row was inserted
    if cursor.rowcount == 0:
        logging.warning(f"⚠️ Ignored insert into {table}, likely due to duplicate or missing key. Data: {values}")



def insert_report_related(conn, report):
    assert report.get("safetyreportid") is not None, "Missing safetyreportid"
    report_data = {k: report.get(k) for k in [
        "safetyreportid", "safetyreportversion", "receivedateformat", "receivedate",
        "receiptdateformat", "receiptdate", "transmissiondateformat", "transmissiondate",
        "companynumb", "reporttype", "fulfillexpeditecriteria", "serious",
        "seriousnessdeath", "seriousnesslifethreatening", "seriousnesshospitalization",
        "seriousnessdisabling", "seriousnesscongenitalanomali", "seriousnessother",
        "primarysourcecountry", "authoritynumb", "occurcountry", "duplicate"
    ]}
    report_data["sender_sendertype"] = report.get("sender", {}).get("sendertype") if isinstance(report.get("sender"), dict) else None
    report_data["sender_senderorganization"] = report.get("sender", {}).get("senderorganization") if isinstance(report.get("sender"), dict) else None
    report_data["receiver_receivertype"] = report.get("receiver", {}).get("receivertype") if isinstance(report.get("receiver"), dict) else None
    report_data["receiver_receiverorganization"] = report.get("receiver", {}).get("receiverorganization") if isinstance(report.get("receiver"), dict) else None
    report_data["primarysource_qualification"] = report.get("primarysource", {}).get("qualification") if isinstance(report.get("primarysource"), dict) else None
    report_data["primarysource_literaturereference"] = report.get("primarysource", {}).get("literaturereference") if isinstance(report.get("primarysource"), dict) else None
    report_data["primarysource_reportercountry"] = report.get("primarysource", {}).get("reportercountry") if isinstance(report.get("primarysource"), dict) else None
    insert_with_fields(conn, "report", list(report_data.keys()), report_data)


def insert_patient_optional(conn, report):
    patient = report.get("patient", {})
    if isinstance(patient, dict):
        fields = [
            "safetyreportid", "patientagegroup", "patientonsetage", "patientonsetageunit",
            "patientsex", "patientweight"
        ]
        data = {k: patient.get(k) for k in fields[1:]}
        data["safetyreportid"] = report["safetyreportid"]
        insert_with_fields(conn, "patient_optional", fields, data)


def insert_summary(conn, report):
    val = report.get("patient", {}).get("summary", {}).get("narrativeincludeclinical") if isinstance(report.get("patient", {}).get("summary"), dict) else None
    if val:
        insert_with_fields(conn, "summary", ["safetyreportid", "narrativeincludeclinical"], {
            "safetyreportid": report["safetyreportid"],
            "narrativeincludeclinical": val
        })


def insert_reactions(conn, report):
    reactions = report.get("patient", {}).get("reaction", [])
    if isinstance(reactions, list):
        for reaction in reactions:
            if isinstance(reaction, dict):
                fields = [
                    "safetyreportid", "reactionmeddrapt", "reactionmeddraversionpt", "reactionoutcome"
                ]
                data = {
                    "safetyreportid": report["safetyreportid"],
                    "reactionmeddrapt": reaction.get("reactionmeddrapt"),
                    "reactionmeddraversionpt": reaction.get("reactionmeddraversionpt"),
                    "reactionoutcome": reaction.get("reactionoutcome")
                }
                insert_with_fields(conn, "reaction", fields, data)


def insert_reportduplicates(conn, report):
    duplicates = report.get("reportduplicate", [])
    if isinstance(duplicates, list):
        for dup in duplicates:
            if isinstance(dup, dict):
                fields = ["safetyreportid", "duplicatesource", "duplicatenumb"]
                data = {
                    "safetyreportid": report["safetyreportid"],
                    "duplicatesource": dup.get("duplicatesource"),
                    "duplicatenumb": dup.get("duplicatenumb")
                }
                insert_with_fields(conn, "reportduplicate", fields, data)


def insert_drugs(conn, report):
    drugs = report.get("patient", {}).get("drug", [])
    if isinstance(drugs, list):
        for drug in drugs:
            if not isinstance(drug, dict):
                continue
            base_fields = [
                "safetyreportid", "medicinalproduct", "drugcharacterization", "drugauthorizationnumb",
                "drugdosagetext", "drugstartdate", "drugenddate", "drugadministrationroute",
                "drugdosageform", "drugindication"
            ]
            base_data = {f: drug.get(f) for f in base_fields[1:]}
            base_data["safetyreportid"] = report["safetyreportid"]
            cursor = conn.cursor()
            placeholders = ", ".join([f":{f}" for f in base_fields])
            cursor.execute(f"INSERT OR IGNORE INTO drug ({', '.join(base_fields)}) VALUES ({placeholders})", base_data)
            drug_id = cursor.lastrowid

            openfda = drug.get("openfda", {})
            if isinstance(openfda, dict):
                openfda_fields = [
                    "application_number", "brand_name", "generic_name", "manufacturer_name",
                    "nui", "package_ndc", "pharm_class_cs", "pharm_class_epc", "pharm_class_moa",
                    "pharm_class_pe", "product_ndc", "product_type", "route", "rxcui",
                    "spl_id", "spl_set_id", "substance_name", "unii"
                ]
                data = {f: ", ".join(openfda.get(f, [])) if isinstance(openfda.get(f), list) else openfda.get(f) for f in openfda_fields}
                data["drug_id"] = drug_id
                insert_with_fields(conn, "drug_openfda", ["drug_id"] + openfda_fields, data)

            optional_fields = [
                "actiondrug", "drugadditional", "drugbatchnumb", "drugcumulativedosagenumb",
                "drugcumulativedosageunit", "drugenddateformat", "drugintervaldosagedefinition",
                "drugintervaldosageunitnumb", "drugrecurreadministration", "drugseparatedosagenumb",
                "drugstartdateformat", "drugstructuredosagenumb", "drugstructuredosageunit",
                "drugtreatmentduration", "drugtreatmentdurationunit"
            ]
            optional_data = {f: drug.get(f) for f in optional_fields}
            optional_data["drug_id"] = drug_id
            insert_with_fields(conn, "drug_optional", ["drug_id"] + optional_fields, optional_data)

            actives = drug.get("activesubstance", [])
            if isinstance(actives, list):
                for active in actives:
                    if isinstance(active, dict):
                        data = {
                            "drug_id": drug_id,
                            "activesubstancename": active.get("activesubstancename")
                        }
                        insert_with_fields(conn, "drug_activesubstance", ["drug_id", "activesubstancename"], data)


def main(db_path, json_path):
    conn = sqlite3.connect(db_path)
    print(f"🔗 Connected to DB at: {db_path}")
    inserted = 0
    print("🧪 Starting iteration over reports...")
    for report in iterate_reports_ijson(json_path):
        # print("⚙️ Report:", report.get("safetyreportid"))
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
    parser.add_argument("--db", default="sql/openfda_base.db")
    parser.add_argument("--json_path", default="data/raw/source_data")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    main(args.db, args.json_path)


"""

One-liner to run the script from the command line:
```bash
python src/db_sql/insert_pipeline_sql.py --db sql/openfda_base.db      
"""