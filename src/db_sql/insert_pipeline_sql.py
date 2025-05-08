
import argparse
import logging
import os
import sqlite3
import sys

# Add src to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from src.parser.iterate_reports import iterate_reports_ijson


def insert_report(conn, report):
    try:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO report (
                safetyreportid, safetyreportversion, receivedateformat, receivedate,
                receiptdateformat, receiptdate, transmissiondateformat, transmissiondate,
                companynumb, reporttype, fulfillexpeditecriteria, serious,
                seriousnessdeath, seriousnesslifethreatening, seriousnesshospitalization,
                seriousnessdisabling, seriousnesscongenitalanomali, seriousnessother,
                primarysourcecountry, sender_sendertype, sender_senderorganization,
                receiver_receivertype, receiver_receiverorganization, primarysource_qualification,
                primarysource_literaturereference, primarysource_reportercountry,
                authoritynumb, occurcountry, duplicate
            ) VALUES (
                :safetyreportid, :safetyreportversion, :receivedateformat, :receivedate,
                :receiptdateformat, :receiptdate, :transmissiondateformat, :transmissiondate,
                :companynumb, :reporttype, :fulfillexpeditecriteria, :serious,
                :seriousnessdeath, :seriousnesslifethreatening, :seriousnesshospitalization,
                :seriousnessdisabling, :seriousnesscongenitalanomali, :seriousnessother,
                :primarysourcecountry, :sender_sendertype, :sender_senderorganization,
                :receiver_receivertype, :receiver_receiverorganization, :primarysource_qualification,
                :primarysource_literaturereference, :primarysource_reportercountry,
                :authoritynumb, :occurcountry, :duplicate
            );
        """, report)
        conn.commit()
        return True
    except sqlite3.IntegrityError as e:
        logging.warning(f"Duplicate or constraint error: {e}")
        return False
    except Exception as e:
        logging.error(f"Error inserting report {report.get('safetyreportid')}: {e}")
        return False


def insert_reports_sqlite(db_path, json_path):
    conn = sqlite3.connect(db_path)
    total_inserted = 0

    for i, report in enumerate(iterate_reports_ijson(json_path), 1):
        flat_report = {}
        try:
            flat_report["safetyreportid"] = report["safetyreportid"]
            flat_report["safetyreportversion"] = report.get("safetyreportversion")
            flat_report["receivedateformat"] = report.get("receivedateformat")
            flat_report["receivedate"] = report.get("receivedate")
            flat_report["receiptdateformat"] = report.get("receiptdateformat")
            flat_report["receiptdate"] = report.get("receiptdate")
            flat_report["transmissiondateformat"] = report.get("transmissiondateformat")
            flat_report["transmissiondate"] = report.get("transmissiondate")
            flat_report["companynumb"] = report.get("companynumb")
            flat_report["reporttype"] = report.get("reporttype")
            flat_report["fulfillexpeditecriteria"] = report.get("fulfillexpeditecriteria")
            flat_report["serious"] = report.get("serious")
            flat_report["seriousnessdeath"] = report.get("seriousnessdeath")
            flat_report["seriousnesslifethreatening"] = report.get("seriousnesslifethreatening")
            flat_report["seriousnesshospitalization"] = report.get("seriousnesshospitalization")
            flat_report["seriousnessdisabling"] = report.get("seriousnessdisabling")
            flat_report["seriousnesscongenitalanomali"] = report.get("seriousnesscongenitalanomali")
            flat_report["seriousnessother"] = report.get("seriousnessother")
            flat_report["primarysourcecountry"] = report.get("primarysourcecountry")
            flat_report["sender_sendertype"] = report.get("sender", {}).get("sendertype")
            flat_report["sender_senderorganization"] = report.get("sender", {}).get("senderorganization")
            flat_report["receiver_receivertype"] = report.get("receiver", {}).get("receivertype")
            flat_report["receiver_receiverorganization"] = report.get("receiver", {}).get("receiverorganization")
            flat_report["primarysource_qualification"] = report.get("primarysource", {}).get("qualification")
            flat_report["primarysource_literaturereference"] = report.get("primarysource", {}).get("literaturereference")
            flat_report["primarysource_reportercountry"] = report.get("primarysource", {}).get("reportercountry")
            flat_report["authoritynumb"] = report.get("authoritynumb")
            flat_report["occurcountry"] = report.get("occurcountry")
            flat_report["duplicate"] = report.get("duplicate")

            success = insert_report(conn, flat_report)
            if success:
                total_inserted += 1
                if total_inserted % 100 == 0:
                    logging.info(f"Inserted {total_inserted} reports...")

        except Exception as e:
            logging.error(f"Failed processing report at index {i}: {e}")

    logging.info(f"✅ Finished. Total inserted: {total_inserted}")
    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Insert OpenFDA reports into SQLite.")
    parser.add_argument("--db", default="openfda_base.db", help="SQLite database file")
    parser.add_argument("--json_path", default="data/raw/source_data", help="Path to OpenFDA JSON source data")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    insert_reports_sqlite(args.db, args.json_path)
