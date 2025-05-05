
import os
import glob
import sqlite3
import ijson
import time

def iterate_reports_ijson(path):
    def yield_file(file_path):
        with open(file_path, 'rb') as f:
            parser = ijson.items(f, 'results.item')
            for report in parser:
                yield report
    if os.path.isdir(path):
        for file_path in sorted(glob.glob(os.path.join(path, '*.json'))):
            yield from yield_file(file_path)
    else:
        yield from yield_file(path)

# Paths
db_path = r"C:\Users\macie\OneDrive\Documents\Edukacja\YEAR 3\SM2\BEP\OpenFDA\notebooks\SQL_DB\OpenFDA_sample.db"
data_path = r"C:\Users\macie\OneDrive\Documents\Edukacja\YEAR 3\SM2\BEP\OpenFDA\Data_Q1_2024"

# Start timer
start_time = time.time()

# Connect to DB
conn = sqlite3.connect(db_path)
cur = conn.cursor()

# Table creation assumed to be already executed in a schema script

for report in iterate_reports_ijson(data_path):
    srid = report.get("safetyreportid")
    srver = report.get("safetyreportversion")
    recv_date = report.get("receivedate")
    recp_date = report.get("receiptdate")
    trans_date = report.get("transmissiondate")
    reporttype = report.get("reporttype")
    occurcountry = report.get("occurcountry")
    fulfil = report.get("fulfillexpeditecriteria")
    serious = report.get("serious")
    sd = report.get("seriousnessdeath")
    slt = report.get("seriousnesslifethreatening")
    sh = report.get("seriousnesshospitalization")
    sdg = report.get("seriousnessdisabling")
    sca = report.get("seriousnesscongenitalanomali")
    so = report.get("seriousnessother")
    authnumb = report.get("authoritynumb")
    companynumb = report.get("companynumb")
    duplicate = report.get("duplicate")

    cur.execute("INSERT OR IGNORE INTO safety_reports VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (srid, srver, reporttype, serious, sd, slt, sh, sdg, sca, so, recv_date, recp_date, trans_date, occurcountry, fulfil, authnumb))

    cur.execute("INSERT INTO report_metadata (safetyreportid, companynumb, duplicate) VALUES (?, ?, ?)",
                (srid, companynumb, str(duplicate)))

    patient = report.get("patient", {})

    cur.execute("INSERT INTO patients (safetyreportid, patientsex, patientweight, patientonsetage, patientonsetageunit, patientagegroup) VALUES (?, ?, ?, ?, ?, ?)",
                (srid, patient.get("patientsex"), patient.get("patientweight"),
                 patient.get("patientonsetage"), patient.get("patientonsetageunit"), patient.get("patientagegroup")))

    pid = cur.lastrowid

    death = patient.get("patientdeath", {})
    if death:
        cur.execute("INSERT INTO patient_deaths (patient_id, dateofdeath, dateofdeathformat, autopsyperformed) VALUES (?, ?, ?, ?)",
                    (pid, death.get("dateofdeath"), death.get("dateofdeathformat"), death.get("autopsyperformed")))

    for rxn in patient.get("reaction", []):
        cur.execute("INSERT INTO reactions (safetyreportid, reactionmeddrapt, reactionoutcome, reactionmeddraversionpt) VALUES (?, ?, ?, ?)",
                    (srid, rxn.get("reactionmeddrapt"), rxn.get("reactionoutcome"), rxn.get("reactionmeddraversionpt")))

    for drug in patient.get("drug", []):
        cur.execute("""
            INSERT INTO drugs (safetyreportid, medicinalproduct, drugauthorizationnumb, drugcharacterization,
                               drugdosagetext, drugadministrationroute, drugstartdate, drugenddate, drugindication,
                               actiondrug, drugrecurreadministration, drugadditional)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            srid,
            drug.get("medicinalproduct"),
            drug.get("drugauthorizationnumb"),
            drug.get("drugcharacterization"),
            drug.get("drugdosagetext"),
            drug.get("drugadministrationroute"),
            drug.get("drugstartdate"),
            drug.get("drugenddate"),
            drug.get("drugindication"),
            drug.get("actiondrug"),
            drug.get("drugrecurreadministration"),
            drug.get("drugadditional")
        ))

        drug_id = cur.lastrowid

        # Extended dosage details
        cur.execute("""
            INSERT INTO drug_dosage_details (drug_id, drugbatchnumb, drugdosageform, drugenddateformat,
                                             drugintervaldosagedefinition, drugintervaldosageunitnumb,
                                             drugseparatedosagenumb, drugstartdateformat, drugstructuredosagenumb,
                                             drugstructuredosageunit, drugtreatmentduration, drugtreatmentdurationunit,
                                             drugcumulativedosagenumb, drugcumulativedosageunit)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            drug_id,
            drug.get("drugbatchnumb"),
            drug.get("drugdosageform"),
            drug.get("drugenddateformat"),
            drug.get("drugintervaldosagedefinition"),
            drug.get("drugintervaldosageunitnumb"),
            drug.get("drugseparatedosagenumb"),
            drug.get("drugstartdateformat"),
            drug.get("drugstructuredosagenumb"),
            drug.get("drugstructuredosageunit"),
            drug.get("drugtreatmentduration"),
            drug.get("drugtreatmentdurationunit"),
            drug.get("drugcumulativedosagenumb"),
            drug.get("drugcumulativedosageunit")
        ))

        actives = drug.get("activesubstance", {})
        if actives:
            name = actives.get("activesubstancename")
            if name:
                cur.execute("INSERT INTO activesubstances (drug_id, activesubstancename) VALUES (?, ?)", (drug_id, name))

        fda = drug.get("openfda", {})
        if fda:
            cur.execute("""
                INSERT INTO openfda (drug_id, application_number, brand_name, generic_name, manufacturer_name,
                                     route, product_type, substance_name, rxcui, unii)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                drug_id,
                fda.get("application_number", [None])[0],
                fda.get("brand_name", [None])[0],
                fda.get("generic_name", [None])[0],
                fda.get("manufacturer_name", [None])[0],
                fda.get("route", [None])[0],
                fda.get("product_type", [None])[0],
                fda.get("substance_name", [None])[0],
                fda.get("rxcui", [None])[0],
                fda.get("unii", [None])[0]
            ))

            cur.execute("""
                INSERT INTO openfda_metadata_extended (openfda_id, nui, package_ndc, pharm_class_cs, pharm_class_epc,
                                                       pharm_class_moa, pharm_class_pe, product_ndc, spl_id, spl_set_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                cur.lastrowid,
                fda.get("nui", [None])[0],
                fda.get("package_ndc", [None])[0],
                fda.get("pharm_class_cs", [None])[0],
                fda.get("pharm_class_epc", [None])[0],
                fda.get("pharm_class_moa", [None])[0],
                fda.get("pharm_class_pe", [None])[0],
                fda.get("product_ndc", [None])[0],
                fda.get("spl_id", [None])[0],
                fda.get("spl_set_id", [None])[0]
            ))

conn.commit()
conn.close()

# End timer
end_time = time.time()
print(f"✅ Full schema data insertion completed in {end_time - start_time:.2f} seconds.")
