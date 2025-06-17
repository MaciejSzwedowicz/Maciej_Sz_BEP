"""
Final schema creation script mirroring working reference style with full coverage.
"""

import sqlite3

def create_tables(conn):
    with conn:
        conn.executescript("""

CREATE TABLE report (
    safetyreportid TEXT PRIMARY KEY,
    safetyreportversion TEXT,
    receivedateformat TEXT,
    receivedate TEXT,
    receiptdateformat TEXT,
    receiptdate TEXT,
    transmissiondateformat TEXT,
    transmissiondate TEXT,
    companynumb TEXT,
    reporttype TEXT,
    fulfillexpeditecriteria TEXT,
    serious TEXT,
    seriousnessdeath TEXT,
    seriousnesslifethreatening TEXT,
    seriousnesshospitalization TEXT,
    seriousnessdisabling TEXT,
    seriousnesscongenitalanomali TEXT,
    seriousnessother TEXT,
    primarysourcecountry TEXT,
    sender_sendertype TEXT,
    sender_senderorganization TEXT,
    receiver_receivertype TEXT,
    receiver_receiverorganization TEXT,
    primarysource_qualification TEXT,
    primarysource_literaturereference TEXT,
    primarysource_reportercountry TEXT,
    authoritynumb TEXT,
    occurcountry TEXT,
    duplicate TEXT
);


CREATE TABLE reaction (
    id INTEGER PRIMARY KEY,
    safetyreportid TEXT,
    reactionmeddrapt TEXT,
    reactionmeddraversionpt TEXT,
    reactionoutcome TEXT,
    FOREIGN KEY (safetyreportid) REFERENCES report(safetyreportid)
);


CREATE TABLE reportduplicate (
    id INTEGER PRIMARY KEY,
    safetyreportid TEXT,
    duplicatenumb TEXT,
    duplicatesource TEXT,
    FOREIGN KEY (safetyreportid) REFERENCES report(safetyreportid)
);


CREATE TABLE patient_optional (
    safetyreportid TEXT PRIMARY KEY,
    patientagegroup TEXT,
    patientonsetage TEXT,
    patientonsetageunit TEXT,
    patientsex TEXT,
    patientweight TEXT,
    narrativeincludeclinical TEXT,
    FOREIGN KEY (safetyreportid) REFERENCES report(safetyreportid)
);


CREATE TABLE drug (
    id INTEGER PRIMARY KEY,
    safetyreportid TEXT,
    medicinalproduct TEXT,
    drugcharacterization TEXT,
    drugauthorizationnumb TEXT,
    drugdosagetext TEXT,
    drugstartdate TEXT,
    drugenddate TEXT,
    drugadministrationroute TEXT,
    drugdosageform TEXT,
    drugindication TEXT,
    FOREIGN KEY (safetyreportid) REFERENCES report(safetyreportid)
);

CREATE TABLE drug_openfda (
    id INTEGER PRIMARY KEY,
    drug_id INTEGER,
    application_number TEXT,
    brand_name TEXT,
    generic_name TEXT,
    manufacturer_name TEXT,
    nui TEXT,
    package_ndc TEXT,
    pharm_class_cs TEXT,
    pharm_class_epc TEXT,
    pharm_class_moa TEXT,
    pharm_class_pe TEXT,
    product_ndc TEXT,
    product_type TEXT,
    route TEXT,
    rxcui TEXT,
    spl_id TEXT,
    spl_set_id TEXT,
    substance_name TEXT,
    unii TEXT,
    FOREIGN KEY (drug_id) REFERENCES drug(id)
);

CREATE TABLE drug_activesubstance (
    id INTEGER PRIMARY KEY,
    drug_id INTEGER,
    activesubstancename TEXT,
    FOREIGN KEY (drug_id) REFERENCES drug(id)
);


CREATE TABLE drug_optional (
    id INTEGER PRIMARY KEY,
    drug_id INTEGER,
    actiondrug TEXT,
    drugadditional TEXT,
    drugbatchnumb TEXT,
    drugcumulativedosagenumb TEXT,
    drugcumulativedosageunit TEXT,
    drugenddateformat TEXT,
    drugintervaldosagedefinition TEXT,
    drugintervaldosageunitnumb TEXT,
    drugrecurreadministration TEXT,
    drugseparatedosagenumb TEXT,
    drugstartdateformat TEXT,
    drugstructuredosagenumb TEXT,
    drugstructuredosageunit TEXT,
    drugtreatmentduration TEXT,
    drugtreatmentdurationunit TEXT,
    FOREIGN KEY (drug_id) REFERENCES drug(id)
);


CREATE TABLE summary (
    id INTEGER PRIMARY KEY,
    safetyreportid TEXT,
    narrativeincludeclinical TEXT,
    FOREIGN KEY (safetyreportid) REFERENCES report(safetyreportid)
);

        """)

if __name__ == "__main__":
    db_path = "sql/openfda_base.db"
    conn = sqlite3.connect(db_path)
    create_tables(conn)
    print("✅ Tables created successfully in", db_path)
    conn.close()
