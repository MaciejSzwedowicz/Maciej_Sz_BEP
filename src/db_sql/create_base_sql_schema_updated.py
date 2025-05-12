
""" 
Updated schema creation script with:
- Typed fields (INTEGER, REAL)
- drugstartdateformat and drugenddateformat moved into drug table
- case_event_date_extracted added to summary table
"""

import sqlite3

def create_tables(conn):
    with conn:
        conn.executescript("""

CREATE TABLE report (
    safetyreportid INTEGER PRIMARY KEY,
    safetyreportversion INTEGER,
    receivedateformat INTEGER,
    receivedate TEXT,
    receiptdateformat INTEGER,
    receiptdate TEXT,
    transmissiondateformat INTEGER,
    transmissiondate TEXT,
    companynumb TEXT,
    reporttype INTEGER,
    fulfillexpeditecriteria INTEGER,
    serious INTEGER,
    seriousnessdeath INTEGER,
    seriousnesslifethreatening INTEGER,
    seriousnesshospitalization INTEGER,
    seriousnessdisabling INTEGER,
    seriousnesscongenitalanomali INTEGER,
    seriousnessother INTEGER,
    primarysourcecountry TEXT,
    sendertype INTEGER,
    senderorganization TEXT,
    receivertype INTEGER,
    receiverorganization TEXT,
    primarysource_qualification INTEGER,
    primarysource_reportercountry TEXT,
    authoritynumb TEXT,
    occurcountry TEXT,
    duplicate INTEGER
);

CREATE TABLE reaction (
    id INTEGER PRIMARY KEY,
    safetyreportid INTEGER,
    reactionmeddrapt TEXT,
    reactionmeddraversionpt REAL,
    reactionoutcome INTEGER,
    FOREIGN KEY (safetyreportid) REFERENCES report(safetyreportid)
);

CREATE TABLE report_duplicate (
    id INTEGER PRIMARY KEY,
    safetyreportid INTEGER,
    duplicatenumb TEXT,
    duplicatesource TEXT,
    FOREIGN KEY (safetyreportid) REFERENCES report(safetyreportid)
);

CREATE TABLE patient_optional (
    safetyreportid INTEGER PRIMARY KEY,
    patientagegroup INTEGER,
    patientonsetage INTEGER,
    patientonsetageunit INTEGER,
    patientsex INTEGER,
    patientweight REAL,
    narrativeincludeclinical TEXT,
    FOREIGN KEY (safetyreportid) REFERENCES report(safetyreportid)
);

CREATE TABLE drug (
    id INTEGER PRIMARY KEY,
    safetyreportid INTEGER,
    medicinalproduct TEXT,
    drugcharacterization INTEGER,
    drugauthorizationnumb TEXT,
    drugdosagetext TEXT,
    drugstartdate TEXT,
    drugstartdateformat INTEGER,
    drugenddate TEXT,
    drugenddateformat INTEGER,
    drugadministrationroute INTEGER,
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
    actiondrug INTEGER,
    drugadditional INTEGER,
    drugbatchnumb TEXT,
    drugcumulativedosagenumb REAL,
    drugcumulativedosageunit INTEGER,
    drugintervaldosagedefinition INTEGER,
    drugintervaldosageunitnumb REAL,
    drugrecurreadministration INTEGER,
    drugseparatedosagenumb REAL,
    drugstructuredosagenumb REAL,
    drugstructuredosageunit INTEGER,
    drugtreatmentduration REAL,
    drugtreatmentdurationunit INTEGER,
    FOREIGN KEY (drug_id) REFERENCES drug(id)
);

CREATE TABLE summary (
    id INTEGER PRIMARY KEY,
    safetyreportid INTEGER,
    narrativeincludeclinical TEXT,
    case_event_date_extracted TEXT,
    FOREIGN KEY (safetyreportid) REFERENCES report(safetyreportid)
);
                           
CREATE TABLE primarysource_literature_reference (
    id INTEGER PRIMARY KEY,
    safetyreportid INTEGER,
    literature_reference TEXT,
    FOREIGN KEY (safetyreportid) REFERENCES report(safetyreportid)
);
        """)

if __name__ == "__main__":
    db_path = "sql/openfda_base_updated.db"
    conn = sqlite3.connect(db_path)
    create_tables(conn)
    print("✅ Updated tables created successfully in", db_path)
    conn.close()
