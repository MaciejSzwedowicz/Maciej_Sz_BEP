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
    occurcountry TEXT,
    patientsex INTEGER,
    duplicate INTEGER
);

CREATE TABLE report_authority (
    safetyreportid INTEGER PRIMARY KEY,
    authoritynumb TEXT,
    FOREIGN KEY (safetyreportid) REFERENCES report(safetyreportid)
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

CREATE TABLE patient_age (
    safetyreportid INTEGER PRIMARY KEY,
    patientonsetage INTEGER,
    patientonsetageunit INTEGER,
    FOREIGN KEY (safetyreportid) REFERENCES report(safetyreportid)
);

CREATE TABLE patient_age_group (
    safetyreportid INTEGER PRIMARY KEY,
    patientagegroup INTEGER,
    FOREIGN KEY (safetyreportid) REFERENCES report(safetyreportid)
);

CREATE TABLE patient_weight (
    safetyreportid INTEGER PRIMARY KEY,
    patientweight REAL,
    FOREIGN KEY (safetyreportid) REFERENCES report(safetyreportid)
);

CREATE TABLE drug_catalog (
    drug_id INTEGER PRIMARY KEY,
    medicinalproduct TEXT NOT NULL
);

CREATE TABLE drug_activesubstance (
    drug_id INTEGER,
    activesubstancename TEXT,
    PRIMARY KEY (drug_id, activesubstancename),
    FOREIGN KEY (drug_id) REFERENCES drug_catalog(drug_id)
);

CREATE TABLE patient_drug_history (
    safetyreportid INTEGER,
    drug_instance_index INTEGER,
    drug_id INTEGER,
    drugauthorizationnumb TEXT,
    drugcharacterization INTEGER,
    drugstartdate TEXT,
    drugenddate TEXT,
    drugindication TEXT,
    actiondrug INTEGER,
    drugadministrationroute INTEGER,
    drugdosagetext TEXT,
    drugstructuredosagenumb REAL,
    drugstructuredosageunit INTEGER,
    drugseparatedosagenumb REAL,
    drugintervaldosagedefinition INTEGER,
    drugintervaldosageunitnumb REAL,
    drugseparatedosageunit TEXT,
    drugcumulativedosagenumb REAL,
    drugcumulativedosageunit INTEGER,
    drugbatchnumb TEXT,
    drugtreatmentduration REAL,
    drugtreatmentdurationunit INTEGER,
    drugadditional INTEGER,
    PRIMARY KEY (safetyreportid, drug_instance_index),
    FOREIGN KEY (safetyreportid) REFERENCES report(safetyreportid),
    FOREIGN KEY (drug_id) REFERENCES drug_catalog(drug_id)
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

CREATE TABLE drug_openfda (
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
    PRIMARY KEY (drug_id),
    FOREIGN KEY (drug_id) REFERENCES drug_catalog(drug_id)
);

        
""")
        
if __name__ == "__main__":
    db_path = "sql/openfda_final_v3.db"
    conn = sqlite3.connect(db_path)
    create_tables(conn)
    print("✅ Redesigned tables created successfully in", db_path)
    conn.close()