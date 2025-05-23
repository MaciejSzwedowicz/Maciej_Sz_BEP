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

CREATE TABLE drug_fda_product_type (
    drug_id INTEGER PRIMARY KEY,
    product_type TEXT,
    FOREIGN KEY (drug_id) REFERENCES drug_catalog(drug_id)
);

CREATE TABLE drug_fda_substance (
    drug_id INTEGER,
    substance_name TEXT,         
    FoREIGN KEY (drug_id) REFERENCES drug_catalog(drug_id)                
);

CREATE TABLE drug_fda_application_number (
    drug_id INTEGER,
    application_number TEXT,
    FOREIGN KEY (drug_id) REFERENCES drug_catalog(drug_id)
);

CREATE TABLE drug_fda_brand_name (
    drug_id INTEGER,
    brand_name TEXT,
    FOREIGN KEY (drug_id) REFERENCES drug_catalog(drug_id)
);

CREATE TABLE drug_fda_generic_name (
    drug_id INTEGER,
    generic_name TEXT,
    FOREIGN KEY (drug_id) REFERENCES drug_catalog(drug_id)
);

CREATE TABLE drug_fda_manufacturer_name (
    drug_id INTEGER,
    manufacturer_name TEXT,
    FOREIGN KEY (drug_id) REFERENCES drug_catalog(drug_id)
);

CREATE TABLE drug_fda_product_ndc (
    drug_id INTEGER,
    product_ndc TEXT,
    FOREIGN KEY (drug_id) REFERENCES drug_catalog(drug_id)
);

CREATE TABLE drug_fda_package_ndc (
    drug_id INTEGER,
    package_ndc TEXT,
    FOREIGN KEY (drug_id) REFERENCES drug_catalog(drug_id)
);

CREATE TABLE drug_fda_pharm_class_epc (
    drug_id INTEGER,
    pharm_class_epc TEXT,
    FOREIGN KEY (drug_id) REFERENCES drug_catalog(drug_id)
);

CREATE TABLE drug_fda_pharm_class_cs (
    drug_id INTEGER,
    pharm_class_cs TEXT,
    FOREIGN KEY (drug_id) REFERENCES drug_catalog(drug_id)
);

CREATE TABLE drug_fda_pharm_class_moa (
    drug_id INTEGER,
    pharm_class_moa TEXT,
    FOREIGN KEY (drug_id) REFERENCES drug_catalog(drug_id)
);

CREATE TABLE drug_fda_pharm_class_pe (
    drug_id INTEGER,
    pharm_class_pe TEXT,
    FOREIGN KEY (drug_id) REFERENCES drug_catalog(drug_id)
);

CREATE TABLE drug_fda_rxcui (
    drug_id INTEGER,
    rxcui TEXT,
    FOREIGN KEY (drug_id) REFERENCES drug_catalog(drug_id)
);

CREATE TABLE drug_fda_unii (
    drug_id INTEGER,
    unii TEXT,
    FOREIGN KEY (drug_id) REFERENCES drug_catalog(drug_id)
);

CREATE TABLE drug_fda_route (
    drug_id INTEGER,
    route TEXT,
    FOREIGN KEY (drug_id) REFERENCES drug_catalog(drug_id)
);

CREATE TABLE drug_fda_spl_id (
    drug_id INTEGER,
    spl_id TEXT,
    FOREIGN KEY (drug_id) REFERENCES drug_catalog(drug_id)
);

CREATE TABLE drug_fda_spl_set_id (
    drug_id INTEGER,
    spl_set_id TEXT,
    FOREIGN KEY (drug_id) REFERENCES drug_catalog(drug_id)
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
                           

-- Create unique indexes to support INSERT OR IGNORE semantics
CREATE UNIQUE INDEX idx_drug_application_number_unique ON drug_fda_application_number(drug_id, application_number);
CREATE UNIQUE INDEX idx_drug_brand_name_unique ON drug_fda_brand_name(drug_id, brand_name);
CREATE UNIQUE INDEX idx_drug_generic_name_unique ON drug_fda_generic_name(drug_id, generic_name);
CREATE UNIQUE INDEX idx_drug_manufacturer_name_unique ON drug_fda_manufacturer_name(drug_id, manufacturer_name);
CREATE UNIQUE INDEX idx_drug_product_ndc_unique ON drug_fda_product_ndc(drug_id, product_ndc);
CREATE UNIQUE INDEX idx_drug_package_ndc_unique ON drug_fda_package_ndc(drug_id, package_ndc);
CREATE UNIQUE INDEX idx_drug_pharm_class_epc_unique ON drug_fda_pharm_class_epc(drug_id, pharm_class_epc);
CREATE UNIQUE INDEX idx_drug_pharm_class_cs_unique ON drug_fda_pharm_class_cs(drug_id, pharm_class_cs);
CREATE UNIQUE INDEX idx_drug_pharm_class_moa_unique ON drug_fda_pharm_class_moa(drug_id, pharm_class_moa);
CREATE UNIQUE INDEX idx_drug_pharm_class_pe_unique ON drug_fda_pharm_class_pe(drug_id, pharm_class_pe);
CREATE UNIQUE INDEX idx_drug_rxcui_unique ON drug_fda_rxcui(drug_id, rxcui);
CREATE UNIQUE INDEX idx_drug_unii_unique ON drug_fda_unii(drug_id, unii);
CREATE UNIQUE INDEX idx_drug_route_unique ON drug_fda_route(drug_id, route);
CREATE UNIQUE INDEX idx_drug_spl_id_unique ON drug_fda_spl_id(drug_id, spl_id);
CREATE UNIQUE INDEX idx_drug_spl_set_id_unique ON drug_fda_spl_set_id(drug_id, spl_set_id);
CREATE UNIQUE INDEX idx_drug_substance_name_unique ON drug_fda_substance(drug_id, substance_name);
CREATE UNIQUE INDEX idx_drug_product_type_unique ON drug_fda_product_type(drug_id, product_type);
        
""")
        
if __name__ == "__main__":
    db_path = "sql/openfda_final_v10.db"
    conn = sqlite3.connect(db_path)
    create_tables(conn)
    print("✅ Redesigned tables created successfully in", db_path)
    conn.close()
