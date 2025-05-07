"""
This script creates the base and auxiliary tables for the OpenFDA SQLite schema.
"""

import sqlite3

def create_tables(conn):
    with conn:
        conn.executescript("""
-- Core report table
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
    primarysource_qualification TEXT
);

-- Reaction table (1:N)
CREATE TABLE reaction (
    id INTEGER PRIMARY KEY,
    safetyreportid TEXT REFERENCES report(safetyreportid),
    reactionmeddrapt TEXT,
    reactionmeddraversionpt TEXT,
    reactionoutcome TEXT
);

-- Drug table (1:N)
CREATE TABLE drug (
    id INTEGER PRIMARY KEY,
    safetyreportid TEXT REFERENCES report(safetyreportid),
    medicinalproduct TEXT,
    drugcharacterization TEXT,
    drugauthorizationnumb TEXT,
    drugdosagetext TEXT,
    drugstartdate TEXT,
    drugenddate TEXT,
    drugadministrationroute TEXT,
    drugdosageform TEXT,
    drugindication TEXT
);

-- Report duplicate table (1:N)
CREATE TABLE reportduplicate (
    id INTEGER PRIMARY KEY,
    safetyreportid TEXT REFERENCES report(safetyreportid),
    duplicatenumb TEXT,
    duplicatesource TEXT
);

-- Drug openfda field values (1:N)
CREATE TABLE drug_openfda (
    id INTEGER PRIMARY KEY,
    drug_id INTEGER REFERENCES drug(id),
    field TEXT,
    value TEXT
);

-- Drug active substances (1:N)
CREATE TABLE drug_activesubstance (
    drug_id INTEGER REFERENCES drug(id),
    activesubstancename TEXT
);

-- Drug optional fields (1:1 with drug)
CREATE TABLE drug_optional (
    drug_id INTEGER PRIMARY KEY REFERENCES drug(id),
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
    drugtreatmentdurationunit TEXT
);

-- Patient-level optional fields
CREATE TABLE patient_optional (
    safetyreportid TEXT PRIMARY KEY REFERENCES report(safetyreportid),
    patientagegroup TEXT,
    patientonsetage TEXT,
    patientonsetageunit TEXT,
    patientsex TEXT,
    patientweight TEXT,
    narrativeincludeclinical TEXT
);

-- Report-level optional fields
CREATE TABLE report_optional (
    safetyreportid TEXT PRIMARY KEY REFERENCES report(safetyreportid),
    authoritynumb TEXT,
    occurcountry TEXT,
    primarysource_literaturereference TEXT,
    primarysource_reportercountry TEXT
);
""")

if __name__ == "__main__":
    db_path = "openfda_base.db"  # Change as needed
    conn = sqlite3.connect(db_path)
    create_tables(conn)
    print("✅ Tables created successfully in", db_path)
    conn.close()
