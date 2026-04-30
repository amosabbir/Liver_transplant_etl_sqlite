CREATE TABLE patients (
    patient_id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_patient_id TEXT UNIQUE,
    patient_type TEXT,
    redcap_record_id INTEGER
);

CREATE TABLE donors (
    donor_id INTEGER PRIMARY KEY AUTOINCREMENT,
    patient_id INTEGER,
    date_of_birth DATE,
    donor_sex TEXT,
    donor_bloodtype TEXT,
    donor_blood_rh TEXT,
    donor_cod TEXT,
    idx_first INTEGER,
    FOREIGN KEY (patient_id) REFERENCES patients(patient_id)
);

CREATE TABLE recipient_demo (
    patient_id INTEGER PRIMARY KEY,
    date_of_birth DATE,
    date_of_death DATE,
    bloodtype TEXT,
    blood_rh TEXT,
    sex TEXT,
    FOREIGN KEY (patient_id) REFERENCES patients(patient_id)
);

CREATE TABLE labs (
    lab_id INTEGER PRIMARY KEY AUTOINCREMENT,
    patient_id INTEGER NOT NULL,
    date_of_lab DATE,
    analyte TEXT NOT NULL,
    analyte_value REAL,
    FOREIGN KEY (patient_id) REFERENCES patients(patient_id)
);

CREATE TABLE transplants (
    transplant_id INTEGER PRIMARY KEY AUTOINCREMENT,
    patient_id INTEGER NOT NULL,
    donor_id INTEGER,
    date_of_trans DATE NOT NULL,
    organ TEXT,
    tx_multiorgan INTEGER,
    tx_graft_type TEXT,
    tx_donor_type TEXT,
    FOREIGN KEY (patient_id) REFERENCES patients(patient_id),
    FOREIGN KEY (donor_id) REFERENCES donors(donor_id)
);

CREATE INDEX idx_labs_patient_date ON labs(patient_id, date_of_lab);
CREATE INDEX idx_transplants_patient ON transplants(patient_id, date_of_trans);
CREATE INDEX idx_transplants_donor ON transplants(donor_id);