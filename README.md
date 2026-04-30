# Liver Transplant Data Warehouse SQlite
A modular ETL pipeline that transforms multi-source clinical data (REDCap, EPIC, PDFs) into a normalized SQLite database for analytics.

## Overview

A lightweight clinical data warehouse built in SQLite with a Python ETL pipeline, integrating EPIC (Oracle extracts), REDCap, and Excel data to support longitudinal analysis of liver transplant patients.

This project demonstrates end-to-end data engineering: schema design, ETL processing, and analytical querying on real-world clinical data structures.

```
EPIC (EHR)    ─┐
REDCap ────────├──> Python ETL (pandas) ───> SQLite Database ───> SQL Queries / Analysis
PDF docs ──────┘                     
```


## Data Model

The schema is designed to support longitudinal clinical data and transplant events.

### Core Tables


* patients: unique individuals (recipients and living donors)
* transplants: transplant events (linked to patients and donors)
* donors: donor information (living or deceased)
* labs: long-format laboratory data (time-series)
* recipient_demo: demographic extension (1:1 with patients)
* notes: optional unstructured clinical notes


## Database Schema

<img width="1032" height="807" alt="schema_diagram" src="https://github.com/user-attachments/assets/02e8341a-8746-4f83-aac1-4366463112b4" />

The schema separates patients, transplant events, donors, demographics, and longitudinal labs to support flexible clinical analytics.

### Key Design Decisions
* Surrogate keys (patient_id, transplant_id, donor_id)
  * Decouples internal identifiers (e.g., MRN) from relationships
* Long-format labs table
  * Supports flexible time-series queries and multiple analytes
* Minimal database constraints
  * Data quality enforced in ETL for flexibility and scalability
* Categorical values stored as TEXT (no lookup tables)
  * Aligns with REDCap and simplifies integration
* Event-based structure
  * Transplants and labs modeled as time-dependent events



## ETL Pipeline

The ETL pipeline is implemented in Python using pandas and is structured into three stages:

1. Extract
    * EPIC (Oracle extracts)
    * REDCap exports
    * Excel files
2. Transform
    * Standardize column names and formats
    * Clean and normalize lab values
    * Convert wide → long format for labs
    * Deduplicate records using exact-match keys
    * Parse timestamps and derive dates

3. Load
    * Insert into SQLite with foreign key relationships
    * Append-only design for longitudinal tracking



### Handling Unstructured Data (PDF)

An OCR-based ETL step is included to extract laboratory values from synthetic clinical PDF reports and convert them into structured rows within the labs table.
This demonstrates integration of unstructured clinical data into the warehouse.



## Example Queries

1. Latest Lab Value per Patient (Window Function)

Returns the most recent lab value for each analyte per patient using a window function.
This is a common BI pattern for generating a current clinical snapshot or preparing features for downstream analysis.

```sql
SELECT patient_id,
       analyte,
       analyte_value AS latest_value,
       date_of_lab AS latest_date
FROM (
    SELECT patient_id,
           analyte,
           analyte_value,
           date_of_lab,
           ROW_NUMBER() OVER (
               PARTITION BY patient_id, analyte
               ORDER BY date_of_lab DESC
           ) AS rn
    FROM labs
)
WHERE rn = 1
ORDER BY patient_id, analyte
LIMIT 15;
```
Example Output (truncated):
Showing a subset of patients and analytes for readability.

<img width="603" height="440" alt="Query_1" src="https://github.com/user-attachments/assets/bd68021c-0ca2-4200-8205-335d90e8f21f" />

### Use Case
* Identifies most recent clinical values per patient.
* Handles longitudinal lab data efficiently.
* Demonstrates use of window functions, a key SQL skill in analytics and BI.
* Forms the basis for dashboards, cohort definitions, and ML feature engineering.

2. Donor–Recipient Linkage from Parsed PDF Charts

```sql
SELECT
    t.transplant_id AS tx_id,
    t.patient_id AS recipient_id,
    p.source_patient_id AS donor_id,
    t.date_of_trans AS tx_date,
    t.organ,
    t.tx_graft_type AS graft_type,
    t.tx_donor_type AS donor_type
FROM transplants t
JOIN donors d ON t.donor_id = d.donor_id
JOIN patients p ON d.patient_id = p.patient_id
ORDER BY recipient_id
LIMIT 5;
```

Example Output:
<img width="1019" height="156" alt="image" src="https://github.com/user-attachments/assets/b05b74de-e5d8-4230-ac2c-5ab374ac9719" />

### Use Case
* Validates donor-recipient linkage across tables
* Shows PDF-derived donor data integrated into SQL
* Demonstrates normalized schema design with surrogate keys

3. Transplant Volume by Donor and Graft Type

```sql
SELECT
    tx_donor_type AS donor_type,
    tx_graft_type AS graft_type,
    COUNT(*) AS transplant_count
FROM transplants
GROUP BY tx_donor_type, tx_graft_type
ORDER BY donor_type, transplant_count DESC;
```
Example Output:
<img width="678" height="325" alt="Query_3" src="https://github.com/user-attachments/assets/902f4aaf-8955-4d7e-a66d-1c3187322b9c" />


### Use Case
* Supports operational reporting of transplant activity
* Highlights distribution of graft types by donor category
* Demonstrates grouped aggregation for BI-style summaries

4. Pipeline Load Validation

```sql
SELECT
    'patients' AS table_name,
    COUNT(*) AS row_count
FROM patients
UNION ALL
SELECT 'transplants', COUNT(*) FROM transplants
UNION ALL
SELECT 'donors', COUNT(*) FROM donors
UNION ALL
SELECT 'labs', COUNT(*) FROM labs
UNION ALL
SELECT 'recipient_demo', COUNT(*) FROM recipient_demo;
```
Example Output:
<img width="324" height="178" alt="Query_4" src="https://github.com/user-attachments/assets/053e3384-5066-49e7-8605-3b8fc48a3cbf" />

### Use Case
* Confirming end-to-end ETL completion and all major tables are loaded.
* Used for diagnosing any QA problems.

## Dataset Summary (Sample)

* This repository includes a small synthetic sample dataset for reproducibility.
* The sample preserves schema and query patterns while reducing size for portability.
* All data in this repository is synthetic and does not contain protected health information (PHI).
* Dates may are generated while preserving temporal relationships for analysis.

## How to run
```
pip install -r requirements.txt
python run_pipeline.py
```

## Repository Structure

```
.
├── data/                # De-identified SQLite database
├── etl/                 # ETL scripts (extract, transform, load)
├── schema/              # Schema definition and diagram
├── queries/             # Example SQL queries
├── README.md
└── requirements.txt
``` 


## Key Takeaways
* Designed a relational clinical database for longitudinal transplant data
* Built a modular ETL pipeline integrating REDCap, EPIC, and PDF sources
* Transformed wide clinical data into analysis-ready long format
* Enabled SQL-based analytics on structured and unstructured healthcare data

