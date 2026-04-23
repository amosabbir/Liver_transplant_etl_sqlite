# Clinical-data-warehouse-sqlite

## Overview

A lightweight clinical data warehouse built in SQLite with a Python ETL pipeline, integrating EPIC (Oracle extracts), REDCap, and Excel data to support longitudinal analysis of liver transplant patients.

This project demonstrates end-to-end data engineering: schema design, ETL processing, and analytical querying on real-world clinical data structures.

```
EPIC (Oracle) ─┐
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

### Why this matters
Identifies most recent clinical values per patient
Handles longitudinal lab data efficiently
Demonstrates use of window functions, a key SQL skill in analytics and BI
Forms the basis for dashboards, cohort definitions, and ML feature engineering

2. Donor–recipient linkage

```sql
SELECT t.transplant_id, p.patient_id, d.donor_id, t.tx_graft_type
FROM transplants t
JOIN patients p ON t.patient_id = p.patient_id
LEFT JOIN donors d ON t.donor_id = d.donor_id;
```

3. Lab trends over time

```sql
SELECT patient_id, analyte, date_of_lab, AVG(value) as avg_value
FROM labs
GROUP BY patient_id, analyte, date_of_lab;
```



## Dataset Summary (Sample)

This repository includes a de-identified sample dataset:

* Patients: ~1,000
* Transplants: ~1,200
* Labs: ~50,000 rows

The sample preserves schema and query patterns while reducing size for portability.

### Data Privacy

All data in this repository is synthetic or de-identified and does not contain protected health information (PHI).
Identifiers have been removed or replaced, and dates may be shifted while preserving temporal relationships for analysis.


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
* Designed a clinical data warehouse for longitudinal patient data
* Built a modular ETL pipeline for messy, multi-source healthcare data
* Modeled time-series lab data for flexible analysis
* Integrated structured and unstructured data into a unified system

