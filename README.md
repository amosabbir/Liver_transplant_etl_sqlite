# Clinical-data-warehouse-sqlite

## Overview

A lightweight clinical data warehouse built in SQLite with a Python ETL pipeline, integrating EPIC (Oracle extracts), REDCap, and Excel data to support longitudinal analysis of liver transplant patients.

This project demonstrates end-to-end data engineering: schema design, ETL processing, and analytical querying on real-world clinical data structures.


EPIC (Oracle) ─┐

              ├── Python ETL (pandas) ─── SQLite Database ─── SQL Queries / Analysis

REDCap ────────┤

Excel ─────────┘

PDF docs ──────┘                     



### Data Model

The schema is designed to support longitudinal clinical data and transplant events.

### Core Tables

* patients — unique individuals (recipients and living donors)
* transplants — transplant events (linked to patients and donors)
* donors — donor information (living or deceased)
* labs — long-format laboratory data (time-series)
* recipient_demo — demographic extension (1:1 with patients)
* notes — optional unstructured clinical notes

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

