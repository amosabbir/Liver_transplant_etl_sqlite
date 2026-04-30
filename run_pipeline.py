import os
import sqlite3
import pandas as pd
import numpy as np
import time 


from etl import DB_initialize
from etl import REDCap_ETL
from etl import EPIC_ETL
from etl import PyRead_ETL


def print_row_counts(conn):
    tables = ["patients", "recipient_demo", "transplants", "donors", "labs"]
    print("\nCurrent table counts:")
    for table in tables:
        try:
            cur = conn.execute(f"SELECT COUNT(*) FROM {table}")
            n = cur.fetchone()[0]
            print(f"  {table}: {n}")
        except sqlite3.Error as e:
            print(f"  {table}: error ({e})")


def run_query_menu(conn):
    queries = {
        "1": {
            "title": "Latest labs selected for one patient",
            "sql": """
                SELECT patient_id, analyte, MAX(date_of_lab) AS latest_lab_date, analyte_value
                FROM labs
                WHERE patient_id = 1
                    AND analyte IN ("sodium","creatinine", "total_bilirubin", "hemoglobin", "albumin")
                GROUP BY analyte;
            """
        },
        "2": {
            "title": "Recipient-transplant-donor linkage",
            "sql": """
                SELECT
                    t.transplant_id,
                    t.patient_id,
                    t.date_of_trans,
                    t.donor_id,
                    d.donor_bloodtype,
                    d.donor_sex
                FROM transplants t
                JOIN donors d
                    ON t.donor_id = d.donor_id
                ORDER BY t.date_of_trans;
            """
        },
        "3": {
            "title": "Lab counts by analyte",
            "sql": """
                SELECT analyte, COUNT(*) AS n_rows
                FROM labs
                GROUP BY analyte
                ORDER BY n_rows DESC, analyte;
            """
        }
    }

    while True:
        print("\nNow that data is entered, would you like to query the database?")
        print(f"1) {queries["1"]['title']}")
        print(f"2) {queries["2"]['title']}")
        print(f"3) {queries["3"]['title']}")
        print(f"4) Close")

        choice = input("Enter choice: ").strip()

        if choice == "4":
            print("Closing.")
            break

        if choice not in queries:
            print("Invalid choice. Please select 1, 2, 3, or 4.")
            continue

        
        try:
            pd.set_option('display.max_columns', None)
            print(f"\nRunning: {queries[choice]['title']}\n")
            print(f"Showing the first 5 results\n")
            print(pd.read_sql(queries[choice]["sql"],conn).head())
            print(f"\n")
            time.sleep(5)

        except sqlite3.Error as e:
            print(f"Query failed: {e}")



def main():

    base_dir = input("Enter the project folder path: ").strip()
    schema_path = os.path.join(base_dir, r"schema\DB_schema_.sql")
    db_path = os.path.join(base_dir, r"data\Clin_data.db")
    redcap_file = os.path.join(base_dir, r"data\REDCap_data.csv")
    epic_file = os.path.join(base_dir, r"data\EPIC_extract.csv")
    donor_pdf_dir = os.path.join(base_dir, r"data\donor_data")

    if os.path.exists(db_path):
        os.remove(db_path)

    time.sleep(3)
    print("\nWe first begin by creating a DB and importing the schema.")
    time.sleep(3)
    DB_initialize.run(db_path=db_path, schema_path=schema_path)

    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON;")
    print_row_counts(conn)

    time.sleep(5) #give some time to read text
    print("\nWe are now importing data from REDCap into SQLite.")
    time.sleep(5)
    print("REDCap data is hierarchical. Recipients have one row for demographic data and repeated rows for transplant events.")
    REDCap_ETL.run(conn=conn, db_path=db_path, redcap_file=redcap_file)
    time.sleep(5)
    print_row_counts(conn)

    time.sleep(5)
    print("\nNow we ingest data from EPIC, an electronic health record export.")
    time.sleep(5)
    print("Lab data arrive in wide format and are transformed to long format before insertion.")
    EPIC_ETL.run(conn=conn, db_path=db_path, epic_file=epic_file)
    time.sleep(5)
    print_row_counts(conn)

    time.sleep(5)
    print("\nDonor data is acquired through PDF charts.")
    time.sleep(5)
    print("These are parsed, standardized, matched to recipients, and inserted into the database.")
    PyRead_ETL.run(conn=conn, pdf_folder=donor_pdf_dir)
    time.sleep(5)
    print_row_counts(conn)

    time.sleep(5)
    run_query_menu(conn)

    conn.close()


if __name__ == "__main__":
    main()