import pandas as pd
import numpy as np
import re




def run(conn, db_path, epic_file):
    # ==============================
    # CONFIG
    # ==============================

    LAB_MAP = {
        "NA": "sodium",
        "CREAT": "creatinine",
        "BILT": "total_bilirubin",
        "AST": "ast",
        "ALT": "alt",
        "ALK_PHOS": "alk_phos",
        "ALB": "albumin",
        "HGB": "hemoglobin",
        "HCT": "hematocrit",
        "aPTT": "aptt",
        "PT": "pt",
        "INR": "inr",
        "HBA1C": "hba1c",
        "PLATELETS": "platelets",
        "LYMPH": "lymphocytes",
        "eGFR": "egfr",
        "AFP": "afp",
        "WBC": "wbc",
        "NEUTROPHIL": "neutrophils",
    }

    # ==============================
    # HELPERS
    # ==============================
    def clean_numeric(x):
        """
        Convert values like:
        54
        54.2
        '21 c'
        '<2.0'
        '7.1 A'
        into numeric when possible.
        """
        if pd.isna(x):
            return np.nan
        if isinstance(x, (int, float, np.integer, np.floating)):
            return float(x)

        x = str(x).strip()
        match = re.search(r"-?\d+(\.\d+)?", x)
        return float(match.group()) if match else np.nan


    def pyify(x):
        if pd.isna(x):
            return None
        if isinstance(x, np.integer):
            return int(x)
        if isinstance(x, np.floating):
            return float(x)
        return x


    # ==============================
    # LOAD EXTRACT
    # ==============================
    epic = pd.read_csv(epic_file, low_memory=False)

    # standardize MRN
    epic["PAT_MRN_ID"] = pd.to_numeric(epic["PAT_MRN_ID"], errors="coerce").astype("Int64")


    lookup = pd.read_sql_query(
        """
        SELECT patient_id, source_patient_id
        FROM patients
        """,
        conn,
    )

    lookup["source_patient_id"] = pd.to_numeric(
        lookup["source_patient_id"], errors="coerce"
    ).astype("Int64")

    # join MRN -> patient_id
    epic = epic.merge(
        lookup,
        left_on="PAT_MRN_ID",
        right_on="source_patient_id",
        how="left"
    )

    # keep only matched patients
    epic = epic.dropna(subset=["patient_id"]).copy()
    epic["patient_id"] = epic["patient_id"].astype(int)

    # ==============================
    # WIDE -> LONG
    # ==============================
    long_frames = []

    for raw_name, analyte_name in LAB_MAP.items():
        date_col = f"{raw_name}_LAB_DATE"
        result_col = f"{raw_name}_LAB_RESULT" if f"{raw_name}_LAB_RESULT" in epic.columns else f"{raw_name}_RESULT"

        if date_col not in epic.columns or result_col not in epic.columns:
            continue

        temp = epic[["patient_id", date_col, result_col]].copy()
        temp = temp.rename(columns={
            date_col: "timestamp_of_lab",
            result_col: "analyte_value"
        })

        temp["analyte"] = analyte_name
        long_frames.append(temp)

    labs = pd.concat(long_frames, ignore_index=True)

    # ==============================
    # CLEAN
    # ==============================
    labs["timestamp_of_lab"] = pd.to_datetime(
        labs["timestamp_of_lab"],
        errors="coerce"
    )

    labs["date_of_lab"] = labs["timestamp_of_lab"].dt.strftime("%Y-%m-%d")
    labs["timestamp_of_lab"] = labs["timestamp_of_lab"].dt.strftime("%Y-%m-%d %H:%M:%S")

    labs["analyte_value"] = labs["analyte_value"].apply(clean_numeric)

    # remove bad rows
    labs = labs.dropna(subset=["patient_id", "timestamp_of_lab", "analyte", "analyte_value"]).copy()

    # deduplicate exact repeats
    labs = labs.drop_duplicates(
        subset=["patient_id", "timestamp_of_lab", "analyte", "analyte_value"]
    ).copy()

    # reorder
    labs = labs[[
        "patient_id",
        "date_of_lab",
        "analyte",
        "analyte_value"
    ]]

    # ==============================
    # INSERT
    # ==============================
    rows = [
        tuple(pyify(x) for x in row)
        for row in labs.itertuples(index=False, name=None)
    ]

    conn.executemany(
        """
        INSERT INTO labs (
            patient_id,
            date_of_lab,
            analyte,
            analyte_value
        )
        VALUES (?, ?, ?, ?)
        """,
        rows
    )

    conn.commit()




def main():
    run(conn, db_path, epic_file)


if __name__=='__main__':
    main()