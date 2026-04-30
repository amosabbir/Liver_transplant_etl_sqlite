import pandas as pd

def run(conn, db_path, redcap_file):
    # ==============================
    # LOAD + CLEAN
    # ==============================
    df = pd.read_csv(redcap_file, dtype={"source_patient_id": "string"})

    # standardize columns
    df.columns = df.columns.str.lower().str.strip()

    # rename for clarity
    df = df.rename(columns={
        "redcap_repeat_instrument": "instrument",
    })

    # normalize IDs
    df["record_id"] = pd.to_numeric(df["record_id"], errors="coerce").astype("Int64")
    df["redcap_repeat_instance"] = pd.to_numeric(df["redcap_repeat_instance"], errors="coerce").astype("Int64")

    # normalize dates
    date_cols = ["date_of_birth", "date_of_death", "date_of_trans"]
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.strftime("%Y-%m-%d")

    # ==============================
    # SPLIT INSTRUMENTS
    # ==============================
    patient_df = df[df["instrument"] == "patient_information"].copy()
    tx_df = df[df["instrument"] == "transplant_and_donor_information"].copy()

    # ==============================
    # BUILD PATIENTS
    # ==============================
    patients = (
        patient_df[["record_id", "source_patient_id"]]
        .dropna(subset=["source_patient_id"])
        .drop_duplicates()
        .rename(columns={"record_id": "redcap_record_id"})
    )

    patients["patient_type"] = "recipient"

    patients = patients[["source_patient_id", "patient_type", "redcap_record_id"]]

    # ==============================
    # BUILD RECIPIENT DEMO
    # ==============================
    recipient_demo = (
        patient_df[
            [
                "source_patient_id",
                "date_of_birth",
                "date_of_death",
                "bloodtype",
                "blood_rh",
                "sex",
            ]
        ]
        .drop_duplicates(subset=["source_patient_id"])
    )


    # ==============================
    # BUILD TRANSPLANTS
    # ==============================
    transplants = (
        tx_df[
            [
                "record_id",
                "redcap_repeat_instance",
                "date_of_trans",
                "organ",
                "tx_multiorgan",
                "tx_graft_type",
                "tx_donor_type",
            ]
        ]
        .copy()
    )

    # clean multiorgan
    transplants["tx_multiorgan"] = pd.to_numeric(
        transplants["tx_multiorgan"], errors="coerce"
    )


    # drop empty rows
    transplants = transplants[transplants["date_of_trans"].notna()].copy()


    # ---- PATIENTS ----
    patients['redcap_record_id'] = patients['redcap_record_id'].map(
        lambda x: int(x) if pd.notna(x) else None
    )

    conn.executemany(
        """
        INSERT INTO patients (source_patient_id, patient_type, redcap_record_id)
        VALUES (?, ?, ?)
        ON CONFLICT(source_patient_id) DO UPDATE SET
            patient_type = excluded.patient_type,
            redcap_record_id = excluded.redcap_record_id;
        """,
        patients.itertuples(index=False, name=None),
    )

    # get lookup
    lookup = pd.read_sql_query(
        "SELECT patient_id, source_patient_id, redcap_record_id FROM patients",
        conn,
    )

    # ---- RECIPIENT DEMO ----
    recipient_demo = recipient_demo.merge(
        lookup[["patient_id", "source_patient_id"]],
        on="source_patient_id",
        how="left",
    )

    recipient_demo = recipient_demo.dropna(subset=["patient_id"])
    recipient_demo["patient_id"] = recipient_demo["patient_id"].astype(int)

    conn.executemany(
        """
        INSERT INTO recipient_demo (
            patient_id, date_of_birth, date_of_death,
            bloodtype, blood_rh, sex
        )
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(patient_id) DO UPDATE SET
            date_of_birth = excluded.date_of_birth,
            date_of_death = excluded.date_of_death,
            bloodtype = excluded.bloodtype,
            blood_rh = excluded.blood_rh,
            sex = excluded.sex;
        """,
        recipient_demo[
            [
                "patient_id",
                "date_of_birth",
                "date_of_death",
                "bloodtype",
                "blood_rh",
                "sex",
            ]
        ].itertuples(index=False, name=None),
    )

    # ---- TRANSPLANTS ----
    transplants = transplants.merge(
        lookup[["patient_id", "redcap_record_id"]],
        left_on="record_id",
        right_on="redcap_record_id",
        how="left",
    )

    transplants = transplants.dropna(subset=["patient_id"])
    transplants["patient_id"] = transplants["patient_id"].astype(int)

    transplants["donor_id"] = None

    conn.executemany(
        """
        INSERT INTO transplants (
            patient_id, donor_id, date_of_trans, organ,
            tx_multiorgan, tx_graft_type, tx_donor_type
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        transplants[
            [
                "patient_id",
                "donor_id",
                "date_of_trans",
                "organ",
                "tx_multiorgan",
                "tx_graft_type",
                "tx_donor_type"
            ]
        ].itertuples(index=False, name=None),
    )

    conn.commit()


def main():
    run(db_path, redcap_file)


if __name__=='__main__':
    main()