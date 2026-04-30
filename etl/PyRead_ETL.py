import re
from pathlib import Path
from typing import Optional, List, Dict, Any
import pandas as pd
from pypdf import PdfReader

def run(conn, pdf_folder): 
    
    def extract_text_from_pdf(pdf_path: Path) -> str:
        reader = PdfReader(str(pdf_path))
        return "\n".join(page.extract_text() or "" for page in reader.pages)


    def grab_first(patterns: List[str], text: str, flags=re.IGNORECASE) -> Optional[str]:
        for pattern in patterns:
            m = re.search(pattern, text, flags)
            if m:
                return m.group(1).strip()
        return None


    def clean_date(value: Optional[str]) -> Optional[str]:
        if not value:
            return None
        dt = pd.to_datetime(value.strip(), errors="coerce")
        if pd.isna(dt):
            return None
        return dt.strftime("%Y-%m-%d")


    def normalize_sex(value: Optional[str]) -> Optional[str]:
        if not value:
            return None
        value = value.strip().lower()
        if value in {"m", "male"}:
            return "Male"
        if value in {"f", "female"}:
            return "Female"
        return value.title()


    def normalize_rh(value: Optional[str]) -> Optional[str]:
        if not value:
            return None
        value = value.strip().lower()
        if value in {"positive", "pos", "+"}:
            return "+"
        if value in {"negative", "neg", "-"}:
            return "-"
        return None


    def split_blood_rh(blood_text: Optional[str], rh_text: Optional[str]):
        abo = None
        rh = normalize_rh(rh_text)

        if blood_text:
            blood_text = blood_text.strip().upper()
            m = re.match(r"^(AB|A|B|O)\s*([+-])?$", blood_text)
            if m:
                abo = m.group(1)
                if m.group(2) and rh is None:
                    rh = m.group(2)
            else:
                abo = blood_text

        return abo, rh


    def parse_pdf_donor_id(raw_id: Optional[str]) -> Optional[int]:
        if not raw_id:
            return None
        m = re.search(r"(\d+)", raw_id)
        if not m:
            return None
        return int(m.group(1))


    def parse_one_donor_pdf(text: str, source_file: str) -> Dict[str, Any]:
        raw_donor_id = grab_first([
            r"Donor ID:\s*([A-Za-z0-9_-]+)",
            r"\bID:\s*([A-Za-z0-9_-]+)",
            r"Donor Identifier:\s*([A-Za-z0-9_-]+)",
        ], text)

        donor_id = parse_pdf_donor_id(raw_donor_id)

        blood_raw = grab_first([
            r"Blood Type:\s*([A-Za-zO][B]?[+-]?)",
            r"ABO Group:\s*([A-Za-zO][B]?[+-]?)",
            r"Blood Group:\s*([A-Za-zO][B]?[+-]?)",
        ], text)

        rh_raw = grab_first([
            r"Rh:\s*(Positive|Negative|Pos|Neg|\+|-)",
            r"Rh Factor:\s*(Positive|Negative|Pos|Neg|\+|-)",
        ], text)

        donor_bloodtype, donor_blood_rh = split_blood_rh(blood_raw, rh_raw)

        donor_sex = normalize_sex(grab_first([
            r"Sex:\s*(Male|Female|M|F)",
        ], text))

        date_of_birth = clean_date(grab_first([
            r"Date of Birth:\s*([^\n]+)",
            r"\bDOB:\s*([^\n]+)",
        ], text))

        donor_cod = grab_first([
            r"Cause of Death:\s*([^\n]*)",
            r"\bCOD:\s*([^\n]+)",
        ], text)
        if donor_cod is not None and donor_cod.strip() == "":
            donor_cod = None

        recipient_source_id = grab_first([
            r"Recipient ID:\s*([A-Za-z0-9_-]+)",
        ], text)

        date_of_trans = clean_date(grab_first([
            r"Date of Transplant:\s*([^\n]+)",
            r"Date of Trans:\s*([^\n]+)",
        ], text))

        return {
            "donor_id": donor_id,
            "date_of_birth": date_of_birth,
            "donor_sex": donor_sex,
            "donor_bloodtype": donor_bloodtype,
            "donor_blood_rh": donor_blood_rh,
            "donor_cod": donor_cod,
            "idx_first": None,
            "recipient_source_id": recipient_source_id,
            "date_of_trans": date_of_trans,
            "source_file": source_file,
            "raw_donor_id": raw_donor_id,
        }


    def build_donor_df(pdf_folder: str) -> pd.DataFrame:
        rows = []

        for pdf_path in sorted(Path(pdf_folder).glob("*.pdf")):
            text = extract_text_from_pdf(pdf_path)
            rows.append(parse_one_donor_pdf(text, pdf_path.name))

        donor_df = pd.DataFrame(rows)

        # optional QA
        donor_df = donor_df.sort_values("donor_id").reset_index(drop=True)

        return donor_df



    # --------------------------------------------------
    # Insert donor raw IDs into patients table first
    #    so each donor gets a patient_id
    # --------------------------------------------------
    donor_df = build_donor_df(pdf_folder)
    donor_patients = donor_df[["raw_donor_id"]].copy()

    donor_patients = donor_patients.rename(columns={
        "raw_donor_id": "source_patient_id"
    })

    donor_patients["patient_type"] = "donor"
    donor_patients["redcap_record_id"] = None

    rows = list(
        donor_patients[
            ["source_patient_id", "patient_type", "redcap_record_id"]
        ]
        .drop_duplicates()
        .itertuples(index=False, name=None)
    )

    conn.executemany(
        """
        INSERT INTO patients (source_patient_id, patient_type, redcap_record_id)
        VALUES (?, ?, ?)
        ON CONFLICT(source_patient_id) DO NOTHING
        """,
        rows
    )

    # --------------------------------------------------
    # Pull donor patient_id lookup from patients table
    # --------------------------------------------------
    donor_patient_lookup = pd.read_sql_query(
        """
        SELECT patient_id, source_patient_id
        FROM patients
        WHERE patient_type = 'donor'
        """,
        conn
    )

    # merge donor patient_id into donor_df
    donor_df = donor_df.merge(
        donor_patient_lookup,
        left_on="raw_donor_id",
        right_on="source_patient_id",
        how="left"
    )

    # keep only one patient_id column for donors table
    donor_df["patient_id"] = donor_df["patient_id"].astype("Int64")
    donor_df = donor_df.drop(columns=["source_patient_id"])

    # --------------------------------------------------
    # Insert into donors table
    # --------------------------------------------------
    # Rebuild rows AFTER donor_df has the correct patient_id values
    rows = []
    for _, r in donor_df.iterrows():
        patient_id = None if pd.isna(r["patient_id"]) else int(r["patient_id"])
        idx_first = None if pd.isna(r["idx_first"]) else int(r["idx_first"])

        rows.append((
            int(r["donor_id"]),
            patient_id,
            None if pd.isna(r["date_of_birth"]) else r["date_of_birth"],
            None if pd.isna(r["donor_sex"]) else r["donor_sex"],
            None if pd.isna(r["donor_bloodtype"]) else r["donor_bloodtype"],
            None if pd.isna(r["donor_blood_rh"]) else r["donor_blood_rh"],
            None if pd.isna(r["donor_cod"]) else r["donor_cod"],
            idx_first,
        ))



    conn.executemany(
        """
        INSERT INTO donors (
            donor_id,
            patient_id,
            date_of_birth,
            donor_sex,
            donor_bloodtype,
            donor_blood_rh,
            donor_cod,
            idx_first
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows
    )


    # --------------------------------------------------
    # Link donors to recipient transplants
    # --------------------------------------------------

    # Get patient lookup for recipients
    patient_lookup = pd.read_sql_query(
        "SELECT patient_id, source_patient_id FROM patients",
        conn
    )

    # clean IDs as strings to avoid .0 / leading zero issues
    donor_df["recipient_source_id"] = donor_df["recipient_source_id"].astype("string").str.strip()
    patient_lookup["source_patient_id"] = patient_lookup["source_patient_id"].astype("string").str.strip()

    # remove accidental .0 if present
    donor_df["recipient_source_id"] = donor_df["recipient_source_id"].str.replace(r"\.0$", "", regex=True)
    patient_lookup["source_patient_id"] = patient_lookup["source_patient_id"].str.replace(r"\.0$", "", regex=True)

    # Merge donor_df with recipient patient_id
    df_link = donor_df.merge(
        patient_lookup,
        left_on="recipient_source_id",
        right_on="source_patient_id",
        how="left"
    )
    
    # Drop bad matches
    df_link = df_link.dropna(subset=["patient_id_y", "date_of_trans"]).copy()
    df_link["patient_id_y"] = df_link["patient_id_y"].astype(int)

    # Update transplants
    update_sql = """
    UPDATE transplants
    SET donor_id = ?
    WHERE patient_id = ?
    AND date_of_trans = ?
    """
    rows = list(
        df_link[["donor_id", "patient_id_y", "date_of_trans"]]
        .itertuples(index=False, name=None)
    )
    
    conn.executemany(update_sql, rows)

    conn.commit()




def main():
    run(conn, pdf_folder)


if __name__=='__main__':
    main()