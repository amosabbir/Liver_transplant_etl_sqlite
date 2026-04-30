import sqlite3
import pandas as pd

def run(db_path, schema_path):
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("PRAGMA foreign_keys = ON;")
    print("\nConnected to SQLite database.\n")

    with open(schema_path, "r") as f:
        conn.executescript(f.read())

    conn.commit()
    conn.close()

    print("\nSchema created successfully.\n")


def main():
    run(db_path, schema_path)


if __name__ == "__main__":
    main()