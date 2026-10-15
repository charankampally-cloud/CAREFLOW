import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery


ROOT_DIR = Path(__file__).resolve().parent.parent
CSV_FILE = ROOT_DIR / "data" / "careflow_raw_event_log.csv"

DATASET_ID = "careflow"
TABLE_ID = "raw_ehr_events"


SCHEMA = [
    bigquery.SchemaField("Case_ID", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("Patient_ID", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("Event_Sequence", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("Activity_Name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("Timestamp", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("Activity_End_Timestamp", "TIMESTAMP"),
    bigquery.SchemaField("Activity_Duration_Minutes", "INTEGER"),
    bigquery.SchemaField("Wait_Time_Minutes", "INTEGER"),
    bigquery.SchemaField("Age", "INTEGER"),
    bigquery.SchemaField("Gender", "STRING"),
    bigquery.SchemaField("Emergency_Severity", "STRING"),
    bigquery.SchemaField("Doctor_ID", "STRING"),
    bigquery.SchemaField("Nurse_ID", "STRING"),
    bigquery.SchemaField("Department", "STRING"),
    bigquery.SchemaField("Shift", "STRING"),
    bigquery.SchemaField("Has_XRay", "BOOLEAN"),
    bigquery.SchemaField("Has_Lab_Test", "BOOLEAN"),
    bigquery.SchemaField("Has_Rework", "BOOLEAN"),
    bigquery.SchemaField("Rework_Type", "STRING"),
    bigquery.SchemaField("Data_Source", "STRING"),
]


def main():
    print("=" * 70)
    print("CAREFLOW - BIGQUERY DATA LOADER")
    print("=" * 70)

    # ---------------------------------------------------------
    # Load environment configuration
    # ---------------------------------------------------------
    env_file = ROOT_DIR / ".env"

    print(f"\nLoading environment file:")
    print(env_file)

    load_dotenv(env_file)

    project_id = os.getenv("GOOGLE_CLOUD_PROJECT")

    if not project_id:
        raise RuntimeError(
            "GOOGLE_CLOUD_PROJECT is not configured in the .env file."
        )

    if project_id == "YOUR_PROJECT_ID":
        raise RuntimeError(
            "Please replace YOUR_PROJECT_ID with your actual "
            "Google Cloud project ID inside .env."
        )

    print(f"Google Cloud project: {project_id}")

    # ---------------------------------------------------------
    # Check source CSV
    # ---------------------------------------------------------
    print(f"\nChecking source CSV:")
    print(CSV_FILE)

    if not CSV_FILE.exists():
        raise FileNotFoundError(
            f"\nCSV file was not found:\n{CSV_FILE}\n\n"
            "Make sure the synthetic EHR generator has already been executed."
        )

    print("CSV file found.")

    # ---------------------------------------------------------
    # Read CSV
    # ---------------------------------------------------------
    print("\nReading CSV...")

    df = pd.read_csv(CSV_FILE)

    print(f"Rows: {len(df):,}")
    print(f"Columns: {len(df.columns)}")

    # ---------------------------------------------------------
    # Validate expected columns
    # ---------------------------------------------------------
    expected_columns = [field.name for field in SCHEMA]

    missing_columns = [
        column
        for column in expected_columns
        if column not in df.columns
    ]

    if missing_columns:
        raise ValueError(
            "The CSV is missing these required columns:\n"
            + "\n".join(f"- {column}" for column in missing_columns)
        )

    # ---------------------------------------------------------
    # Convert timestamps
    # ---------------------------------------------------------
    print("\nConverting timestamps...")

    df["Timestamp"] = pd.to_datetime(
        df["Timestamp"],
        errors="coerce",
        utc=True,
    )

    df["Activity_End_Timestamp"] = pd.to_datetime(
        df["Activity_End_Timestamp"],
        errors="coerce",
        utc=True,
    )

    if df["Timestamp"].isna().any():
        raise ValueError(
            "One or more rows contain an invalid Timestamp."
        )

    # ---------------------------------------------------------
    # Convert integer columns
    # ---------------------------------------------------------
    integer_columns = [
        "Event_Sequence",
        "Activity_Duration_Minutes",
        "Wait_Time_Minutes",
        "Age",
    ]

    for column in integer_columns:
        df[column] = pd.to_numeric(
            df[column],
            errors="coerce",
        ).astype("Int64")

    # ---------------------------------------------------------
    # Convert boolean columns
    # ---------------------------------------------------------
    boolean_columns = [
        "Has_XRay",
        "Has_Lab_Test",
        "Has_Rework",
    ]

    for column in boolean_columns:
        df[column] = (
            df[column]
            .fillna(False)
            .astype(bool)
        )

    # ---------------------------------------------------------
    # Connect to BigQuery
    # ---------------------------------------------------------
    print("\nConnecting to BigQuery...")

    client = bigquery.Client(project=project_id)

    print(f"Connected to project: {client.project}")

    # ---------------------------------------------------------
    # Create dataset
    # ---------------------------------------------------------
    dataset_ref = f"{client.project}.{DATASET_ID}"

    dataset = bigquery.Dataset(dataset_ref)
    dataset.location = "US"

    client.create_dataset(
        dataset,
        exists_ok=True,
    )

    print(f"BigQuery dataset ready: {dataset_ref}")

    # ---------------------------------------------------------
    # Configure destination table
    # ---------------------------------------------------------
    table_ref = (
        f"{client.project}."
        f"{DATASET_ID}."
        f"{TABLE_ID}"
    )

    print(f"Target table: {table_ref}")

    # ---------------------------------------------------------
    # Configure load job
    # ---------------------------------------------------------
    job_config = bigquery.LoadJobConfig(
        schema=SCHEMA,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    # ---------------------------------------------------------
    # Upload dataframe
    # ---------------------------------------------------------
    print("\nUploading event log to BigQuery...")
    print("Please wait...")

    load_job = client.load_table_from_dataframe(
        df,
        table_ref,
        job_config=job_config,
    )

    load_job.result()

    print("\nUpload completed successfully.")

    # ---------------------------------------------------------
    # Configure partitioning and clustering
    # ---------------------------------------------------------
    print("\nConfiguring table partitioning and clustering...")

    table = client.get_table(table_ref)

    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="Timestamp",
    )

    table.clustering_fields = [
        "Case_ID",
        "Activity_Name",
    ]

    client.update_table(
        table,
        [
            "time_partitioning",
            "clustering_fields",
        ],
    )

    print("Partitioning configured.")
    print("Clustering configured.")

    # ---------------------------------------------------------
    # Validate BigQuery data
    # ---------------------------------------------------------
    print("\nRunning BigQuery validation...")

    validation_query = f"""
        SELECT
            COUNT(*) AS total_events,
            COUNT(DISTINCT Case_ID) AS total_cases,
            COUNTIF(Has_Rework = TRUE) AS rework_events,
            COUNT(
                DISTINCT IF(
                    Has_Rework = TRUE,
                    Case_ID,
                    NULL
                )
            ) AS rework_cases
        FROM `{table_ref}`
    """

    result = client.query(validation_query).result()

    row = next(iter(result))

    # ---------------------------------------------------------
    # Final results
    # ---------------------------------------------------------
    print("\n" + "=" * 70)
    print("BIGQUERY VALIDATION")
    print("=" * 70)

    print(f"Total events : {row.total_events:,}")
    print(f"Total cases  : {row.total_cases:,}")
    print(f"Rework events: {row.rework_events:,}")
    print(f"Rework cases : {row.rework_cases:,}")

    print("=" * 70)

    print("\nCareFlow BigQuery load completed successfully.")
    print(f"Table: {table_ref}")


if __name__ == "__main__":
    main()