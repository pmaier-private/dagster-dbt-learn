import dagster as dg
import pandas as pd
import os
import boto3
import psycopg2
from io import BytesIO


@dg.asset
def copy_it():
    dg.get_dagster_logger().info(os.getcwd())
    return pd.read_csv("../sample_data/sample_calls_25.csv")


@dg.asset
def head_it(copy_it: pd.DataFrame) -> pd.DataFrame:

    output_path = "../temp_out/sample_calls_25_copy_headed.csv"
    df = copy_it.head(5)
    df.to_csv(output_path, index=False)
    return df


# defining asset dependency and passing data between assets using function
# parameters requires an I/O manager to be defined.
# def head_it(copy_it: str) -> str:
#    output_path = "../temp_out/sample_calls_25_copy_headed.csv"
#   pd.read_csv(copy_it).head(5).to_csv(
#       output_path, index=False
#   )
#   return output_path


@dg.asset_check(asset=head_it)
def head_check() -> dg.AssetCheckResult:
    head_it_data = pd.read_csv("../temp_out/sample_calls_25_copy_headed.csv")
    return dg.AssetCheckResult(
        passed=(count_right := len(head_it_data) == 5),
        metadata={
            "message": (
                f"Failed because head_it has wrong count: {len(head_it_data)}"
                if not count_right
                else ""
            )
        },
    )


@dg.asset
def s3_to_postgres():
    """
    Downloads all csv files from an AWS S3 bucket and insert the data into a
    Postgres database.
    """

    logger = dg.get_dagster_logger()

    s3_client = boto3.client("s3")
    bucket_name = os.getenv("S3_BUCKET_NAME", "default-bucket")

    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        database=os.getenv("POSTGRES_DB", "dagster_db"),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", ""),
    )
    cursor = conn.cursor()

    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name)
        csv_files = [
            obj["Key"]
            for obj in response.get("Contents", [])
            if obj["Key"].endswith(".csv")
        ]

        for csv_file in csv_files:
            logger.info(f"Processing {csv_file}")
            obj = s3_client.get_object(Bucket=bucket_name, Key=csv_file)
            df = pd.read_csv(BytesIO(obj["Body"].read()))

            table_name = csv_file.replace(".csv", "").replace("/", "_")
            df.to_sql(table_name, conn, if_exists="replace", index=False)
            logger.info(f"Inserted {len(df)} rows into {table_name}")

        conn.commit()
        logger.info(f"Successfully processed {len(csv_files)} CSV files")
    finally:
        cursor.close()
        conn.close()
