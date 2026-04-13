import dagster as dg
import pandas as pd
import os
import boto3
from sqlalchemy import create_engine
from io import BytesIO


from localstack_client.patch import enable_local_endpoints

enable_local_endpoints()


@dg.asset(group_name="dagster_only")
def copy_it():
    dg.get_dagster_logger().info(os.getcwd())
    return pd.read_csv("../sample_data/sample_calls_25.csv")


@dg.asset(group_name="dagster_only")
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


@dg.asset(
    name="raw_table",
    key_prefix=["dagster"],
    group_name="raw",
    kinds={"postgres", "dbt"},
    owners=["team:data-eng"],
)
def raw_dbt_source():
    """
    Downloads all csv files from an AWS S3 bucket and insert the data into a
    Postgres database.

    The asset contains a simulated bug where it will duplicate the data, which
    dbt has to handle downstream.
    """

    logger = dg.get_dagster_logger()

    s3_client = boto3.client("s3")
    bucket_name = "dagster-dbt-raw"
    table_name = "raw_table"

    if any(
        k not in os.environ
        for k in [
            "POSTGRES_HOST",
            "POSTGRES_DB",
            "POSTGRES_USER",
            "POSTGRES_PASSWORD",
        ]
    ):
        raise ValueError(
            "Missing required environment variables for Postgres connection"
        )

    engine = create_engine(
        (
            "postgresql+psycopg2://"
            f"{os.environ['POSTGRES_USER']}:"
            f"{os.environ['POSTGRES_PASSWORD']}@"
            f"{os.environ['POSTGRES_HOST']}/"
            f"{os.environ['POSTGRES_DB']}"
        )
    )

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

            df = pd.concat(
                [df, df]
            )  # simulated bug: duplicate the data before inserting

            df.to_sql(
                table_name,
                con=engine,
                schema="public",
                if_exists="replace",
                index=False,
            )
            logger.info(f"Inserted {len(df)} rows into {table_name}")

        logger.info(f"Successfully processed {len(csv_files)} CSV files")
        return dg.MaterializeResult(
            metadata={
                "num_files": dg.MetadataValue.int(len(csv_files)),
                "table_name": dg.MetadataValue.text(table_name),
            },
            value=1,
        )
    finally:
        engine.dispose()
