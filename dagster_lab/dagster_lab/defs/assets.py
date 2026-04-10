import dagster as dg
import pandas as pd
import os


@dg.asset
def copy_it():
    dg.get_dagster_logger().info(os.getcwd())

    pd.read_csv("../sample_data/sample_calls_25.csv").to_csv(
        "../temp_out/sample_calls_25_copy.csv", index=False
    )
