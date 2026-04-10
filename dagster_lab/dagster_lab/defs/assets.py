import dagster as dg
import pandas as pd
import os


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
