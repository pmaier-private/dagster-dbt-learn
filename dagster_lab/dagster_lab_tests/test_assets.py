import pandas as pd
from unittest.mock import MagicMock, patch

from dagster_lab.defs.assets import copy_it, head_it


def test_copy_it_reads_sample_csv():
    expected = pd.DataFrame(
        {
            "call_id": ["c1", "c2"],
            "duration_seconds": [12, 34],
        }
    )
    mocked_read_csv = MagicMock(return_value=expected)

    with patch("dagster_lab.defs.assets.pd.read_csv", mocked_read_csv):
        result = copy_it.op.compute_fn.decorated_fn()

    mocked_read_csv.assert_called_once_with(
        "../sample_data/sample_calls_25.csv"
    )

    pd.testing.assert_frame_equal(result, expected)


def test_head_it_writes_first_five_rows():
    input_df = pd.DataFrame(
        {
            "call_id": [f"c{i}" for i in range(1, 8)],
            "duration_seconds": [10, 20, 30, 40, 50, 60, 70],
        }
    )

    with patch.object(pd.DataFrame, "to_csv", autospec=True) as mocked_to_csv:
        head_it.op.compute_fn.decorated_fn(input_df)

    mocked_to_csv.assert_called_once()
    called_df = mocked_to_csv.call_args.args[0]
    called_path = mocked_to_csv.call_args.args[1]
    called_index = mocked_to_csv.call_args.kwargs["index"]
    pd.testing.assert_frame_equal(called_df, input_df.head(5))
    assert called_path == "../temp_out/sample_calls_25_copy_headed.csv"
    assert called_index is False
