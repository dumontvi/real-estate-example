import numpy as np
import pandas as pd
from etl.common.aws_utils import read_s3_file
from etl.common.aws_utils import write_s3_file
from etl.common.base_etl import Transform
from etl.common.helper import _flatten
from etl.common.helper import _pd_to_csv


class TransformHouseSales(Transform):
    def __init__(self):
        pass

    def transform(self):
        try:
            # Key generation based on date and file path
            df = pd.read_csv(read_s3_file("test_bucket", "test_key"))

            # Some cleanup
            # 1. Type Casting - Code sampled from kaggle notebooks for cleanup
            df.date = pd.to_datetime(df.date)
            df.floors = np.ceil(df.floors).astype(int)

            # 2. Deduping
            df.drop_duplicates(key="id")  # key should be the upk of the table to dedup

            # 3. Flatten any columns
            flattened_df = _flatten(
                df, columns=["unstructured_data"]
            )  # Here an example is we have a column called unstrunctured_data and we flatten it.

            # 4. Convert all column to same case -> snake_case

            ## 5. any other cleanup particular to the dataset

            write_s3_file("test_bucket", "test_file", _pd_to_csv(flattened_df))

            response = {"Status": "Passed"}
        except Exception as e:
            logger.error(f"Failed transforming the data due to: {e}")
            response = {"Status": "Failed", "ErrorMessage": e}
        finally:
            return response
