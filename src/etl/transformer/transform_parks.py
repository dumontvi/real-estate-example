import numpy as np
import pandas as pd
from etl.common.aws_utils import read_s3_file
from etl.common.aws_utils import write_s3_file
from etl.common.base_etl import Transform
from etl.common.helper import _flatten
from etl.common.helper import _pd_to_csv


class TransformParks(Transform):
    def __init__(self, report_date):
        self.report_date = report_date

    def transform(self):
        try:
            # Key generation based on date and file path
            ## Seattle Parks And Recreation Park Addresses dataset
            parks_addr_df = pd.read_csv(read_s3_file("test_bucket", "parks_address"))

            ## Seattle Parks And Recreation Park With Features
            parks_w_features_df = pd.read_csv(read_s3_file("test_bucket", "parks_w_features"))

            ## Get Zip code from Parks DF (Not Available in parks with features dataset)
            df = parks_addr_df.select("pmaid", "zipcode").join(parks_w_features_df, on="pmaid")

            # Some cleanup
            # 1. Type Casting
            df.zipcode = np.ceil(df.floors).astype(int)

            # 2. Deduping
            df.drop_duplicates(key=["pmaid", "feature_desc"])  # UPK = Pmaid, hash(feature_desc)

            # 3. Flatten any columns
            flattened_df = self._flatten(
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
