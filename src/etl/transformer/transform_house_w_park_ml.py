import numpy as np
import pandas as pd
from etl.common.aws_utils import read_s3_file
from etl.common.aws_utils import write_s3_file
from etl.common.base_etl import Transform
from etl.common.helper import _flatten
from etl.common.helper import _pd_to_csv


class HouseWParksFeatures(Transform):
    def __init__(self, report_date):
        self.report_date = report_date

    def transform(self):
        try:
            # Key generation based on date and file path
            ## King County House dataset
            ## Can read historical data (many dates) from S3 for training, only today required for prediction
            houses = pd.read_csv(read_s3_file("test_bucket", "king_county_house.csv"))

            ## Seattle Parks And Recreation Park With Features
            ## Can read historical data (many dates) from S3 for training, only today required for prediction
            parks_w_features_df = pd.read_csv(read_s3_file("test_bucket", "parks_w_features_transformed.csv"))

            ## Get Zip code from Parks DF (Not Available in parks with features dataset)
            _df = houses.join(parks_w_features_df, on="zip_code")
            agg_df = _df.groupby(by="house_id", as_index=False).agg(
                {"park_name": pd.Series.nunique, "feature_desc": pd.Series.nunique}
            )

            ## Feature Set Generation for ML
            # Get num_parks and num_park_features in zip code of house
            new_df = _df.select(...).join(agg_df, on="house_id")

            # Get one hot encoding of category col like house_type
            one_hot = pd.get_dummies(new_df["house_type"])
            new_df = new_df.drop("house_type", axis=1)
            new_df = new_df.join(one_hot)

            # Other Feature generations
            feature_set = ...

            ## Write to Gold Layer S3
            write_s3_file("test_bucket", "test_file", _pd_to_csv(feature_set))

            response = {"Status": "Passed"}
        except Exception as e:
            logger.error(f"Failed transforming the data due to: {e}")
            response = {"Status": "Failed", "ErrorMessage": e}
        finally:
            return response
