import numpy as np
import pandas as pd
from etl.common.aws_utils import read_s3_file
from etl.common.aws_utils import write_s3_file
from etl.common.base_etl import Transform
from etl.common.helper import _flatten
from etl.common.helper import _pd_to_csv


class HouseWParksPredict(Transform):
    def __init__(self, report_date):
        self.report_date = report_date

    def transform(self):
        try:
            # Key generation based on date and file path
            ## King County House dataset
            ## Can read historical data (many dates) from S3 for training, only today required for prediction
            feature_set = pd.read_csv(read_s3_file("test_bucket", "house_w_park_feature_set.csv"))

            model = load_model_from_s3("model_s3_uri")

            predicted_output = self._predict(model, feature_set)

            ## Write to Gold Layer S3
            write_s3_file("test_bucket", "test_file", _pd_to_csv(predicted_output))

            response = {"Status": "Passed"}
        except Exception as e:
            logger.error(f"Failed transforming the data due to: {e}")
            response = {"Status": "Failed", "ErrorMessage": e}
        finally:
            return response

    def _predict(self, model, dataset):
        return model.predict(dataset)
