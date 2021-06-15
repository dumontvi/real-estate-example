from io import StringIO

import pandas as pd
from etl.common.aws_utils import write_s3_file
from etl.common.base_etl import Extract
from etl.common.helper import _pd_to_csv
from requests.exceptions import HTTPError
from sodapy import Socrata


class APIExtract(Extract):
    def __init__(self, base_api_url, dataset_id, report_date):
        self.base_api_url = base_api_url
        self.dataset_id = dataset_id
        self.report_date = report_date

    def extract(self):
        client = self._socrata_init()

        try:
            results = client.get(dataset_id)
        except HTTPError as e:
            logger.error(f"GET Failed to retrieve dataset: {e}")
            response = {"Status": "Failed", "ErrorMessage": e}
        except Exception as e:
            logger.exception(f"Unknown error while retrieving dataset: {e}")
            response = {"Status": "Failed", "ErrorMessage": e}
        else:
            results_df = pd.DataFrame.from_records(results)
            # Generate key based on dataset class name, date for prod purposes
            write_s3_file("test_bucket", "test_key", _pd_to_csv(results_df))
            response = {"Status": "Success"}
        finally:
            return response

    def _socrata_init(self):
        """
        Intializes the Socrata API w/ application token
        - Application Token seems to provide us with higher rate limits

        *For purpose of this demo, will not use app token
        """
        # app_token = retrieve_secrets(secret_id)
        return Socrata(self.base_api_url, None)
