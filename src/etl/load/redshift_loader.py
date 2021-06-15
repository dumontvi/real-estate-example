import os

from etl.common.aws_utils import RedshiftConnector
from etl.common.base_etl import Loader


class RedshiftLoader(Loader):
    def __init__(self, db_creds, s3_file_path):
        """
        db_creds will have things like table name, role, schema, db name ...
        """
        self.db_creds = db_creds
        self.s3_file_path = s3_file_path

    def load(self):
        try:
            rc = RedshiftConnector(self.db_creds)
            result = rc.execute(
                f"""
                COPY {db_creds['table_name']}
                FROM {self.s3_file_path}
                CREDENTIALS 'aws_access_key_id={os.getenv('AWS_KEY_ID')};aws_secret_access_key={os.getenv('AWS_SECRET_KEY_ID')}'
                delimiter ',' removequotes;
            """
            )  # Better way to access aws_access_key, secret key (maybe via secretsmanager)

            ## Log rows copied count and other metrics
            self._log_results(result)
            response = {"Status": "Success"}
        except Exception as e:
            logger.error("Failed to Load data into redshift")
            response = {"Status": "Failed", "ErrorMessage": str(e)}
        finally:
            return response

    def _log_results(self, results):
        """
        Log any metrics we want to keep track of
        """
        pass
