import json
import os

import boto3
import psycopg2

ENV = {"region": os.getenv("region", "us-east-1")}

S3 = boto3.client("s3", region=ENV["region"])
SM_CLIENT = boto3.client("secretsmanager", region=ENV["region"])


def retrive_secrets(secret_id: str):
    """
    Retrieves secret values from Secrets Manager

    ** Can be used to store app tokens and other 3rd party api keys
    """
    try:
        if secret_id:
            response = SM_CLIENT.get_secret_value(secret_id)
            secret_value = response["SecretString"]
        else:
            raise ValueError("No secret id provided")
    except Exception as e:
        raise e
    else:
        return secret_value


def read_s3_file(bucket: str, key: str):
    """
    Reads a single file from S3

    ** Can implement another function to read in S3 in batch
    """
    try:
        obj = S3.get_object(Bucket=bucket, Key=key)
    except Exception as e:
        logger.error(f"Error on GET operation from S3: {bucket}/{key}")
    else:
        return json.loads(obj["Body"].read().decode("utf-8"))


def write_s3_file(bucket: str, key: str, file_content):
    """
    Writes a single file from S3

    ** Can implement another function to read in S3 in batch
    """

    try:
        response = S3.put_object(Bucket=bucket, Key=key, Body=file_content)

        status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

        if status == 200:
            print(f"Successful PUT to S3 - StatusCode: {status}")
        else:
            raise Exception(f"Unable to PUT to S3: {response.error}")
    except Exception as e:
        logger.error(f"Failed to write to S3: {e}")


##### REDSHIFT ######


class RedshiftConnector:
    def __init__(self, db_creds: dict):
        self.conn = connect_to_redshit(db_creds)
        self.cur = self.conn.cursor()

    def connect_to_redshit(self, db_creds: dict):
        creds = retrive_secrets(f"/{db_creds['host']}")
        conn = psycopg2.connect(
            dbname=db_creds["db_name"],
            user=creds["user"],
            host=db_creds["host"],
            port=db_cred["port"],
            password=creds["password"],
        )
        return conn

    def execute_query(self, query: str):
        try:
            self.cur.execute(query)
            result = cur.fetchall()
            self.cur.execute("commit")
        except Exception as e:
            logger.error(f"Unable to Execute {query}")
            self.cur.execute("rollback")
        else:
            return result

    def close_connection(self):
        self.cur.close()
        self.conn.close()

    def __del__(self):
        self.close_connection()
