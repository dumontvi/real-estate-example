from dq.common import nonzero_count_check
from etl.common.aws_utils import read_s3_file


def main():
    df = read_s3_file("bucket", "park_s3_uri")

    # Example Non Zero Check
    assert nonzero_count_check(df)


if __name__ == "__main__":
    main()
