import argparse
import logging

logger = logging.getLogger(__name__)
logger.setLevel("INFO")


class ETL(Enum):
    extract = "extract"
    transform = "transform"
    load = "load"

    def __str__(self):
        return self.value


def main(args):
    try:
        job_type = args["job_type"]
        module = __import__(f"etl.{job_type}")
        _class = getattr(module, args["class"])
        instance = _class(**args["job_args"])
    except Exception as e:
        logger.exception(f"Job failed due to: {e}")
    else:
        return getattr(instance, job_type)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ETL Job Args")
    parser.add_argument("--class", type=str, required=True, help="Job to Run")
    parser.add_argument(
        "--job_args",
        nargs="*",  # TODO: Find a better approach to dealing with args
        help="args to class in right order",
    )
    parser.add_argument("--job_type", type=ETL, choices=list(ETL), help="args to class in right order")

    args = parser.parse_args()
    main(vars(args))
