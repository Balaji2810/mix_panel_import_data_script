import logging
import dump_load
import mongodb_load
import argparse
import datetime
import re

logging.basicConfig(filename="log.txt", level=logging.DEBUG)


def regex_timezone(
    arg_value, pat=re.compile(r"^(?:(?:[+-](?:1[0-4]|0[1-9]):[0-5][0-9])|[+-]00:00)$")
):
    arg_value = arg_value.replace("'", "")
    arg_value.replace('"', "")
    if not pat.match(arg_value):
        raise argparse.ArgumentTypeError("invalid value")
    return arg_value


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    g = parser.add_mutually_exclusive_group(
        required=True,
    )
    g.add_argument(
        "-m",
        "--mongodb",
        help="Fetch data from mongodb, it takes two args start and end date both in the format of yyyy-mm-dd",
        nargs=2,
        type=datetime.datetime.fromisoformat,
        metavar=("start_date", "end_date"),
    )
    parser.add_argument(
        "-t",
        "--timezone",
        help="Timezone offset for start and end date",
        default="+00:00",
        type=regex_timezone,
    )
    g.add_argument(
        "-d",
        "--dumps",
        help="Fetch data from 'S3' or 'local' dumps",
    )

    args = parser.parse_args()
    if args.mongodb:
        # Import data from MongoDB to MixPanel
        mongodb_load.start(args.mongodb[0], args.mongodb[1], args.timezone)
    if args.dumps:
        # Import data from S3 or local dump(gzip) file
        dump_load.start(args.dumps)
