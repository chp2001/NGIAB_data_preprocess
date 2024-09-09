import argparse
from datetime import datetime

# Constants
DATE_FORMAT = "%Y-%m-%d"  # used for datetime parsing
DATE_FORMAT_HINT = "YYYY-MM-DD"  # printed in help message


def parse_arguments() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Subsetting hydrofabrics, forcing generation, and realization creation"
    )
    parser.add_argument(
        "-i",
        "--input_feature",
        "--input_file",
        type=str,
        help="ID of feature to subset, providing a prefix will automatically convert to catid, \n e.g. cat-5173 or gage-01646500 or wb-1234",
        required=True,
    )
    parser.add_argument(
        "-l",
        "--latlon",
        action="store_true",
        help="Use lat lon instead of catid, expects \
            comma separated via the cli \n e.g. python -m ngiab_data_cli -i 54.33,-69.4 -l -s",
    )
    parser.add_argument(
        "-g",
        "--gage",
        action="store_true",
        help="Use gage ID instead of catid, expects a single gage ID via the cli \n e.g. python -m ngiab_data_cli -i 01646500 -g -s",
    )
    parser.add_argument(
        "-s",
        "--subset",
        action="store_true",
        help="Subset the hydrofabric to the given feature",
    )
    parser.add_argument(
        "-f",
        "--forcings",
        action="store_true",
        help="Generate forcings for the given feature",
    )
    parser.add_argument(
        "-r",
        "--realization",
        action="store_true",
        help="Create a realization for the given feature",
    )
    parser.add_argument(
        "--start_date",
        "--start",
        type=lambda s: datetime.strptime(s, DATE_FORMAT),
        help=f"Start date for forcings/realization (format {DATE_FORMAT_HINT})",
    )
    parser.add_argument(
        "--end_date",
        "--end",
        type=lambda s: datetime.strptime(s, DATE_FORMAT),
        help=f"End date for forcings/realization (format {DATE_FORMAT_HINT})",
    )
    parser.add_argument(
        "-o",
        "--output_name",
        type=str,
        help="Name of the output folder",
    )
    parser.add_argument(
        "-D",
        "--debug",
        action="store_true",
        help="enable debug logging",
    )
    parser.add_argument(
        "--run", action="store_true", help="Automatically run Next Gen against the output folder"
    )
    parser.add_argument(
        "--validate", action="store_true", help="Run every missing step required to run ngiab"
    )
    parser.add_argument(
        "--eval", action="store_true", help="Evaluate perforance of the model after running"
    )
    parser.add_argument(
        "-a",
        "--all",
        action="store_true",
        help="Run all operations: subset, forcings, realization, run Next Gen, and evaluate",
    )

    args = parser.parse_args()

    if args.all:
        args.subset = True
        args.forcings = True
        args.realization = True
        args.run = True
        args.eval = True

    if args.run:
        args.validate = True

    return args