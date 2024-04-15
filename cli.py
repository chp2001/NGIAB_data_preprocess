import argparse
from data_processing.file_paths import file_paths
from data_processing.subset import subset
from data_processing.forcings import create_forcings
from data_processing.create_realization import create_realization
import os
import sys
from datetime import datetime
from pathlib import Path
import pickle
from functools import cache

@cache
def get_cached():
    cached_settings = file_paths.root_output_dir() / "cached_settings.pkl"
    if cached_settings.exists():
        with cached_settings.open("rb") as f:
            return pickle.load(f)
    return {
        "args[]": [],
        "settings": {}
    }

def save_cached(data):
    if not data or not isinstance(data, dict) or len(data) == 0:
        return
    cached_settings = file_paths.root_output_dir() / "cached_settings.pkl"
    with cached_settings.open("wb") as f:
        pickle.dump(data, f)

def main():
    # Create the parser
    my_parser = argparse.ArgumentParser(
        description="Subsetting hydrofabrics, forcing generation, and realization creation"
    )

    # Add input arguments
    my_parser.add_argument(
        "-i",
        "--input_file",
        type=str,
        help="Path to a csv or txt file containing a list of waterbody IDs",
        default=None,
    )
    my_parser.add_argument(
        "-s",
        "--subset",
        action="store_true",
        help="Subset the hydrofabric to the given waterbody IDs",
    )
    my_parser.add_argument(
        "-f",
        "--forcings",
        action="store_true",
        help="Generate forcings for the given waterbody IDs",
    )
    my_parser.add_argument(
        "-r",
        "--realization",
        action="store_true",
        help="Create a realization for the given waterbody IDs",
    )
    my_parser.add_argument(
        "--start_date",
        type=lambda s: datetime.strptime(s, "%Y-%m-%d"),
        help="Start date for forcings/realization (format YYYY-MM-DD)",
        default=datetime.strptime("2010-01-01", "%Y-%m-%d"),
    )
    my_parser.add_argument(
        "--end_date",
        type=lambda s: datetime.strptime(s, "%Y-%m-%d"),
        help="End date for forcings/realization (format YYYY-MM-DD)",
        default=datetime.strptime("2010-01-02", "%Y-%m-%d"),
    )
    my_parser.add_argument(
        "-o",
        "--output_name",
        type=str,
        help="Name of the subset to be created (default is the first waterbody ID in the input file)",
    )

    # Execute the parse_args() method
    args = my_parser.parse_args()

    # Get cached settings in case the args are incomplete
    cached = get_cached()
    cache_args = cached.get("args[]", [])
    cache_settings = cached.get("settings", {})

    primaries = ["subset", "forcings", "realization"]
    if not any([args.__dict__[p] for p in primaries]):
        if any([p in cache_args for p in primaries]):
            replacing = [p for p in primaries if p in cache_args and not getattr(args, p)]
            replace_values = [cache_settings[p] for p in replacing]

            if len(replacing) > 0:
                query = input(f"Use cached {replacing}, {replace_values}? (Y/n): ")
                if query.lower() in ["y", "yes", ""]:
                    for p in replacing:
                        setattr(args, p, cache_settings[p])
                        
            if not any([args.__dict__[p] for p in primaries]):
                print("At least one of --subset, --forcings, or --realization must be set.")
                sys.exit()
        else:
            print("At least one of --subset, --forcings, or --realization must be set.")
            sys.exit()

    num_primaries = [args.__dict__[p] for p in primaries].count(True)

    if not args.input_file and "input_file" in cache_settings:
        response = input(f"Use cached input file, {cache_settings['input_file']}? (Y/n): ")
        if response.lower() in ["y", "yes", ""]:
            args.input_file = cache_settings["input_file"]

    if args.subset:
        if not args.input_file:
            print("Subset requires an input file.")
            sys.exit()
        input_file = Path(args.input_file)

        # Check if the input file exists
        if not input_file.exists():
            print(f"The file {input_file} does not exist")
            sys.exit()

        # Validate the file type
        if input_file.suffix not in [".csv", ".txt", ""]:
            print(f"Unsupported file type: {input_file.suffix}")
            sys.exit()

        # Read in the waterbody IDs
        with input_file.open("r") as f:
            waterbody_ids = f.read().splitlines()

        if num_primaries > 1:
            print("Parsed input file for waterbody IDs.")

    wb_id_for_name = None
    if args.output_name:
        wb_id_for_name = args.output_name
        
    elif args.subset and waterbody_ids:
        wb_id_for_name = waterbody_ids[0]

    elif "input_file" in cache_settings:
        fname = Path(cache_settings["input_file"]).stem
        response = input(f"Use cached input file name as output name, {fname}? (Y/n): ")
        if response.lower() in ["y", "yes", ""]:
            wb_id_for_name = fname

    if not wb_id_for_name:
        print("No waterbody input file or output folder provided.")
        sys.exit()

    paths = file_paths(wb_id_for_name)
    output_folder = paths.subset_dir()

    print(f"Output folder: {output_folder}")

    # Create the output folder if it does not exist
    if not output_folder.exists():
        output_folder.mkdir(parents=True)

    # If the subset flag is set, run the subset function
    if args.subset:
        subset(waterbody_ids, subset_name=output_folder.name)

        if num_primaries > 1:
            print(f"Subset operation complete. Output saved to {output_folder}")

    # If the forcings flag is set, run the forcings function
    if args.forcings:
        required = ["start_date", "end_date"]
        if not all([args.__dict__[i] for i in required]):
            replacing = [p for p in required if cache_settings.get(p) and not getattr(args, p)]
            replace_values = [cache_settings[p] for p in replacing]
            if len(replacing) > 0:
                query = input(f"Use cached {replacing}, {replace_values}? (Y/n): ")
                if query.lower() in ["y", "yes", ""]:
                    for p in replacing:
                        setattr(args, p, cache_settings[p])

            if not all([getattr(args, i) for i in required]):
                print("Forcings generation requires both --start_date and --end_date to be provided.")
                sys.exit()

        gpkg_path = paths.geopackage_path()
        if not gpkg_path.exists():
            print(f"Subset geopackage not found: {gpkg_path}")
            sys.exit()

        create_forcings(
            start_time=args.start_date, end_time=args.end_date, output_folder_name=output_folder.name
        )

        if num_primaries > 1:
            print(f"Forcings generation complete. Output saved to {output_folder}")

    # If the realization flag is set, run the realization function
    if args.realization:
        required = ["start_date", "end_date"]
        if not all([args.__dict__[i] for i in required]):
            replacing = [p for p in required if p in cache_settings and not getattr(args, p)]
            replace_values = [cache_settings[p] for p in replacing]
            if len(replacing) > 0:
                query = input(f"Use cached {replacing}, {replace_values}? (Y/n): ")
                if query.lower() in ["y", "yes", ""]:
                    for p in replacing:
                        setattr(args, p, cache_settings[p])

        if not args.start_date or not args.end_date:
            print("Realization creation requires both --start_date and --end_date to be provided.")
            sys.exit()

        forcing_path = paths.forcings_dir()
        if not forcing_path.exists():
            print(f"Forcing folder not found: {forcing_path}")
            sys.exit()

        create_realization(wb_id_for_name, start_time=args.start_date, end_time=args.end_date)

        if num_primaries > 1:
            print(f"Realization creation complete. Output saved to {output_folder}")

    # Save the settings to the cache
    args_dict = vars(args)
    save_cached({"args[]": [k for k, v in args_dict.items() if v], "settings": args_dict})


if __name__ == "__main__":
    main()
