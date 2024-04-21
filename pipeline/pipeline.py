import pandas as pd
import logging
from datetime import datetime
import os
import argparse
from typing import Tuple
import multiprocessing
from functions import *


def parse_year_range(year_range: str) -> Tuple[int, int]:
    try:
        values = year_range.split('-')
        if len(values) == 1:
            start_year = int(values[0])
            end_year = start_year + 1
        elif len(values) == 2:
            start_year = int(values[0])
            end_year = int(values[1])
            if start_year >= end_year:
                raise argparse.ArgumentTypeError(
                    "Start year must be less than end year")
        else:
            raise argparse.ArgumentTypeError("Not valid year format")
        return (start_year, end_year)
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"Year range must be in 'yyyy-yyyy' format. Received: {year_range}")


def parse_state_range(state_range: str) -> Tuple[int, int]:
    try:
        values = state_range.split('-')
        if len(values) == 1:
            start_state = int(values[0])
            end_state = start_state + 1
        elif len(values) == 2:
            start_state = int(values[0])
            end_state = int(values[1])
            if start_state >= end_state:
                raise argparse.ArgumentTypeError(
                    "Start state must be less than end state")
        else:
            raise argparse.ArgumentTypeError("Not valid year format")
        return (start_state, end_state)
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"State range must be in 'int-int' format. Received: {state_range}")


def configure_logging(logging_folder: str, year: int, state: int) -> None:
    try:
        logging_file = f'logs/{logging_folder}/run_{year}_{state}.log'
        check_and_create_folder(logging_file)
        for handler in logging.root.handlers:
            logging.root.removeHandler(handler)
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
                            filename=logging_file, encoding='utf-8')
    except Exception as e:
        raise Exception(f'Failed to configure logging because of:\n{e}')


def run_pipeline(url: str, year: int, state: int, logging_folder: str):
    configure_logging(logging_folder, year, state)
    try:
        logging.info(f"Run {year}:{state} STARTED")
        html = fetch_html(url)
        logging.info(f"Successfully fetched data from {url}")
        if html:
            data_object = parse_html(html)
            logging.info(f"HTML data parsed")
            for key in data_object.keys():
                clean_data = transform_data(
                    data_object[key]['data'], data_object[key]['headers'])
                df = pd.DataFrame(clean_data, columns=clean_data.keys())
                logging.info(f"DataFrame created for {key}")
                columns = list(clean_data.keys())
                df = transform_numeric_types(df, columns)
                df = handle_descriptors(df, columns[0])
                logging.info(f"DataFrame columns transformed for {key}")
                file_name = f"annual_report/{year}/{state}/{key.replace(' ', '_').lower()}"
                logging.info(f"Starting file write for {key}")
                save_to_parquet(df, file_name)
                logging.info(f"File saved at {file_name}")
        logging.info("Run succeeded")
    except Exception as e:
        logging.error(f"Run failed due to error {str(e)}")


def pipeline_wrapper(parameters):
    return run_pipeline(*parameters)


if __name__ == "__main__":

    # Parse command line arguments, if none are given it will use the defaults.
    # If you want to manually run a smaller selection without playing in the command line,
    # just change default values.
    parser = argparse.ArgumentParser(
        description="Process year and state ranges.")
    parser.add_argument("--year_range", type=parse_year_range, default=(2016, 2021),
                        help="Year range in the format 'yyyy-yyyy', default is 2016-2021")
    parser.add_argument("--state_range", type=parse_state_range, default=(1, 58),
                        help="State range in the format 'int-int', default is 1-58")
    args = parser.parse_args()

    parameters = []
    date = datetime.now().strftime('%Y%m%d_%H%M%S')
    for year in range(args.year_range[0], args.year_range[1]):
        for state in range(args.state_range[0], args.state_range[1]):
            url = f"https://www.ic3.gov/Media/PDF/AnnualReport/{year}State/StateReport.aspx#?s={state}"
            parameters.append((url, year, state, date))

    num_cores = os.cpu_count() - 1
    with multiprocessing.Pool(num_cores) as pool:
        results = pool.map(pipeline_wrapper, parameters)
