import argparse
import json
import logging
import os

import pandas as pd
from pyspark_ingestion import constants as C

TABLE_SCHEMA = 'TABLE_SCHEMA'
TABLE_NAME = 'TABLE_NAME'
COLUMN_NAME = 'COLUMN_NAME'
INCREMENTAL_REFERENCE = 'INCREMENTAL_REFERENCE'
TABLE_RESOLUTION = 'TABLE_RESOLUTION'

DEFAULT_FILE_PATH = os.path.join('data', 'excel', 'C1 - GCP Export_Final.xlsx')
DEFAULT_OUTPUT = os.path.join('table-settings.json')

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')


def main(file_path):
    excel = pd.read_excel(file_path, None)

    df = pd.concat(excel).reset_index(drop=True)

    settings = dict()

    df[TABLE_RESOLUTION] = df[TABLE_SCHEMA] + '.' + df[TABLE_NAME]

    for table_name in df[TABLE_RESOLUTION].unique():
        sub_df = df[df[TABLE_RESOLUTION] == table_name]

        columns_to_import = sub_df[COLUMN_NAME].unique().tolist()

        incremental_ref_df = sub_df[sub_df[INCREMENTAL_REFERENCE].notnull()]
        if not incremental_ref_df.empty:
            incremental_ref = incremental_ref_df[COLUMN_NAME].values[0]
        else:
            incremental_ref = ''

        settings[table_name] = {
            C.REF_COLUMN: incremental_ref,
            C.COLUMNS_TO_IMPORT: columns_to_import,
            C.REF_FIRST_VALUE: "2019-4-30T00:00:00.000Z"
        }

    logging.info("Settings dictionary created, length: {}".format(len(settings)))

    with open(DEFAULT_OUTPUT, 'w') as fout:
        json.dump(settings, fout)

    logging.info("Settings file created at: '{}'".format(DEFAULT_OUTPUT))

    return


def create_parser():
    prs = argparse.ArgumentParser(description="Parsing excel data dictionary.")
    prs.add_argument('file_path', type=str, nargs='?', default=DEFAULT_FILE_PATH, help="Path to the excel file.")
    return prs


if __name__ == '__main__':
    logging.info(__file__)
    parser = create_parser()
    args = parser.parse_args()
    _file_path = args.file_path

    logging.info("You are asking to parse '{}' file".format(_file_path))
    main(_file_path)
