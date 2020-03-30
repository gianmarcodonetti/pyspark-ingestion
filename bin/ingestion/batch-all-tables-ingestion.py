import argparse
import logging
import os
from getpass import getpass

from pyspark_ingestion import constants as C, spark as sparkaras
from pyspark_ingestion.ingestion.ingestion import ingestion_step
from pyspark_ingestion.utils.security import decrypt_json

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

DEFAULT_BASE_PATH = os.path.join(os.sep, 'tmp', 'data', 'l0')
SETTINGS_ENCRYPTED = 'settings.json.pydes'


def create_parser():
    prs = argparse.ArgumentParser(description="Ingestion from the DBMS you like.")
    prs.add_argument('system', type=str, help="Name of the system to connect with.")
    prs.add_argument('base_path', nargs='?', default=None, type=str, help="Base path where to store ingested data.")
    prs.add_argument('file_format', nargs='?', default=None, type=str, help="File format to persist data.")
    return prs


if __name__ == '__main__':
    logging.info(__file__)
    parser = create_parser()
    args = parser.parse_args()
    _system = args.system
    _base_path = os.path.join(args.base_path, _system) if args.base_path else os.path.join(DEFAULT_BASE_PATH, _system)
    _file_format = args.file_format

    logging.info("You are asking for:\n\tIngestion from '{}', all tables, towards '{}'".format(_system, _base_path))

    _password = getpass("Encryption key for settings file:\n")
    settings_filename = SETTINGS_ENCRYPTED
    _settings_dict = decrypt_json(settings_filename, _password)['settings']
    logging.info("Settings decrypted successfully.")

    _spark = sparkaras.get_spark_session(
        driver='/Users/gianmarco.donetti/Downloads/sqljdbc_6.0/enu/jre8/sqljdbc42.jar',
        executor_memory='8g',
        driver_memory='8g',
        master='local[*]',
        app_name='pyspark_ingestion',
        shuffle_partitions='40'
    )

    all_tables = _settings_dict[C.CONNECTION][_system][C.TABLE_SETTINGS].keys()
    logging.info("Ingestion prepared for tables: {}".format(all_tables))
    for _table_name in all_tables:
        logging.info("Launching ingestion step for '{}'.".format(_table_name))
        _table_path = os.path.join(_base_path, _table_name)
        ingestion_step(_spark, _system, _table_name, _table_path, settings_dict=_settings_dict,
                       file_format=_file_format)
        logging.info("Ingestion completed for '{}'.".format(_table_name))

    _spark.stop()
