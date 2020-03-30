import argparse
import logging
import os
import sys
from datetime import datetime
from getpass import getpass

from pyspark_ingestion import spark as sparkaras
from pyspark_ingestion.ingestion.ingestion import ingestion_step
from pyspark_ingestion.utils.security import decrypt_json

DEFAULT_BASE_PATH = os.path.join(os.sep, 'tmp', 'data', 'l0')
SETTINGS_ENCRYPTED = 'settings.json.pydes'


def set_logger(project_name='pyspark_ingestion'):
    pwd = os.getcwd()
    log_dir = os.path.join(pwd.split(project_name)[0],
                           project_name,
                           'log',
                           __file__.split(project_name)[-1].split('.py')[0]
                           )
    log_filename = datetime.now().strftime('%Y-%m-%d_%H-%M-%S') + '.log'
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[
            logging.FileHandler("{}".format(os.path.join(log_dir, log_filename))),
            logging.StreamHandler(sys.stdout)
        ]
    )


def create_parser():
    prs = argparse.ArgumentParser(description="Ingestion from the DBMS you like.")
    prs.add_argument('system', type=str, help="Name of the system to connect with.")
    prs.add_argument('table_name', type=str, help="Name of the table to ingest.")
    prs.add_argument('base_path', nargs='?', default=None, type=str, help="Base path where to store ingested data.")
    prs.add_argument('file_format', nargs='?', default=None, type=str, help="File format to persist data.")
    return prs


if __name__ == '__main__':
    set_logger('pyspark_ingestion')
    logging.info(__file__)
    parser = create_parser()
    args = parser.parse_args()
    _system = args.system
    _table_name = args.table_name
    _base_path = os.path.join(args.base_path, _system) if args.base_path else os.path.join(DEFAULT_BASE_PATH, _system)
    _file_format = args.file_format

    logging.info("You are asking for:\n\tIngestion from '{}' @ '{}', towards '{}'".format(
        _system, _table_name, _base_path)
    )

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

    logging.info("Launching main job.")
    _table_path = os.path.join(_base_path, _table_name)
    ingestion_step(_spark, _system, _table_name, _table_path, settings_dict=_settings_dict, file_format=_file_format)
    logging.info("Ingestion completed.")

    _spark.stop()
