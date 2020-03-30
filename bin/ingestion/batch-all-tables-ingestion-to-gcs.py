import argparse
import json
import logging
import os
import shutil
import sys
from datetime import datetime
from distutils.dir_util import copy_tree
from getpass import getpass

from google.cloud import storage

from pyspark_ingestion import constants as C, spark as sparkaras
from pyspark_ingestion.ingestion.ingestion import ingestion_step, copy_local_directory_to_gcs
from pyspark_ingestion.utils.security import decrypt_json

DEFAULT_BASE_PATH = os.path.join(os.sep, 'tmp', 'data', 'l0')

SETTINGS_ENCRYPTED = 'settings.json.pydes'
GOOGLE_APPLICATION_CREDENTIALS = 'GOOGLE_APPLICATION_CREDENTIALS'
BLOB = 'l0/c1/'


def set_logger():
    def is_compiled():
        return hasattr(sys, "frozen")  # is the program compiled?

    handlers = [logging.StreamHandler(sys.stdout)]
    if not is_compiled():
        pwd = os.getcwd()
        pre_log_folder = pwd.split('bin')[0]
        post_log_folder = __file__.split('bin/')[-1].split('.py')[0]
        log_dir = os.path.join(pre_log_folder, 'log', 'bin', post_log_folder)
        log_filename = datetime.now().strftime('%Y-%m-%d_%H-%M-%S') + '.log'
        handlers.append(logging.FileHandler("{}".format(os.path.join(log_dir, log_filename))))
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - {%(module)s:%(lineno)s} - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=handlers
    )


def create_parser():
    prs = argparse.ArgumentParser(description="Ingestion from the DBMS you like.")
    prs.add_argument('system', type=str, help="Name of the system to connect with.")
    prs.add_argument('base_path', nargs='?', default=None, type=str, help="Base path where to store ingested data.")
    prs.add_argument('file_format', nargs='?', default=None, type=str, help="File format to persist data.")
    prs.add_argument('count', nargs='?', default='y', type=str,
                     help="Whether to perform a table count before the ingestion")
    return prs


if __name__ == '__main__':
    set_logger()
    logging.info(__file__)
    parser = create_parser()
    args = parser.parse_args()
    logging.info("Args: {}".format(args))
    _system = args.system
    _base_path = os.path.join(args.base_path, _system) if args.base_path else os.path.join(DEFAULT_BASE_PATH, _system)
    _file_format = args.file_format
    _counting = True if args.count.lower() == 'y' else False

    logging.info("You are asking for:\n\tIngestion from '{}', all tables, towards '{}'".format(_system, _base_path))

    _password = getpass("Encryption key for settings file:\n")
    settings_filename = SETTINGS_ENCRYPTED
    _settings_dict = decrypt_json(settings_filename, _password)['settings']
    logging.info("Settings decrypted successfully, with content:\n{}.".format(
        json.dumps(_settings_dict, sort_keys=True,
                   indent=4, separators=(',', ': '))
    ))

    _spark = sparkaras.get_spark_session(
        driver='/Users/gianmarco.donetti/Downloads/sqljdbc_6.0/enu/jre8/sqljdbc42.jar',
        executor_memory='10g',
        driver_memory='10g',
        master='local[*]',
        app_name='pyspark-ingestion',
        shuffle_partitions='40'
    )
    storage_client = storage.Client.from_service_account_json(os.environ.get(GOOGLE_APPLICATION_CREDENTIALS))
    bucket = storage_client.get_bucket(_settings_dict['connection']['gcp']['landing-bucket'])

    all_tables = _settings_dict[C.CONNECTION][_system][C.TABLE_SETTINGS].keys()
    logging.info("Ingestion prepared for tables: {}".format(all_tables))
    for _table_name in all_tables:
        # Local Ingestion
        _table_path = os.path.join(_base_path, _table_name)
        logging.info("Launching ingestion step for '{}', towards '{}'.".format(_table_name, _table_path))
        moved_something = ingestion_step(_spark, _system, _table_name, _table_path, settings_dict=_settings_dict,
                                         file_format=_file_format, counting=_counting)
        logging.info("Ingestion completed for '{}'.".format(_table_name))

        if moved_something:
            # Moving to GCS
            local_path = _table_path
            gcs_path = BLOB + _file_format + '/' + _table_name
            logging.info("Moving data to GCS, bucket '{}', path '{}'.".format(bucket.name, gcs_path))
            copy_local_directory_to_gcs(local_path, bucket, gcs_path)
            logging.info("Data moved to bucket '{}', path '{}'.".format(bucket.name, gcs_path))

            # Archiving local data
            src = local_path
            dst = os.path.join(os.sep, 'tmp', 'archive', os.path.abspath(src).lstrip('.~/'))
            logging.info("Archiving local data, from source '{}' to destination '{}'.".format(src, dst))

            copy_tree(src, dst)

            # shutil.copytree(src, dst)

            shutil.rmtree(src)
            os.mkdir(src)
            shutil.copy(os.path.join(dst, 'sync.json'), os.path.join(src, 'sync.json'))
            logging.info("Local data archived.")

    _spark.stop()
