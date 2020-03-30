import glob
import json
import logging
import os
from datetime import datetime

from google.cloud.storage.bucket import Bucket
from pyspark.sql import functions as F

from pyspark_ingestion import constants as C, spark as sparkaras
from pyspark_ingestion.ingestion import config

DEFAULT_FETCHSIZE = "10000"


def ingestion_step(spark, system, table_name, table_path, settings_dict, file_format='parquet', counting=True):
    """

    Args:
        spark: Spark Session
        system: string
        table_name: string
        table_path: string
        settings_dict: dict
        file_format: string
        counting: boolean

    Returns:

    """
    # Read sync file, that is in the same target folder of the table data
    sync = read_sync_file(table_path, system, table_name, settings_dict)

    # Get connection parameters
    dbms = settings_dict[C.CONNECTION][system][C.DBMS]
    jdbc_url = config.config_dict['dbms-to-url-function'][dbms](
        settings_dict[C.CONNECTION][system][C.USER],
        settings_dict[C.CONNECTION][system][C.PASSWORD],
        settings_dict[C.CONNECTION][system][C.HOST],
        settings_dict[C.CONNECTION][system][C.PORT],
        settings_dict[C.CONNECTION][system][C.SERVICE]
    )
    ref_column = settings_dict[C.CONNECTION][system][C.TABLE_SETTINGS][table_name][C.REF_COLUMN]
    repartitions = 8 * 4

    # Read dataframe from server with spark
    jdbc_df = (sparkaras.read_dataframe_jdbc(
        spark=spark,
        table_name=table_name,
        jdbc_url=jdbc_url,
        driver=settings_dict[C.CONNECTION][system][C.DRIVER],
        fetchsize=settings_dict[C.CONNECTION][system][C.TABLE_SETTINGS][table_name].get(C.FETCHSIZE, DEFAULT_FETCHSIZE))
               .repartition(repartitions)
               .filter(F.col(ref_column).isNotNull())
               # .persist()
               )

    logging.info("JDBC DF schema:")
    jdbc_df.printSchema()

    # Do dataframe simple preparation and create columns for partitioning
    table_settings = settings_dict[C.CONNECTION][system][C.TABLE_SETTINGS][table_name]
    prep_df, partitioning_columns = config.config_dict['system-to-prep-function'][system](
        jdbc_df, table_settings, sync[C.SYNC]
    )

    # Log count and write partitioned
    prep_df_persisted = prep_df.persist()

    logging.info("PREPARED DF schema:")
    prep_df_persisted.printSchema()

    is_empty = prep_df_persisted.rdd.isEmpty()
    if not is_empty:
        if counting:
            tot_count = prep_df_persisted.count()
            logging.info("Table to write count: {}".format(tot_count))
        else:
            logging.info("Table needs to be updated.")
        sparkaras.write_partitioned(prep_df_persisted,
                                    path_to_write=table_path,
                                    partition_columns=partitioning_columns,
                                    mode='append',
                                    compression='gzip',
                                    file_format=file_format)

        # Write sync
        new_sync = sync.copy()
        max_col = 'max'
        new_sync[C.SYNC][C.REF_LAST_VALUE] = (prep_df_persisted
                                              .select(F.max(table_settings[C.REF_COLUMN]).alias(max_col))
                                              .collect()[0]
                                              .asDict()[max_col]
                                              .strftime(C.DATETIME_FORMAT)
                                              )
        write_sync_file(new_sync, table_path)
        moving_something = True
    else:
        logging.info("No new records to append.")
        moving_something = False

    prep_df_persisted.unpersist()
    return moving_something


def copy_local_directory_to_gcs(local_path: str, bucket: Bucket, gcs_path: str):
    """Copy the content of a local folder to a specified path in Google Cloud Storage.

    Args:
        local_path: str
        bucket: gcloud bucket object
        gcs_path: str

    Returns:

    """
    assert os.path.isdir(local_path), "'{}' is not a dir".format(local_path)

    def _copy_local_directory_to_gcs(_local_path: str, _bucket: Bucket, _gcs_path: str, _n_to_remove: int = 0):
        for local_file in glob.glob(_local_path + '/**'):
            if not os.path.isfile(local_file):
                _copy_local_directory_to_gcs(local_file, _bucket, _gcs_path, _n_to_remove)
                continue
            remote_path_tmp = os.path.join(_gcs_path, local_file[_n_to_remove:])
            if 'part' in remote_path_tmp:
                now = datetime.now()
                day, hour = now.day, now.hour
                folder = '/'.join(remote_path_tmp.split('/')[:-1]) + '/'
                file_name = remote_path_tmp.split('/')[-1]
                remote_path = (folder
                               + file_name.split('.')[0]
                               + '-{}{}.'.format(day, hour)
                               + '.'.join(file_name.split('.')[1:])
                               )
            else:
                remote_path = remote_path_tmp
            blob = _bucket.blob(remote_path)
            blob.upload_from_filename(local_file)

    _copy_local_directory_to_gcs(local_path, bucket, gcs_path, _n_to_remove=len(local_path) + 1)


def read_sync_file(table_path, system, table_name, settings_dict):
    try:
        with open(os.path.join(table_path, 'sync.json')) as fin:
            sync = json.load(fin)
            logging.info("Sync file found, with content:\n{}".format(sync))
    except FileNotFoundError:
        sync = {
            C.SYNC: {
                C.SYSTEM: system,
                C.TABLE_NAME: table_name,
                C.REF_COLUMN: settings_dict[C.CONNECTION][system][C.TABLE_SETTINGS][table_name][C.REF_COLUMN],
                C.REF_LAST_VALUE: settings_dict[C.CONNECTION][system][C.TABLE_SETTINGS][table_name][C.REF_FIRST_VALUE]
            }
        }
        logging.info("Sync file NOT found, initializing with content:\n{}".format(sync))
    return sync


def write_sync_file(new_sync, table_path):
    with open(os.path.join(table_path, 'sync.json'), 'w') as fout:
        json.dump(new_sync, fout)
        logging.info("New Sync file written, with content:\n{}".format(new_sync))
