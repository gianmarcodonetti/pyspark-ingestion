import boto3
import pandas as pd
import pyathena

# TODO make a temporary saving of the dropped table in case CTAS fails
TMP_DB = 'tmp'


class AthenaContext:

    def __init__(self, aki, sak, s3_staging_dir='s3://data-architect-athena-staging-area/stage/',
                 region_name='eu-west-1'):
        self.aki = aki
        self.sak = sak
        self.s3_staging_dir = s3_staging_dir
        self.region_name = region_name
        self.conn = pyathena.connect(aws_access_key_id=self.aki,
                                     aws_secret_access_key=self.sak,
                                     s3_staging_dir=self.s3_staging_dir,
                                     region_name=self.region_name)

    def execute(self, sql, debug=False):
        cursor = self.conn.cursor()
        cursor.execute(sql.rstrip(';') + ';')
        if debug:
            print("Description:\n", cursor.description)
            fetched = cursor.fetchall()
            df = pd.DataFrame(fetched, columns=[tup[0] for tup in cursor.description])
            print("\nData:\n", df)

    def repair_table(self, database, table, debug=False):
        msck = "MSCK REPAIR TABLE {}.{}".format(database, table)
        print(msck)
        self.execute(msck, debug=debug)

    def drop(self, database, table, debug=False):
        drop = "DROP TABLE IF EXISTS {}.{} PURGE".format(database, table)
        print(drop)
        self.execute(drop, debug=debug)

    def ctas(self, target_database, target_table, select_statement, target_location, partition_columns=None,
             file_format='PARQUET', compression='GZIP', debug=False):
        ctas = athena_ctas_statement(target_database, target_table, select_statement, target_location,
                                     partition_columns, file_format, compression)
        print(ctas)
        self.execute(ctas, debug=debug)

    def cvas_partition_date(self, database, table, view_name, debug=False):
        cvas = athena_cvas_partition_date_statement(database, table, view_name)
        print(cvas)
        self.execute(cvas, debug=debug)

    def s3_delete_objects(self, bucket, prefix):
        s3 = boto3.resource('s3', aws_access_key_id=self.aki, aws_secret_access_key=self.sak)
        objects_to_delete = s3.meta.client.list_objects(Bucket=bucket, Prefix=prefix)
        delete_keys = {'Objects': [{'Key': k} for k in [obj['Key'] for obj in objects_to_delete.get('Contents', [])]]}
        if delete_keys['Objects']:
            s3.meta.client.delete_objects(Bucket=bucket, Delete=delete_keys)


def athena_ctas_statement(target_database, target_table, select_statement, target_location, partition_columns=None,
                          file_format='PARQUET', compression='GZIP'):
    partition_string = " partitioned_by = ARRAY{},".format(partition_columns) if partition_columns else ""
    sql = ("" +
           "CREATE TABLE {db}.{table}".format(db=target_database, table=target_table) +
           " WITH (" +
           " {}".format(partition_string) +
           " external_location = '{}',".format(target_location) +
           " format = '{}',".format(file_format) +
           " {}_compression = '{}')".format(file_format.lower(), compression) +
           " AS {select}".format(select=select_statement) +
           ";"
           )
    return sql


def athena_cvas_partition_date_statement(database, table, view_name):
    cvas = """
        CREATE OR REPLACE VIEW {}.{} AS 
        SELECT *, CAST("date_parse"("concat"("year", "month", "day"), '%Y%m%d') AS date) "partition_date"
        FROM {}.{}
    """.format(database, view_name, database, table)
    return cvas
