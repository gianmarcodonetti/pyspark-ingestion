from py4j.protocol import Py4JJavaError
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

format_to_pkg = {
    'avro': "com.databricks.spark.avro"
}


def get_spark_session(driver='/Users/gianmarco.donetti/Downloads/sqljdbc_6.0/enu/jre8/sqljdbc42.jar',
                      executor_memory='8g',
                      driver_memory='8g', master='local[*]', app_name='spark-ingestion',
                      shuffle_partitions='40'):
    conf = (SparkConf()
            .set('spark.executor.memory', executor_memory)
            .set('spark.driver.memory', driver_memory)
            .set('spark.driver.extraClassPath', driver)
            .set('spark.executor.extraClassPath', driver)
            .set('spark.sql.shuffle.partitions', shuffle_partitions)
            .setMaster(master)
            .setAppName(app_name)
            )

    sc = SparkContext(conf=conf)

    spark = SparkSession(sc)
    return spark


def get_spark_oracle_session(oracle_driver='D:/profiles/s005028/Downloads/ojdbc6.jar', executor_memory='6g',
                             driver_memory='6g', master='local[*]', app_name='spark-oracle-ingestion',
                             shuffle_partitions='40'):
    conf = (SparkConf()
            .set('spark.executor.memory', executor_memory)
            .set('spark.driver.memory', driver_memory)
            .set('spark.driver.extraClassPath', oracle_driver)
            .set('spark.executor.extraClassPath', oracle_driver)
            .set('spark.sql.shuffle.partitions', shuffle_partitions)
            .set('hive.metastore.client.factory.class',
                 'com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory')
            .setMaster(master)
            .setAppName(app_name)
            )

    sc = SparkContext(conf=conf)

    spark = SparkSession(sc)
    return spark


def write_partitioned(df_to_write, path_to_write, partition_columns=None, mode='errorifexists', compression='gzip',
                      file_format='parquet', **kwargs):
    # Partition data and prepare for writing
    if partition_columns:
        df_writer = (df_to_write
                     .repartition(*partition_columns)
                     .write
                     .partitionBy(partition_columns)
                     )
    else:
        df_writer = df_to_write.write
    # Write data accordingly to the requested file format
    if file_format == 'csv':
        kwargs['header'] = 'true'
        kwargs['quote'] = ''
    try:
        getattr(df_writer, file_format)(path_to_write, mode=mode, compression=compression, **kwargs)
    except AttributeError:
        (df_writer
         .format(format_to_pkg[file_format])
         .mode(mode)
         .option('compression', compression)
         .options(**kwargs)
         .save(path_to_write)
         )


def get_jdbc_thin_url(user, pw, host, port, service):
    jdbc_url = "jdbc:oracle:thin:{user}/{pw}@//{host}:{port}/{service}".format(
        user=user, pw=pw, host=host, port=port, service=service
    )
    return jdbc_url


def get_jdbc_url(config_dict, dialect='sqlserver'):
    """Retrieve the jdbc url connection string, given a DataBase dialect.

    Args:
        config_dict (dict): Dictionary with configuration details: first key is the dialect, then 'server', 'port',
            'database', 'user', 'pw'
        dialect (str): The SQL dialect of the database, like 'sqlserver'

    Returns:
        str: a string for the jdbc connection to the database.
    """
    jdbc_url = "jdbc:{dialect}://{server}:{jdbcPort};database={database};user={user};password={pw}".format(
        dialect=dialect,
        server=config_dict[dialect]['server'],
        jdbcPort=config_dict[dialect]['port'],
        database=config_dict[dialect]['database'],
        user=config_dict[dialect]['user'],
        pw=config_dict[dialect]['pw']
    )
    return jdbc_url


def get_jdbc_sqlserver_url(user, pw, server, port, database, dialect='sqlserver'):
    jdbc_url = "jdbc:{dialect}://{server}:{jdbcPort};database={database};user={user};password={pw}".format(
        dialect=dialect,
        server=server,
        jdbcPort=port,
        database=database,
        user=user,
        pw=pw
    )
    return jdbc_url


def read_dataframe_jdbc(spark, table_name, jdbc_url, driver="oracle.jdbc.driver.OracleDriver", fetchsize="100000"):
    try:
        jdbc_df = (spark
                   .read
                   .format("jdbc")
                   .option("url", jdbc_url)
                   .option("dbtable", table_name)
                   .option("driver", driver)
                   .option("fetchsize", fetchsize)
                   .load()
                   # .filter(F.col(date_column).isNotNull())
                   )
        return jdbc_df
    except Py4JJavaError as e:
        raise RuntimeError("Unable to read table '{}'...\nSee the stack trace:\n{}".format(table_name, e))
