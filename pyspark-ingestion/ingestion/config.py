from datetime import datetime

from pyspark.sql import functions as F
from pyspark.sql import types as T

from loipy import constants as C
from loipy.spark import get_jdbc_thin_url, get_jdbc_sqlserver_url

DATETIME_FROM = datetime(1980, 1, 1)
DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"


def preparation_sap_df(df, table_settings, sync):
    ref_column = sync[C.REF_COLUMN]
    ref_last_value = datetime.strptime(sync[C.REF_LAST_VALUE], DATETIME_FORMAT)

    year_getter_sap = lambda x: int(x[:4])
    month_getter_sap = lambda x: int(x[4:6])
    day_getter_sap = lambda x: int(x[6:])
    hour_getter_sap = lambda x: int(x[:2])
    minute_getter_sap = lambda x: int(x[2:4])
    second_getter_sap = lambda x: int(x[4:])

    custom_date = lambda dat, tim: datetime(year_getter_sap(dat),
                                            month_getter_sap(dat),
                                            day_getter_sap(dat),
                                            hour_getter_sap(tim),
                                            minute_getter_sap(tim),
                                            second_getter_sap(tim))

    custom_date_udf = F.udf(custom_date, T.TimestampType())

    elab_df = (df
               .withColumn(ref_column, custom_date_udf(F.col(table_settings[C.DATE_COLUMN]),
                                                       F.col(table_settings[C.TIME_COLUMN])))
               .where(F.col(ref_column) > ref_last_value)
               .withColumn('YEAR', F.udf(lambda x: x.year, T.StringType())(F.col(ref_column)))
               .withColumn('MONTH', F.udf(lambda x: x.month, T.StringType())(F.col(ref_column)))
               # .drop(ref_column)
               )

    return elab_df, ['YEAR', 'MONTH']


def preparation_lims_df(df, table_settings, sync):
    date_column = table_settings[C.DATE_COLUMN]
    ref_column = sync[C.REF_COLUMN]
    ref_last_value = datetime.strptime(sync[C.REF_LAST_VALUE], DATETIME_FORMAT)

    elab_df = (df
               .where(F.col(ref_column) > ref_last_value)
               .withColumn('YEAR', F.udf(lambda x: x.year, T.StringType())(F.col(date_column)))
               .withColumn('MONTH', F.udf(lambda x: x.month, T.StringType())(F.col(date_column)))
               )
    return elab_df, ['YEAR', 'MONTH']


def preparation_c1_df(df, table_settings, sync):
    ref_column = sync[C.REF_COLUMN]
    ref_last_value = datetime.strptime(sync[C.REF_LAST_VALUE], DATETIME_FORMAT)

    if C.COLUMNS_TO_IMPORT in table_settings:
        df_sel = df.select(table_settings[C.COLUMNS_TO_IMPORT])
    else:
        df_sel = df

    elab_df = (df_sel
               .where(F.col(ref_column) > ref_last_value)
               .withColumn('YEAR', F.udf(lambda x: x.year, T.StringType())(F.col(ref_column)))
               .withColumn('WEEK', F.udf(lambda x: x.isocalendar()[1], T.StringType())(F.col(ref_column)))
               )
    return elab_df, ['YEAR', 'WEEK']


config_dict = {
    "dbms-to-url-function": {
        "oracle": get_jdbc_thin_url,
        "sqlserver": get_jdbc_sqlserver_url
    },
    "system-to-prep-function": {
        "sap-pru": preparation_sap_df,
        "lims": preparation_lims_df,
        "c1": preparation_c1_df
    }
}
