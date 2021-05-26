'''Functions for reactivation roll rate analysis.'''

import pandas as pd
import pyspark.sql.functions as f
from pyspark.sql.window import Window
import cvm_activity.spark_session as ss


def expand_subscriber_dates_activity(df, min_date, id_column='SERVICE_NUMBER'):
    max_date = df.groupby().agg(f.max('CALL_BEGIN_DATE')).collect()[0]['max(CALL_BEGIN_DATE)']
    pd_dates = pd.DataFrame(pd.date_range(min_date, max_date), columns=['DATE'])
    dates = ss.spark.createDataFrame(pd_dates).withColumn('DATE', f.col('DATE').cast('date'))

    subscribers = df.select(id_column).distinct()

    subscriber_activity = (
        df
        .withColumnRenamed('CALL_BEGIN_DATE', 'DATE')
        .select(
            id_column,
            'DATE',
            'ACTIVE_DATA',
            'ACTIVE_VOICE',
            'ACTIVE_SMS',
            'ACTIVE_ANY'
        )
    )

    df = subscribers.crossJoin(dates)
    df = df.join(subscriber_activity, on=[id_column, 'DATE'], how='left')
    df = df.fillna(0)
    return df


def add_cvm_dormancy(df, id_column='SERVICE_NUMBER'):
    w1 = Window.partitionBy(id_column).orderBy('DATE')
    w2 = Window.partitionBy(id_column, 'GROUP').orderBy('DATE')

    w3 = (Window
          .partitionBy(id_column)
          .orderBy('DATE')
          .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
          )

    w4 = (Window
          .partitionBy(id_column)
          .orderBy('DATE')
          )

    for activity_level in ['_DATA', '_VOICE', '_SMS', '_ANY']:
        active_col = 'ACTIVE%s' % activity_level
        dormant_col = 'DORMANCY_PERIOD%s' % activity_level
        still_dormant_col = 'STILL_DORMANT%s' % activity_level
        df = (
            df
            .withColumn(
                'DORMANCY_START',
                f.when(
                    (f.col(active_col) == 0)
                    & (f.lag(active_col, 1, default=1).over(w1) == 1),
                    1
                )
                .otherwise(0)
            )
            .withColumn('GROUP', f.sum('DORMANCY_START').over(w1))
            .withColumn('PERIOD', f.row_number().over(w2))
            .withColumn(
                dormant_col,
                f.when(f.col(active_col) == 0, f.col('PERIOD'))
                .otherwise(0)
            )
            .drop('GROUP', 'PERIOD', 'DORMANCY_START')
        )

        df = (
            df
            .withColumn(
                'MAX_DORMANCY',
                f.col(dormant_col) + f.count('DATE').over(w3) - f.row_number().over(w4)
            )
            .withColumn(
                still_dormant_col,
                f.when(
                    (f.col(active_col) == 0)
                    & (f.col('MAX_DORMANCY') == f.last(dormant_col).over(w3)),
                    1
                )
                .otherwise(0))
            .drop('MAX_DORMANCY')
        )

    return df


def expand_with_dormancy(df, min_date, id_column='SERVICE_NUMBER'):
    df = expand_subscriber_dates_activity(df, min_date, id_column=id_column)
    df = add_cvm_dormancy(df, id_column=id_column)
    return df
