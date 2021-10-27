import snowflake_basics.spark_session as ss
import os


USER_KEY = "my_name"
PASSWORD_KEY = "my_pass"
SF_URL_KEY = "SF_URL_KEY"


def append_snowflake(user, password, sdf, tablename, sf_url,
                     database="DEV", schema="MATOGEN", warehouse="MATOGEN_WH"):
    """
    The `mode` option has been specifically set to "append".
    """
    options = {
        'sfUrl': sf_url,
        'sfUser': user,
        'sfPassword': password,
        'sfDatabase': database,
        'sfSchema': schema,
        'sfWarehouse': warehouse
    }
    sdf\
        .write\
        .format("snowflake")\
        .options(**options)\
        .option('dbtable', tablename)\
        .mode("append")\
        .save()
    return None


def read_snowflake(user, password, query, sf_url, database="DEV",
                   schema="MATOGEN", warehouse="MATOGEN_WH"):
    options = {
        'sfUrl': sf_url,
        'sfUser': user,
        'sfPassword': password,
        'sfDatabase': database,
        'sfSchema': schema,
        'sfWarehouse': warehouse
    }
    sdf = ss\
        .spark\
        .read\
        .format('snowflake')\
        .options(**options)\
        .option('query', query)\
        .load()
    return sdf


def write_snowflake(user, password, sdf, tablename, sf_url,
                    database="DEV", schema="MATOGEN", warehouse="MATOGEN_WH"):
    """
    The `mode` option has been specifically set to "overwrite".
    """
    options = {
        'sfUrl': sf_url,
        'sfUser': user,
        'sfPassword': password,
        'sfDatabase': database,
        'sfSchema': schema,
        'sfWarehouse': warehouse
    }
    sdf\
        .write\
        .format("snowflake")\
        .options(**options)\
        .option('dbtable', tablename)\
        .mode("overwrite")\
        .save()
    return None


def write_out_csvgz(sdf, fp_base, fn):
    """
    Write a PySpark Dataset out to .csv.gz format.
    :param sdf:  The PySpark dataset we wish to fetch from the s3 bucket via awscli tools.
    :param fp_base:  The parent directory of the dataset.  Type String.
    :param fn:  The filename of the dataset.  Type String.
    :return:  Nothing is returned.  The .csv.gz is stored in s3 bucket from which we need to fetch it.
    """
    fp_absolute = os.path.join(fp_base, fn)
    fp_gz = os.path.join(fp_base, fn + ".csv.gz")
    sdf\
        .repartition(1)\
        .write\
        .mode("overwrite")\
        .option("header", True)\
        .option("delimiter", "|")\
        .option("compression", "gzip")\
        .csv(fp_absolute)

    print(fp_absolute, fp_gz)
