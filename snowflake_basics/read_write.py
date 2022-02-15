import snowflake_basics.spark_session as ss
from py4j.protocol import Py4JJavaError
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


def append_safely(partition_var_name, partition_var_val, sdf, database, schema, tablename, user, password, sf_url,
                  warehouse="MATOGEN_WH"):

    # Determine whether the table exists in the first place.  This is then stored inside `tb_exists`.
    try:
        query_1 = f"""SELECT * FROM {database}.{schema}.{tablename}"""
        read_snowflake(user=user,
                       password=password,
                       query=query_1,
                       sf_url=sf_url,
                       database=database,
                       schema=schema,
                       warehouse=warehouse)
        tb_exists = 1
    except Py4JJavaError:
        # The table could not be found in Snowflake
        tb_exists = 0

    if tb_exists == 1:
        query_2 = f"""SELECT * FROM {database}.{schema}.{tablename}
                      WHERE CAST({partition_var_name} AS VARCHAR) = '{partition_var_val}'"""
        sdf_check_part = read_snowflake(user=user,
                                        password=password,
                                        query=query_2,
                                        sf_url=sf_url,
                                        database=database,
                                        schema=schema,
                                        warehouse=warehouse)
        if sdf_check_part.count() == 0:  # The partition does not exist, therefore append.
            # EXECUTE
            print("tb=1, pt=0, append_snowflake...")
            append_snowflake(user=user,
                             password=password,
                             sdf=sdf,
                             tablename=tablename,
                             sf_url=sf_url,
                             database=database,
                             schema=schema,
                             warehouse=warehouse)
        else:
            # The partition already exists.
            # [1] Strip out the pre-existing partition.
            # [2] Overwrite the remainder to the existing Snowflake table with the write function.
            # [3] Append the new sdf of interest to the existing Snowflake table.

            # [1]
            query_3 = f"""SELECT * FROM {database}.{schema}.{tablename}
                          WHERE CAST({partition_var_name} AS VARCHAR) <> '{partition_var_val}'"""
            sdf_rem = read_snowflake(user=user,
                                     password=password,
                                     query=query_3,
                                     sf_url=sf_url,
                                     database=database,
                                     schema=schema,
                                     warehouse=warehouse)
            # [2]
            if sdf_rem.count() != 0:
                write_snowflake(user=user,
                                password=password,
                                sdf=sdf_rem,
                                tablename=tablename,
                                sf_url=sf_url,
                                database=database,
                                schema=schema,
                                warehouse=warehouse)
            # [3]
            # EXECUTE
            print("tb=1, pt=1, append_snowflake...")
            append_snowflake(user=user,
                             password=password,
                             sdf=sdf,
                             tablename=tablename,
                             sf_url=sf_url,
                             database=database,
                             schema=schema,
                             warehouse=warehouse)
    else:  # The table does not exist in the first place.
        # EXECUTE
        print("tb=0, pt=0, append_snowflake...")
        append_snowflake(user=user,
                         password=password,
                         sdf=sdf,
                         tablename=tablename,
                         sf_url=sf_url,
                         database=database,
                         schema=schema,
                         warehouse=warehouse)
    return tb_exists


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
