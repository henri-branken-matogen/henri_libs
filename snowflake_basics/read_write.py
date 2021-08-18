import snowflake_basics.spark_session as ss
import snowflake_basics.dbutils_session as dbs
import os


USER_KEY = "my_name"
PASSWORD_KEY = "my_pass"
SF_URL_KEY = "SF_URL_KEY"


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
                    database="DEV", schema="MATOGEN", warehouse="MATOGEN_WH",
                    mode="overwrite"):
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
        .mode(mode)\
        .save()


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

    # Find the ".csv.gz" file of interest
    entity = [x.path for x in dbs.fs.ls(fp_absolute) if x.path.endswith(".csv.gz")][0]

    # Isolate the .csv.gz entity that we are interested in, and give it a readable name.
    dbs.fs.cp(entity, fp_gz)

    # Do a cleanup of the redundant folder:
    dbs.fs.rm(fp_absolute, recurse=True)

    # Print the `fp_gz` to copy into shell for download.
    print(fp_gz)
    return None
