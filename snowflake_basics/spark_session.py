def init_spark_session():
    global spark
    spark = None


def set_spark_session(spark_session):
    global spark
    spark = spark_session


def init_dbutils():
    global dbutils
    dbutils = None


def set_dbutils_session(dbutils_session):
    global dbutils
    dbutils = dbutils_session
