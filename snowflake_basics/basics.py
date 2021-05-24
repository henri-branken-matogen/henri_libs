SF_URL = 'https://os27104.eu-west-1.snowflakecomputing.com/'


def read_snowflake(user, password, query, spark_sess, database="DEV", schema="MATOGEN", warehouse="MATOGEN_WH"):
    options = {
        'sfUrl': SF_URL,
        'sfUser': user,
        'sfPassword': password,
        'sfDatabase': database,
        'sfSchema': schema,
        'sfWarehouse': warehouse,
    }
    sdf = spark_sess\
        .read\
        .format('snowflake')\
        .options(**options)\
        .option('query', query)\
        .load()
    return sdf


def write_snowflake(user, password, sdf, tablename,
                    database="DEV", schema="MATOGEN", warehouse="MATOGEN_WH", mode="overwrite"):
    options = {
        'sfUrl': 'https://os27104.eu-west-1.snowflakecomputing.com/',
        'sfUser': user,
        'sfPassword': password,
        'sfDatabase': database,
        'sfSchema': schema,
        'sfWarehouse': warehouse,
    }
    sdf\
        .write\
        .format("snowflake")\
        .options(**options)\
        .option('dbtable', tablename)\
        .mode(mode)\
        .save()
