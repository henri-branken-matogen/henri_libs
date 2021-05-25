USER_KEY = "my_name"
PASSWORD_KEY = "my_pass"
SF_URL_KEY = "SF_URL_KEY"


def read_snowflake(user, password, query, spark_sess, sf_url, database="DEV",
                   schema="MATOGEN", warehouse="MATOGEN_WH"):
    options = {
        'sfUrl': sf_url,
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


def write_snowflake(user, password, sdf, tablename, sf_url,
                    database="DEV", schema="MATOGEN", warehouse="MATOGEN_WH",
                    mode="overwrite"):
    options = {
        'sfUrl': sf_url,
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
