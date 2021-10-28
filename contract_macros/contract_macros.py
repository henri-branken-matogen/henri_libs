import pyspark.sql.functions as f
import pyspark.sql.types as t


def Contract_Treatment(ACT_MOB, CON_Renewal, CON_Started_0, CON_Started_1, CON_Started_2, CON_Status_Bar_Outgoing,
                       CON_Months_00, CON_Months_01, CON_Ended_2, CON_Ended_1, CON_Ended_0, CON_Ending_1, CON_Ending_2,
                       CON_Months_24, CON_Months_12, CON_Months_06):
    CON_Treatment = None      # 0
    CON_Treat_No_0 = None     # 1
    CON_Treat_No_1 = None     # 2
    CON_Treat_No_2 = None     # 3
    CON_Treat_No_3 = None     # 4
    CON_Treat_No_4 = None     # 5
    CON_Treat_No_5 = None     # 6
    CON_Treat_Yes_0 = None    # 7
    CON_Treat_Yes_1 = None    # 8
    CON_Treat_Yes_2 = None    # 9
    CON_Treat_Yes_3 = None    # 10
    CON_Treat_Yes_4 = None    # 11
    CON_Treat_Yes_5 = None    # 12
    CON_Treat_Yes_6 = None    # 13
    CON_Treat_Yes_7 = None    # 14
    CON_Treat_Yes_8 = None    # 15
    CON_Treat_Yes_9 = None    # 16
    ls_return = [CON_Treatment, CON_Treat_No_0, CON_Treat_No_1, CON_Treat_No_2, CON_Treat_No_3, CON_Treat_No_4,
                 CON_Treat_No_5, CON_Treat_Yes_0, CON_Treat_Yes_1, CON_Treat_Yes_2, CON_Treat_Yes_3, CON_Treat_Yes_4,
                 CON_Treat_Yes_5, CON_Treat_Yes_6, CON_Treat_Yes_7, CON_Treat_Yes_8, CON_Treat_Yes_9]
    if ACT_MOB == 0:
        ls_return[0] = "1 New account (activation = contract start)"
        ls_return[1] = 1
        return ls_return
    elif CON_Renewal == 1:
        ls_return[0] = "2 Contract renewed this period"
        ls_return[2] = 1
        return ls_return
    elif CON_Started_0 == 1:
        ls_return[0] = "3 Contract commenced this period"
        ls_return[3] = 1
        return ls_return
    elif CON_Started_1 == 1:
        ls_return[0] = "4 Contract commenced last period"
        ls_return[4] = 1
        return ls_return
    elif CON_Started_2 == 1:
        ls_return[0] = "5 Contract commenced prior period"
        ls_return[5] = 1
        return ls_return
    elif CON_Status_Bar_Outgoing == 1:
        ls_return[0] = "6 Contract service barred"
        ls_return[6] = 1
        return ls_return
    elif CON_Months_00 == 1:
        ls_return[0] = "A Active on a 0 month contract"
        ls_return[7] = 1
        return ls_return
    elif CON_Months_01 == 1:
        ls_return[0] = "B Active on a 1 month contract"
        ls_return[8] = 1
        return ls_return
    elif CON_Ended_2 == 1:
        ls_return[0] = "C Contract active and ended prior period"
        ls_return[9] = 1
        return ls_return
    elif CON_Ended_1 == 1:
        ls_return[0] = "D Contract active and ended last period"
        ls_return[10] = 1
        return ls_return
    elif CON_Ended_0 == 1:
        ls_return[0] = "E Contract active and ended this period"
        ls_return[11] = 1
        return ls_return
    elif CON_Ending_1 == 1:
        ls_return[0] = "F Contract active and ending next period"
        ls_return[12] = 1
        return ls_return
    elif CON_Ending_2 == 1:
        ls_return[0] = "G Contract active and ending following period"
        ls_return[13] = 1
        return ls_return
    elif CON_Months_24 == 1:
        ls_return[0] = "H 24 month contract active and out of period"
        ls_return[14] = 1
        return ls_return
    elif CON_Months_12 == 1:
        ls_return[0] = "I 12 month contract active and out of period"
        ls_return[15] = 1
        return ls_return
    elif CON_Months_06 == 1:
        ls_return[0] = "J 6 month contract active and out of period"
        ls_return[16] = 1
        return ls_return
    else:
        ls_return[0] = "X Contract active and out of period"
        return ls_return


schema_CT = t.StructType([
    t.StructField("CON_Treatment", t.StringType(), True),
    t.StructField("CON_Treat_No_0", t.IntegerType(), True),
    t.StructField("CON_Treat_No_1", t.IntegerType(), True),
    t.StructField("CON_Treat_No_2", t.IntegerType(), True),
    t.StructField("CON_Treat_No_3", t.IntegerType(), True),
    t.StructField("CON_Treat_No_4", t.IntegerType(), True),
    t.StructField("CON_Treat_No_5", t.IntegerType(), True),
    t.StructField("CON_Treat_Yes_0", t.IntegerType(), True),
    t.StructField("CON_Treat_Yes_1", t.IntegerType(), True),
    t.StructField("CON_Treat_Yes_2", t.IntegerType(), True),
    t.StructField("CON_Treat_Yes_3", t.IntegerType(), True),
    t.StructField("CON_Treat_Yes_4", t.IntegerType(), True),
    t.StructField("CON_Treat_Yes_5", t.IntegerType(), True),
    t.StructField("CON_Treat_Yes_6", t.IntegerType(), True),
    t.StructField("CON_Treat_Yes_7", t.IntegerType(), True),
    t.StructField("CON_Treat_Yes_8", t.IntegerType(), True),
    t.StructField("CON_Treat_Yes_9", t.IntegerType(), True),
])
udf_Contract_Treatment = f.udf(Contract_Treatment, returnType=schema_CT)
