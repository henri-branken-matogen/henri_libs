import pyspark.sql.functions as f
import pyspark.sql.types as t
from operator import add
from functools import reduce
from pyspark.sql import Window
from stallion.py_filter_fxs import *


def Applications_Contracts_Update(NBR, Account, APP_Account_Number1, APP_Account_Number2, APP_Account_Number3,
                                  Subscriber_Number, CON_Start_Date, Matched_Distance, CON_PERIOD):
    APP_Subscriptions = NBR

    APP_Subscriber_Number1 = APP_Subscriber_Number2 = APP_Subscriber_Number3 = APP_Subscriber_Number4 = APP_Subscriber_Number5 = None
    APP_Activation_Date1 = APP_Activation_Date2 = APP_Activation_Date3 = APP_Activation_Date4 = APP_Activation_Date5 = None
    APP_Activation_Days1 = APP_Activation_Days2 = APP_Activation_Days3 = APP_Activation_Days4 = APP_Activation_Days5 = None
    APP_Activation_Weeks1 = APP_Activation_Weeks2 = APP_Activation_Weeks3 = APP_Activation_Weeks4 = APP_Activation_Weeks5 = None
    APP_Activation_Month1 = APP_Activation_Month2 = APP_Activation_Month3 = APP_Activation_Month4 = APP_Activation_Month5 = None

    if NBR == 1:
        if APP_Account_Number1 == Account:
            APP_Account1 = "1"
        elif APP_Account_Number2 == Account:
            APP_Account1 = "2"
        elif APP_Account_Number3 == Account:
            APP_Account1 = "3"
        elif APP_Account_Number1 is None:
            APP_Account_Number1 = Account
            APP_Account1 = "1"
        elif APP_Account_Number2 is None:
            APP_Account_Number2 = Account
            APP_Account1 = "2"
        elif APP_Account_Number3 is None:
            APP_Account_Number3 = Account
            APP_Account1 = "3"
        else:
            APP_Account1 = None
        APP_Accounts = APP_Account1
        APP_Subscriber_Number1 = Subscriber_Number
        APP_Activation_Date1 = CON_Start_Date
        APP_Activation_Days1 = Matched_Distance
        if APP_Activation_Days1 is None:
            APP_Activation_Weeks1 = None
        elif APP_Activation_Days1 == 0:
            APP_Activation_Weeks1 = 0
        elif APP_Activation_Days1 <= -1:
            APP_Activation_Weeks1 = int((APP_Activation_Days1 + 1) / 7) - 1
        elif APP_Activation_Days1 >= 1:
            APP_Activation_Weeks1 = int((APP_Activation_Days1 - 1) / 7) + 1
        else:
            pass
        APP_Activation_Month1 = CON_PERIOD
    elif NBR == 2:
        if APP_Account_Number1 == Account:
            APP_Account2 = "1"
        elif APP_Account_Number2 == Account:
            APP_Account2 = "2"
        elif APP_Account_Number3 == Account:
            APP_Account2 = "3"
        elif APP_Account_Number1 is None:
            APP_Account_Number1 = Account
            APP_Account2 = "1"
        elif APP_Account_Number2 is None:
            APP_Account_Number2 = Account
            APP_Account2 = "2"
        elif APP_Account_Number3 is None:
            APP_Account_Number3 = Account
            APP_Account2 = "3"
        else:
            APP_Account2 = None
        APP_Accounts = APP_Account2
        APP_Subscriber_Number2 = Subscriber_Number
        APP_Activation_Date2 = CON_Start_Date
        APP_Activation_Days2 = Matched_Distance
        if APP_Activation_Days2 is None:
            APP_Activation_Weeks2 = None
        elif APP_Activation_Days2 == 0:
            APP_Activation_Weeks2 = 0
        elif APP_Activation_Days2 <= -1:
            APP_Activation_Weeks2 = int((APP_Activation_Days2 + 1) / 7) - 1
        elif APP_Activation_Days2 >= 1:
            APP_Activation_Weeks2 = int((APP_Activation_Days2 - 1) / 7) + 1
        else:
            pass
        APP_Activation_Month2 = CON_PERIOD
    elif NBR == 3:
        if APP_Account_Number1 == Account:
            APP_Account3 = "1"
        elif APP_Account_Number2 == Account:
            APP_Account3 = "2"
        elif APP_Account_Number3 == Account:
            APP_Account3 = "3"
        elif APP_Account_Number1 is None:
            APP_Account_Number1 = Account
            APP_Account3 = "1"
        elif APP_Account_Number2 is None:
            APP_Account_Number2 = Account
            APP_Account3 = "2"
        elif APP_Account_Number3 is None:
            APP_Account_Number3 = Account
            APP_Account3 = "3"
        else:
            APP_Account3 = None
        APP_Accounts = APP_Account3
        APP_Subscriber_Number3 = Subscriber_Number
        APP_Activation_Date3 = CON_Start_Date
        APP_Activation_Days3 = Matched_Distance
        if APP_Activation_Days3 is None:
            APP_Activation_Weeks3 = None
        elif APP_Activation_Days3 == 0:
            APP_Activation_Weeks3 = 0
        elif APP_Activation_Days3 <= -1:
            APP_Activation_Weeks3 = int((APP_Activation_Days3 + 1) / 7) - 1
        elif APP_Activation_Days3 >= 1:
            APP_Activation_Weeks3 = int((APP_Activation_Days3 - 1) / 7) + 1
        else:
            pass
        APP_Activation_Month3 = CON_PERIOD
    elif NBR == 4:
        if APP_Account_Number1 == Account:
            APP_Account4 = "1"
        elif APP_Account_Number2 == Account:
            APP_Account4 = "2"
        elif APP_Account_Number3 == Account:
            APP_Account4 = "3"
        elif APP_Account_Number1 is None:
            APP_Account_Number1 = Account
            APP_Account4 = "1"
        elif APP_Account_Number2 is None:
            APP_Account_Number2 = Account
            APP_Account4 = "2"
        elif APP_Account_Number3 is None:
            APP_Account_Number3 = Account
            APP_Account4 = "3"
        else:
            APP_Account4 = None
        APP_Accounts = APP_Account4
        APP_Subscriber_Number4 = Subscriber_Number
        APP_Activation_Date4 = CON_Start_Date
        APP_Activation_Days4 = Matched_Distance
        if APP_Activation_Days4 is None:
            APP_Activation_Weeks4 = None
        elif APP_Activation_Days4 == 0:
            APP_Activation_Weeks4 = 0
        elif APP_Activation_Days4 <= -1:
            APP_Activation_Weeks4 = int((APP_Activation_Days4 + 1) / 7) - 1
        elif APP_Activation_Days4 >= 1:
            APP_Activation_Weeks4 = int((APP_Activation_Days4 - 1) / 7) + 1
        else:
            pass
        APP_Activation_Month4 = CON_PERIOD
    elif NBR == 5:
        if APP_Account_Number1 == Account:
            APP_Account5 = "1"
        elif APP_Account_Number2 == Account:
            APP_Account5 = "2"
        elif APP_Account_Number3 == Account:
            APP_Account5 = "3"
        elif APP_Account_Number1 is None:
            APP_Account_Number1 = Account
            APP_Account5 = "1"
        elif APP_Account_Number2 is None:
            APP_Account_Number2 = Account
            APP_Account5 = "2"
        elif APP_Account_Number3 is None:
            APP_Account_Number3 = Account
            APP_Account5 = "3"
        else:
            APP_Account5 = None
        APP_Accounts = APP_Account5
        APP_Subscriber_Number5 = Subscriber_Number
        APP_Activation_Date5 = CON_Start_Date
        APP_Activation_Days5 = Matched_Distance
        if APP_Activation_Days5 is None:
            APP_Activation_Weeks5 = None
        elif APP_Activation_Days5 == 0:
            APP_Activation_Weeks5 = 0
        elif APP_Activation_Days5 <= -1:
            APP_Activation_Weeks5 = int((APP_Activation_Days5 + 1) / 7) - 1
        elif APP_Activation_Days5 >= 1:
            APP_Activation_Weeks5 = int((APP_Activation_Days5 - 1) / 7) + 1
        else:
            pass
        APP_Activation_Month5 = CON_PERIOD
    else:
        APP_Accounts = None

    return (APP_Subscriptions, APP_Account_Number1, APP_Account_Number2, APP_Account_Number3, APP_Accounts,
            APP_Subscriber_Number1, APP_Subscriber_Number2, APP_Subscriber_Number3, APP_Subscriber_Number4, APP_Subscriber_Number5,
            APP_Activation_Date1, APP_Activation_Date2, APP_Activation_Date3, APP_Activation_Date4, APP_Activation_Date5,
            APP_Activation_Days1, APP_Activation_Days2, APP_Activation_Days3, APP_Activation_Days4, APP_Activation_Days5,
            APP_Activation_Weeks1, APP_Activation_Weeks2, APP_Activation_Weeks3, APP_Activation_Weeks4, APP_Activation_Weeks5,
            APP_Activation_Month1, APP_Activation_Month2, APP_Activation_Month3, APP_Activation_Month4, APP_Activation_Month5)


schema_Applications_Contracts_Update = t.StructType([
    t.StructField("APP_Subscriptions", t.IntegerType(), True),
    t.StructField("APP_Account_Number1", t.LongType(), True),
    t.StructField("APP_Account_Number2", t.LongType(), True),
    t.StructField("APP_Account_Number3", t.LongType(), True),
    t.StructField("APP_Accounts", t.StringType(), True),
    t.StructField("APP_Subscriber_Number1", t.LongType(), True),
    t.StructField("APP_Subscriber_Number2", t.LongType(), True),
    t.StructField("APP_Subscriber_Number3", t.LongType(), True),
    t.StructField("APP_Subscriber_Number4", t.LongType(), True),
    t.StructField("APP_Subscriber_Number5", t.LongType(), True),
    t.StructField("APP_Activation_Date1", t.DateType(), True),
    t.StructField("APP_Activation_Date2", t.DateType(), True),
    t.StructField("APP_Activation_Date3", t.DateType(), True),
    t.StructField("APP_Activation_Date4", t.DateType(), True),
    t.StructField("APP_Activation_Date5", t.DateType(), True),
    t.StructField("APP_Activation_Days1", t.IntegerType(), True),
    t.StructField("APP_Activation_Days2", t.IntegerType(), True),
    t.StructField("APP_Activation_Days3", t.IntegerType(), True),
    t.StructField("APP_Activation_Days4", t.IntegerType(), True),
    t.StructField("APP_Activation_Days5", t.IntegerType(), True),
    t.StructField("APP_Activation_Weeks1", t.IntegerType(), True),
    t.StructField("APP_Activation_Weeks2", t.IntegerType(), True),
    t.StructField("APP_Activation_Weeks3", t.IntegerType(), True),
    t.StructField("APP_Activation_Weeks4", t.IntegerType(), True),
    t.StructField("APP_Activation_Weeks5", t.IntegerType(), True),
    t.StructField("APP_Activation_Month1", t.IntegerType(), True),
    t.StructField("APP_Activation_Month2", t.IntegerType(), True),
    t.StructField("APP_Activation_Month3", t.IntegerType(), True),
    t.StructField("APP_Activation_Month4", t.IntegerType(), True),
    t.StructField("APP_Activation_Month5", t.IntegerType(), True)
])

udf_Applications_Contracts_Update = f.udf(Applications_Contracts_Update,
                                          returnType=schema_Applications_Contracts_Update)


def Match_Applications_Contracts(sdf_0):
    windowspec = Window\
        .partitionBy()\
        .orderBy([f.col("APP_Record_Number").asc()])

    sdf_0a = sdf_0\
        .repartition(1)\
        .orderBy([f.col("APP_Record_Number").asc()])

    sdf_0b = sdf_0a\
        .repartition(1)\
        .withColumn("RET_Record_Number", f.lag(f.col("APP_Record_Number"), 1).over(windowspec))

    # Only keep the application record.
    sdf_1 = sdf_0b\
        .filter(f.col("APP_Date").isNotNull())

    # Calculated the distance between an application and a matched activation.
    sdf_2 = sdf_1\
        .withColumn("Matched_Distance", f.datediff(f.col("CON_Start_Date"), f.col("APP_Date")))

    # Based on the distance and application status sequence, matches and update the associated filters.
    sdf_3 = sdf_2\
        .withColumn("nest",
                    udf_Filter_Activation_Status(f.col("Matched_Distance"),
                                                 f.col("Filter_Decision_Outcome_Declined"),
                                                 f.col("Filter_Decision_Outcome_Arrears"),
                                                 f.col("Filter_Decision_Outcome_Referred"),
                                                 f.col("Filter_Decision_Outcome_Approved"),
                                                 f.col("Filter_Declined_No_Activations"),
                                                 f.col("Filter_Arrears_No_Activations"),
                                                 f.col("Filter_Referred_No_Activations"),
                                                 f.col("Filter_Approved_No_Activations"),
                                                 f.col("Filter_Declined_With_Activations"),
                                                 f.col("Filter_Arrears_With_Activations"),
                                                 f.col("Filter_Referred_With_Activations"),
                                                 f.col("Filter_Approved_With_Activations")))
    sdf_4 = sdf_3\
        .withColumn("FILTER_ACTIVATION_SEQ", f.col("nest.FILTER_ACTIVATION_SEQ"))\
        .withColumn("FILTER_DECLINED_NO_ACTIVATIONS", f.col("nest.FILTER_DECLINED_NO_ACTIVATIONS"))\
        .withColumn("FILTER_ARREARS_NO_ACTIVATIONS", f.col("nest.FILTER_ARREARS_NO_ACTIVATIONS"))\
        .withColumn("FILTER_REFERRED_NO_ACTIVATIONS", f.col("nest.FILTER_REFERRED_NO_ACTIVATIONS"))\
        .withColumn("FILTER_APPROVED_NO_ACTIVATIONS", f.col("nest.FILTER_APPROVED_NO_ACTIVATIONS"))\
        .withColumn("FILTER_DECLINED_WITH_ACTIVATIONS", f.col("nest.FILTER_DECLINED_WITH_ACTIVATIONS"))\
        .withColumn("FILTER_ARREARS_WITH_ACTIVATIONS", f.col("nest.FILTER_ARREARS_WITH_ACTIVATIONS"))\
        .withColumn("FILTER_REFERRED_WITH_ACTIVATIONS", f.col("nest.FILTER_REFERRED_WITH_ACTIVATIONS"))\
        .withColumn("FILTER_APPROVED_WITH_ACTIVATIONS", f.col("nest.FILTER_APPROVED_WITH_ACTIVATIONS"))\
        .drop(*["nest"])

    sdf_5 = sdf_4\
        .withColumn("nest",
                    f.when(f.col("Matched_Distance").isNotNull() &
                           (f.col("APP_Record_Number") != f.col("RET_Record_Number")),
                           udf_Applications_Contracts_Update(f.lit(1),
                                                             f.col("Account"),
                                                             f.col("APP_Account_Number1"),
                                                             f.col("APP_Account_Number2"),
                                                             f.col("APP_Account_Number3"),
                                                             f.col("Subscriber_Number"),
                                                             f.col("CON_Start_Date"),
                                                             f.col("Matched_Distance"),
                                                             f.col("CON_PERIOD")))
                     .when(f.col("Matched_Distance").isNotNull() &
                           (f.col("APP_Subscriptions") == 1),
                           udf_Applications_Contracts_Update(f.lit(2),
                                                             f.col("Account"),
                                                             f.col("APP_Account_Number1"),
                                                             f.col("APP_Account_Number2"),
                                                             f.col("APP_Account_Number3"),
                                                             f.col("Subscriber_Number"),
                                                             f.col("CON_Start_Date"),
                                                             f.col("Matched_Distance"),
                                                             f.col("CON_PERIOD")))
                    .when(f.col("Matched_Distance").isNotNull() &
                          (f.col("APP_Subscriptions") == 2),
                          udf_Applications_Contracts_Update(f.lit(3),
                                                            f.col("Account"),
                                                            f.col("APP_Account_Number1"),
                                                            f.col("APP_Account_Number2"),
                                                            f.col("APP_Account_Number3"),
                                                            f.col("Subscriber_Number"),
                                                            f.col("CON_Start_Date"),
                                                            f.col("Matched_Distance"),
                                                            f.col("CON_PERIOD")))
                    .when(f.col("Matched_Distance").isNotNull() &
                          (f.col("APP_Subscriptions") == 3),
                          udf_Applications_Contracts_Update(f.lit(4),
                                                            f.col("Account"),
                                                            f.col("APP_Account_Number1"),
                                                            f.col("APP_Account_Number2"),
                                                            f.col("APP_Account_Number3"),
                                                            f.col("Subscriber_Number"),
                                                            f.col("CON_Start_Date"),
                                                            f.col("Matched_Distance"),
                                                            f.col("CON_PERIOD")))
                    .when(f.col("Matched_Distance").isNotNull() &
                          (f.col("APP_Subscriptions") == 4),
                          udf_Applications_Contracts_Update(f.lit(5),
                                                            f.col("Account"),
                                                            f.col("APP_Account_Number1"),
                                                            f.col("APP_Account_Number2"),
                                                            f.col("APP_Account_Number3"),
                                                            f.col("Subscriber_Number"),
                                                            f.col("CON_Start_Date"),
                                                            f.col("Matched_Distance"),
                                                            f.col("CON_PERIOD"))))

    sdf_6 = sdf_5\
        .withColumn("APP_Subscriptions", f.col("nest.APP_Subscriptions"))\
        .withColumn("APP_Account_Number1", f.col("nest.APP_Account_Number1"))\
        .withColumn("APP_Account_Number2", f.col("nest.APP_Account_Number2"))\
        .withColumn("APP_Account_Number3", f.col("nest.APP_Account_Number3"))\
        .withColumn("APP_Accounts", f.col("nest.APP_Accounts"))\
        .withColumn("APP_Subscriber_Number1", f.col("nest.APP_Subscriber_Number1"))\
        .withColumn("APP_Subscriber_Number2", f.col("nest.APP_Subscriber_Number2"))\
        .withColumn("APP_Subscriber_Number3", f.col("nest.APP_Subscriber_Number3"))\
        .withColumn("APP_Subscriber_Number4", f.col("nest.APP_Subscriber_Number4"))\
        .withColumn("APP_Subscriber_Number5", f.col("nest.APP_Subscriber_Number5"))\
        .withColumn("APP_Activation_Date1", f.col("nest.APP_Activation_Date1"))\
        .withColumn("APP_Activation_Date2", f.col("nest.APP_Activation_Date2"))\
        .withColumn("APP_Activation_Date3", f.col("nest.APP_Activation_Date3"))\
        .withColumn("APP_Activation_Date4", f.col("nest.APP_Activation_Date4"))\
        .withColumn("APP_Activation_Date5", f.col("nest.APP_Activation_Date5"))\
        .withColumn("APP_Activation_Days1", f.col("nest.APP_Activation_Days1"))\
        .withColumn("APP_Activation_Days2", f.col("nest.APP_Activation_Days2"))\
        .withColumn("APP_Activation_Days3", f.col("nest.APP_Activation_Days3"))\
        .withColumn("APP_Activation_Days4", f.col("nest.APP_Activation_Days4"))\
        .withColumn("APP_Activation_Days5", f.col("nest.APP_Activation_Days5"))\
        .withColumn("APP_Activation_Weeks1", f.col("nest.APP_Activation_Weeks1"))\
        .withColumn("APP_Activation_Weeks2", f.col("nest.APP_Activation_Weeks2"))\
        .withColumn("APP_Activation_Weeks3", f.col("nest.APP_Activation_Weeks3"))\
        .withColumn("APP_Activation_Weeks4", f.col("nest.APP_Activation_Weeks4"))\
        .withColumn("APP_Activation_Weeks5", f.col("nest.APP_Activation_Weeks5"))\
        .withColumn("APP_Activation_Month1", f.col("nest.APP_Activation_Month1"))\
        .withColumn("APP_Activation_Month2", f.col("nest.APP_Activation_Month2"))\
        .withColumn("APP_Activation_Month3", f.col("nest.APP_Activation_Month3"))\
        .withColumn("APP_Activation_Month4", f.col("nest.APP_Activation_Month4"))\
        .withColumn("APP_Activation_Month5", f.col("nest.APP_Activation_Month5"))\
        .drop(*["nest"])

    ls_keep = [
        "AIL_AvgMonthsOnBook",
        "ALL_AvgMonthsOnBook",
        "ALL_DaysSinceMRPayment",
        "ALL_MaxDelq180DaysLT24M",
        "ALL_MaxDelqEver",
        "ALL_Notices5Years",
        "ALL_Num0Delq1Year",
        "ALL_NumEnqs180Days",
        "ALL_NumEnqs30Days",
        "ALL_NumEnqs7Days",
        "ALL_NumEnqs90Days",
        "ALL_NumPayments2Years",
        "ALL_NumPayments90Days",
        "ALL_NumTrades180Days",
        "ALL_NumTrades30Days",
        "ALL_NumTrades90Days",
        "ALL_Perc0Delq90Days",
        "ALL_PercPayments2Years",
        "ALL_PercPayments90Days",
        "ALL_TimeOldestTrade",
        "ALT_AirtimePurchasedAvg36M",
        "ALT_TAU",
        "APP_Account_Number1",
        "APP_Account_Number2",
        "APP_Account_Number3",
        "APP_Account1",
        "APP_Account2",
        "APP_Account3",
        "APP_Account4",
        "APP_Account5",
        "APP_Accounts",
        "APP_Activation_Date1",
        "APP_Activation_Date2",
        "APP_Activation_Date3",
        "APP_Activation_Date4",
        "APP_Activation_Date5",
        "APP_Activation_Days1",
        "APP_Activation_Days2",
        "APP_Activation_Days3",
        "APP_Activation_Days4",
        "APP_Activation_Days5",
        "CON_PERIOD",
        "APP_Activation_Month1",
        "APP_Activation_Month2",
        "APP_Activation_Month3",
        "APP_Activation_Month4",
        "APP_Activation_Month5",
        "APP_Activation_Weeks1",
        "APP_Activation_Weeks2",
        "APP_Activation_Weeks3",
        "APP_Activation_Weeks4",
        "APP_Activation_Weeks5",
        "APP_Channel",
        "APP_Channel_CEC",
        "APP_Customer_Score",
        "APP_Customer_Score_CNT",
        "APP_Date",
        "APP_Decision_Outcome",
        "APP_Decision_Service",
        "APP_Decision_Service_Waterfall",
        "APP_Gross_Income",
        "APP_Gross_Income_CNT",
        "APP_IDNumber",
        "APP_Max_Product_Term",
        "APP_Max_Product_Term_CNT",
        "APP_Month",
        "APP_Predicted_Income",
        "APP_Predicted_Income_CNT",
        "APP_Record_Number",
        "APP_Risk_Grade",
        "APP_SLIR",
        "APP_Subscriber_Number1",
        "APP_Subscriber_Number2",
        "APP_Subscriber_Number3",
        "APP_Subscriber_Number4",
        "APP_Subscriber_Number5",
        "APP_Subscription_Limit",
        "APP_Subscription_Limit_CNT",
        "APP_Subscriptions",
        "BNK_NumOpenTrades",
        "CAM_Customer_Score",
        "CAM_Customer_Score_CNT",
        "CAM_Risk_Grade",
        "CBX_Prism_TM",
        "CBX_Prism_TM_CNT",
        "CBX_Sabre_TM",
        "CBX_Sabre_TM_CNT",
        "COM_NumOpenTrades",
        "COM_NumTrades2Years",
        "CRC_NumOpenTrades",
        "CSN_NumTrades60Days",
        "CST_Citizen",
        "CST_CustomerAge",
        "CST_DebtReviewGranted",
        "CST_DebtReviewRequested",
        "CST_Deceased",
        "CST_Dispute",
        "CST_Emigrated",
        "CST_Fraud",
        "CST_Sequestration",
        "DUP_Applicant",
        "DUP_Application_Sequence",
        "DUP_Calendar_Months_Skipped",
        "DUP_Days_Between_Applications",
        "DUP_Decision_Outcome",
        "DUP_Risk_Grade",
        "EST_Customer_Score",
        "EST_Customer_Score_CNT",
        "EST_Risk_Grade",
        "Filter_Approved_No_Activations",
        "Filter_Approved_With_Activations",
        "Filter_Arrears_Activations",
        "Filter_Arrears_No_Activations",
        "Filter_Arrears_State",
        "Filter_Arrears_With_Activations",
        "Filter_Channel_Dealer",
        "Filter_Channel_Franchise",
        "Filter_Channel_Inbound",
        "Filter_Channel_Online",
        "Filter_Channel_Other",
        "Filter_Channel_Outbound",
        "Filter_Channel_Store",
        "Filter_Clear_Activations",
        "Filter_Clear_State",
        "Filter_Decision_Outcome_Approved",
        "Filter_Decision_Outcome_Arrears",
        "Filter_Decision_Outcome_Declined",
        "Filter_Decision_Outcome_Referred",
        "Filter_Decision_Outcome_SEQ",
        "Filter_Decision_Outcome_Unknown",
        "Filter_Declined_No_Activations",
        "Filter_Declined_With_Activations",
        "Filter_DMP_Campaign",
        "Filter_DMP_Established",
        "Filter_DMP_New",
        "Filter_DMP_Unknown",
        "Filter_Erratic_Activations",
        "Filter_Erratic_State",
        "Filter_First_Account_Applicant",
        "Filter_Immature_Activations",
        "Filter_Immature_State",
        "Filter_New_To_Credit",
        "Filter_Other_Activations",
        "Filter_Referred_No_Activations",
        "Filter_Referred_With_Activations",
        "Filter_Responsible_Activations",
        "Filter_Responsible_State",
        "Filter_Risk_Grade_1",
        "Filter_Risk_Grade_2",
        "Filter_Risk_Grade_3",
        "Filter_Risk_Grade_4",
        "Filter_Risk_Grade_5",
        "Filter_Risk_Grade_6",
        "Filter_Risk_Grade_7",
        "Filter_Risk_Grade_8",
        "Filter_Risk_Grade_9",
        "Filter_Risk_Grade_X",
        "Filter_Telesales_Inbound",
        "Filter_Telesales_Outbound",
        "Filter_Web_Service",
        "Filter_XXXXXX_State",
        "FRN_NumOpenTrades",
        "GMP_Risk_Grade",
        "IDKey",
        "IDNumber",
        "NEW_Customer_Score",
        "NEW_Customer_Score_CNT",
        "NEW_Risk_Grade",
        "NTC_Accept_Final_Score_V01",
        "NTC_Accept_Final_Score_V01_CNT",
        "NTC_Accept_Risk_Score_V01",
        "NTC_Accept_Risk_Score_V01_CNT",
        "RCG_NumTradesUtilisedLT10",
        "UND_GMIP_Ratio",
        "UND_Risk_Grade",
        "UNN_PercTradesUtilisedLT100MR60",
        "UNS_MaxDelq1YearLT24M"
    ]

    sdf_7 = sdf_6\
        .select(*[ls_keep])

    return sdf_7


def RiskGrade_Mandate(Applicants, APP_Accounts, BadRate12M, BadRate9M, BadRate6M):
    Risk_Grade_Mandate = None
    if Applicants is None:
        Applicants = 0
    if APP_Accounts is None:
        APP_Accounts = 0

    # Only Calculate the Risk_Grade_Mandate when there are 100+ applications and 50+ accounts touched.
    if (Applicants >= 100) and (APP_Accounts >= 50):
        # Calculate `RiskGradeBadRate12M`
        if (BadRate12M is None) or (BadRate12M == 0):
            RiskGradeBadRate12M = 0
        elif BadRate12M < 6.7:
            RiskGradeBadRate12M = 1
        elif BadRate12M < 12.4:
            RiskGradeBadRate12M = 2
        elif BadRate12M < 18.1:
            RiskGradeBadRate12M = 3
        elif BadRate12M < 25.1:
            RiskGradeBadRate12M = 4
        elif BadRate12M < 26.1:
            RiskGradeBadRate12M = 5
        elif BadRate12M < 39.2:
            RiskGradeBadRate12M = 6
        elif BadRate12M < 47.2:
            RiskGradeBadRate12M = 7
        elif BadRate12M < 57.1:
            RiskGradeBadRate12M = 8
        else:
            RiskGradeBadRate12M = 9

        # Calculate `RiskGradeBadRate9M`
        if (BadRate9M is None) or (BadRate9M == 0):
            RiskGradeBadRate9M = 0
        elif BadRate9M < 5.9:
            RiskGradeBadRate9M = 1
        elif BadRate9M < 11.1:
            RiskGradeBadRate9M = 2
        elif BadRate9M < 15.2:
            RiskGradeBadRate9M = 3
        elif BadRate9M < 20.9:
            RiskGradeBadRate9M = 4
        elif BadRate9M < 30.3:
            RiskGradeBadRate9M = 5
        elif BadRate9M < 31.7:
            RiskGradeBadRate9M = 6
        elif BadRate9M < 40.1:
            RiskGradeBadRate9M = 7
        elif BadRate9M < 50.3:
            RiskGradeBadRate9M = 8
        else:
            RiskGradeBadRate9M = 9

        # Calculate `RiskGradeBadRate6M`
        if (BadRate6M is None) or (BadRate6M == 0):
            RiskGradeBadRate6M = 0
        elif BadRate6M < 4.8:
            RiskGradeBadRate6M = 1
        elif BadRate6M < 9.3:
            RiskGradeBadRate6M = 2
        elif BadRate6M < 11.6:
            RiskGradeBadRate6M = 3
        elif BadRate6M < 15.1:
            RiskGradeBadRate6M = 4
        elif BadRate6M < 22.3:
            RiskGradeBadRate6M = 5
        elif BadRate6M < 23.8:
            RiskGradeBadRate6M = 6
        elif BadRate6M < 29.8:
            RiskGradeBadRate6M = 7
        elif BadRate6M < 39.3:
            RiskGradeBadRate6M = 8
        else:
            RiskGradeBadRate6M = 9

        # Calculate `Risk_Grade_Mandate`
        if (BadRate12M is not None) and (BadRate9M is not None) and (BadRate6M is not None):
            Risk_Grade_Mandate = int(10 * sum([RiskGradeBadRate12M, RiskGradeBadRate9M, RiskGradeBadRate6M]) / 3) / 10
        elif (BadRate12M is not None) and (BadRate9M is not None):
            Risk_Grade_Mandate = int(10 * sum([RiskGradeBadRate12M, RiskGradeBadRate9M]) / 2) / 10
        elif (BadRate9M is not None) and (BadRate6M is not None):
            Risk_Grade_Mandate = int(10 * sum([RiskGradeBadRate9M, RiskGradeBadRate6M]) / 2) / 10
        elif (BadRate9M is not None) and (BadRate6M is not None):
            Risk_Grade_Mandate = int(10 * sum([RiskGradeBadRate12M, RiskGradeBadRate6M]) / 2) / 10
        elif BadRate12M is not None:
            Risk_Grade_Mandate = BadRate12M
        elif BadRate9M is not None:
            Risk_Grade_Mandate = BadRate9M
        elif BadRate6M is not None:
            Risk_Grade_Mandate = BadRate6M
        else:
            pass
    else:
        pass
    return Risk_Grade_Mandate


udf_RiskGrade_Mandate = f.udf(RiskGrade_Mandate,
                              returnType=t.FloatType())


def Risk_Grade_Matrix(RiskGrade, SA1, SA2, SA3, SA4, SA5, SA6, CustomScore, SB1, SB2, SB3, SB4, SB5, SB6,
                      Decision_Services_Waterfall, CBX_Prism_TM):
    if (Decision_Services_Waterfall == "") or (Decision_Services_Waterfall is None):
        Decision_Services_Outcome = "Approve " + str(RiskGrade)
        Decision_Services_Waterfall = "P. Pass Credit Policies"
    else:
        Decision_Services_Outcome = "Decline"

    if CBX_Prism_TM < SA1:
        CBX_Prism_TM_BND = "001"
    elif CBX_Prism_TM < SA2:
        CBX_Prism_TM_BND = SA1
    elif CBX_Prism_TM < SA3:
        CBX_Prism_TM_BND = SA2
    elif CBX_Prism_TM < SA4:
        CBX_Prism_TM_BND = SA3
    elif CBX_Prism_TM < SA5:
        CBX_Prism_TM_BND = SA4
    elif CBX_Prism_TM < SA6:
        CBX_Prism_TM_BND = SA5
    else:
        CBX_Prism_TM_BND = SA6

    if CustomScore < SB1:
        y_BND = "001"
    elif CustomScore < SB2:
        y_BND = SB1
    elif CustomScore < SB3:
        y_BND = SB2
    elif CustomScore < SB4:
        y_BND = SB3
    elif CustomScore < SB5:
        y_BND = SB4
    elif CustomScore < SB6:
        y_BND = SB5
    else:
        y_BND = SB6

    Decision_Services_Matrix = str(CBX_Prism_TM_BND) + "x" + str(y_BND)

    return Decision_Services_Outcome, Decision_Services_Waterfall, Decision_Services_Matrix


schema_Risk_Grade_Matrix = t.StructType([
    t.StructField("Decision_Services_Outcome", t.StringType(), True),
    t.StructField("Decision_Services_Waterfall", t.StringType(), True),
    t.StructField("Decision_Services_Matrix", t.StringType(), True)
])

udf_Risk_Grade_Matrix = f.udf(Risk_Grade_Matrix, returnType=schema_Risk_Grade_Matrix)


def DUP_subroutine(sdf_inp):
    """
    This is the Python translation of STEP 4 in `Input_Applications_DMP.sas`.
    In STEP 4, we sort by the optimal decision services outcome, best risk grade obtained, and highest subscription
    limit within the acceptable period.
    """
    def change_day(x_dte):
        if x_dte is None:
            return None
        else:
            y_dte = x_dte.replace(day=1)
            return y_dte
    udf_change_day = f.udf(change_day,
                           returnType=t.DateType())

    sdf_0 = sdf_inp\
        .repartition(1)\
        .orderBy([f.col("IDKey").asc(),
                  f.col("DUP_Application_Sequence").asc(),
                  f.col("Filter_Decision_Outcome_SEQ").asc(),
                  f.col("APP_Risk_Grade").asc(),
                  f.col("APP_Gross_Income").desc(),
                  f.col("APP_Subscription_Limit").desc(),
                  f.col("APP_Date").asc()])

    windowspecIDKEY = Window \
        .partitionBy(f.col("IDKey")) \
        .orderBy([f.col("IDKey").asc(),
                  f.col("DUP_Application_Sequence").asc(),
                  f.col("Filter_Decision_Outcome_SEQ").asc(),
                  f.col("APP_Risk_Grade").asc(),
                  f.col("APP_Gross_Income").desc(),
                  f.col("APP_Subscription_Limit").desc(),
                  f.col("APP_Date").asc()])

    sdf_1 = sdf_0\
        .withColumn("RET_IDKey", f.lag(f.col("IDKey"), 1).over(windowspecIDKEY))\
        .withColumn("RET_Date", f.lag(f.col("APP_Date"), 1).over(windowspecIDKEY))\
        .withColumn("RET_Application_Sequence", f.lag(f.col("DUP_Application_Sequence"), 1).over(windowspecIDKEY))\
        .withColumn("APP_Month_dte", udf_change_day(f.col("APP_Date")))\
        .withColumn("RET_Month", f.lag(f.col("APP_Month"), 1).over(windowspecIDKEY))\
        .withColumn("RET_Month_dte", udf_change_day(f.col("RET_Date")))\
        .withColumn("RET_Decision_Outcome", f.lag(f.col("Filter_Decision_Outcome_SEQ"), 1).over(windowspecIDKEY))\
        .withColumn("RET_Risk_Grade", f.lag(f.col("APP_Risk_Grade"), 1).over(windowspecIDKEY))\
        .withColumn("DUP_Days_Between_Applications", f.lit(None))

    sdf_2 = sdf_1\
        .withColumn("DUP_Days_Between_Applications", f.when((f.col("IDKey") == f.col("RET_IDKey")),
                                                            f.datediff(f.col("APP_Date"), f.col("RET_Date")))
                                                      .otherwise(f.lit(None)))\
        .withColumn("DUP_Applicant", f.when((f.col("IDKey") == f.col("RET_IDKey")) &
                                            (f.col("RET_Application_Sequence") == f.col("DUP_Application_Sequence")),
                                            f.lit("Z"))
                                      .otherwise(f.col("DUP_Applicant")))\
        .withColumn("DUP_Calendar_Months_Skipped", f.when((f.col("IDKey") == f.col("RET_IDKey")) &
                                                          (f.col("RET_Application_Sequence") == f.col("DUP_Application_Sequence")) &
                                                          (f.col("APP_Month") != f.col("RET_Month")),
                                                          f.months_between(f.col("APP_Month_dte"), f.col("RET_Month_dte")))
                                                    .otherwise(f.lit(None)))\
        .withColumn("DUP_Decision_Outcome", f.when((f.col("IDKey") == f.col("RET_IDKey")) &
                                                   (f.col("RET_Application_Sequence") == f.col("DUP_Application_Sequence")) &
                                                   f.col("Filter_Decision_Outcome_SEQ").isin([1, 2, 3]) &
                                                   f.col("RET_Decision_Outcome").isin([1, 2, 3]) &
                                                   (f.col("Filter_Decision_Outcome_SEQ") != f.col("RET_Decision_Outcome")),
                                                   f.col("RET_Decision_Outcome").astype(t.IntegerType()) - f.col("Filter_Decision_Outcome_SEQ").astype(t.IntegerType()))
                                            .otherwise(f.lit(None)))\
        .withColumn("DUP_Risk_Grade", f.when((f.col("IDKey") == f.col("RET_IDKey")) &
                                             (f.col("RET_Application_Sequence") == f.col("DUP_Application_Sequence")) &
                                             f.col("APP_Risk_Grade").isNotNull() &
                                             f.col("RET_Risk_Grade").isNotNull() &
                                             (f.col("APP_Risk_Grade") != f.col("RET_Risk_Grade")),
                                             f.col("RET_Risk_Grade").astype(t.IntegerType()) - f.col("APP_Risk_Grade").astype(t.IntegerType()))
                                       .otherwise(f.lit(None)))\
        .drop(*["RET_IDKey",
                "RET_Date",
                "RET_Application_Sequence",
                "RET_Month",
                "RET_Decision_Outcome",
                "RET_Risk_Grade"])

    return sdf_2


def Flag_DUP_Applicant(SRT, sdf_inp, DAY=14):
    if SRT.upper() == "DESCENDING":
        sdf_0 = sdf_inp\
            .repartition(1)\
            .orderBy([f.col("IDKey").asc(),
                      f.col("APP_Date").desc(),
                      f.col("APP_Record_Number").asc()])
        windowspecDESC = Window\
            .partitionBy(f.col("IDKey")) \
            .orderBy([f.col("IDKey").asc(),
                      f.col("APP_Date").desc(),
                      f.col("APP_Record_Number").asc()])
        sdf_1 = sdf_0\
            .repartition(1)\
            .orderBy([f.col("IDKey").asc(),
                      f.col("APP_Date").desc(),
                      f.col("APP_Record_Number").asc()])\
            .withColumn("APP_Record_Number", f.monotonically_increasing_id())
        sdf_2 = sdf_1\
            .repartition(1)\
            .withColumn("RET_IDKey", f.lag(f.col("IDKey"), 1).over(windowspecDESC))\
            .withColumn("RET_Date", f.lag(f.col("APP_Date"), 1).over(windowspecDESC))\
            .withColumn("RET_Days_Between_Applications", f.lag(f.col("DUP_Days_Between_Applications"), 1).over(windowspecDESC))\
            .withColumn("RET_Application_Sequence", f.lag(f.col("DUP_Application_Sequence"), 1).over(windowspecDESC))
    else:
        sdf_0 = sdf_inp\
            .repartition(1)\
            .orderBy([f.col("IDKey").asc(),
                      f.col("APP_Date").asc(),
                      f.col("APP_Record_Number").desc()])
        windowspecASC = Window \
            .partitionBy(f.col("IDKey")) \
            .orderBy([f.col("IDKey").asc(),
                      f.col("APP_Date").asc(),
                      f.col("APP_Record_Number").desc()])
        sdf_1 = sdf_0\
            .repartition(1)\
            .orderBy([f.col("IDKey").asc(),
                      f.col("APP_Date").asc(),
                      f.col("APP_Record_Number").desc()])\
            .withColumn("APP_Record_Number", f.monotonically_increasing_id())
        sdf_2 = sdf_1 \
            .repartition(1) \
            .withColumn("RET_IDKey", f.lag(f.col("IDKey"), 1).over(windowspecASC)) \
            .withColumn("RET_Date", f.lag(f.col("APP_Date"), 1).over(windowspecASC)) \
            .withColumn("RET_Days_Between_Applications",
                        f.lag(f.col("DUP_Days_Between_Applications"), 1).over(windowspecASC)) \
            .withColumn("RET_Application_Sequence", f.lag(f.col("DUP_Application_Sequence"), 1).over(windowspecASC))

    sdf_2a = sdf_2\
        .withColumn("RET_IDKey", f.when(f.col("IDKey").isNotNull() & f.col("RET_IDKey").isNull(),
                                        f.col("IDKey"))
                                  .otherwise(f.col("RET_IDKey")))\
        .withColumn("RET_Date", f.when(f.col("APP_Date").isNotNull() & f.col("RET_Date").isNull(),
                                       f.col("APP_Date"))
                                 .otherwise(f.col("RET_Date")))\
        .withColumn("RET_Days_Between_Applications",
                    f.when(f.col("DUP_Days_Between_Applications").isNotNull() &
                           f.col("RET_Days_Between_Applications").isNull(),
                           f.col("DUP_Days_Between_Applications"))
                     .otherwise(f.col("RET_Days_Between_Applications")))\
        .withColumn("RET_Application_Sequence",
                    f.when(f.col("DUP_Application_Sequence").isNotNull() &
                           f.col("RET_Application_Sequence").isNull(),
                           f.col("DUP_Application_Sequence"))
                     .otherwise(f.col("RET_Application_Sequence")))

    sdf_3 = sdf_2a\
        .withColumn("DUP_Applicant", f.when((f.col("IDKey") == f.col("RET_IDKey")),
                                            f.lit("Y"))
                                      .otherwise(f.lit("N")))

    if SRT.upper() == "ASCENDING":
        sdf_4 = sdf_3\
            .withColumn("DUP_Days_Between_Applications",
                        f.when((f.col("IDKey") == f.col("RET_IDKey")),
                               f.datediff(f.col("APP_Date"), f.col("RET_Date")))
                         .otherwise(f.col("DUP_Days_Between_Applications")))\
            .withColumn("RET_Days_Between_Applications",
                        f.when((f.col("IDKey") == f.col("RET_IDKey")),
                               reduce(add, [f.col("RET_Days_Between_Applications"),
                                            f.col("DUP_Days_Between_Applications")]))
                         .otherwise(f.col("RET_Days_Between_Applications")))\
            .withColumn("RET_Days_Between_Applications",
                        f.when((f.col("IDKey") == f.col("RET_IDKey")) & (f.col("RET_Days_Between_Applications") > f.lit(DAY)),
                               f.lit(None))
                         .otherwise(f.col("RET_Days_Between_Applications")))\
            .withColumn("RET_Application_Sequence",
                        f.when((f.col("IDKey") == f.col("RET_IDKey")) & (f.col("RET_Days_Between_Applications") > f.lit(DAY)),
                               reduce(add, [f.col("RET_Application_Sequence"), f.lit(1)]))
                         .otherwise(f.lit(None)))\
            .withColumn("DUP_Application_Sequence",
                        f.when((f.col("IDKey") == f.col("RET_IDKey")),
                               f.col("RET_Application_Sequence"))
                         .otherwise(f.col("DUP_Application_Sequence")))
    else:
        sdf_4 = sdf_3\
            .select("*")

    sdf_5 = sdf_4\
        .withColumn("DUP_Days_Between_Applications",
                    f.when((f.col("IDKey") != f.col("RET_IDKey")),
                           f.lit(None))
                     .otherwise(f.col("DUP_Days_Between_Applications")))\
        .withColumn("RET_Days_Between_Applications",
                    f.when((f.col("IDKey") != f.col("RET_IDKey")),
                           f.lit(None))
                    .otherwise(f.col("RET_Days_Between_Applications"))) \
        .withColumn("DUP_Application_Sequence",
                    f.when((f.col("IDKey") != f.col("RET_IDKey")),
                           f.lit(0))
                    .otherwise(f.col("DUP_Application_Sequence"))) \
        .withColumn("RET_Application_Sequence",
                    f.when((f.col("IDKey") != f.col("RET_IDKey")),
                           f.lit(0))
                    .otherwise(f.col("RET_Application_Sequence")))

    sdf_6 = sdf_5\
        .drop(*["RET_IDKey",
                "RET_Date",
                "RET_Days_Between_Applications",
                "RET_Application_Sequence"])

    return sdf_6


# def Risk_Grade_Matrix(sdf_inp, RiskGrade, SA1, SA2, SA3, SA4, SA5, SA6,
#                       CustomScore, SB1, SB2, SB3, SB4, SB5, SB6):
#     sdf_0 = sdf_inp\
#         .withColumn("Decision_Services_Outcome",
#                     f.when(f.col("Decision_Services_Waterfall") == "",
#                            f.concat(f.lit("Approve "), f.lit(RiskGrade)))
#                      .otherwise(f.lit("Decline")))\
#         .withColumn("Decision_Services_Waterfall",
#                     f.when(f.col("Decision_Services_Waterfall") == "",
#                            f.lit("P. Pass Credit Policies"))
#                      .otherwise(f.col("Decision_Services_Waterfall")))
#
#     sdf_1 = sdf_0\
#         .withColumn("CBX_Prism_TM_BND",
#                     f.when(f.col("CBX_Prism_TM") < SA1,
#                            f.lit("001"))
#                      .when(f.col("CBX_Prism_TM") < SA2,
#                            f.lit(SA1))
#                      .when(f.col("CBX_Prism_TM") < SA3,
#                            f.lit(SA2))
#                      .when(f.col("CBX_Prism_TM") < SA4,
#                            f.lit(SA3))
#                      .when(f.col("CBX_Prism_TM") < SA5,
#                            f.lit(SA4))
#                      .when(f.col("CBX_Prism_TM") < SA6,
#                            f.lit(SA5))
#                      .otherwise(f.lit(SA6)))
#
#     sdf_2 = sdf_1\
#         .withColumn((CustomScore + "_BND"),
#                     f.when(f.col(CustomScore) < SB1,
#                            f.lit("001"))
#                      .when(f.col(CustomScore) < SB2,
#                            f.lit(SB1))
#                      .when(f.col(CustomScore) < SB3,
#                            f.lit(SB2))
#                      .when(f.col(CustomScore) < SB4,
#                            f.lit(SB3))
#                      .when(f.col(CustomScore) < SB5,
#                            f.lit(SB4))
#                      .when(f.col(CustomScore) < SB6,
#                            f.lit(SB5))
#                      .otherwise(f.lit(SB6)))
#
#     sdf_3 = sdf_2\
#         .withColumn("Decision_Services_Matrix",
#                     f.concat(f.col("CBX_Prism_TM_BND"), f.lit("x"), f.col(CustomScore + "_BND")))
#
#     return sdf_3
