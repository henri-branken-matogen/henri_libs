import pyspark.sql.types as t
import pyspark.sql.functions as f
from functools import reduce
from operator import add

ls_ds_summary = [
    "FREQ",
    "APP_Subscriptions",
    "APP_Accounts",
    "Filter_Decision_Outcome_Approved",
    "Filter_Decision_Outcome_Referred",
    "Filter_Decision_Outcome_Declined",
    "Filter_Approved_With_Activations",
    "Filter_Referred_With_Activations",
    "Filter_Declined_With_Activations",
    "Filter_Approved_No_Activations",
    "Filter_Referred_No_Activations",
    "Filter_Declined_No_Activations",
    "BadRate1M",
    "BadRate3M",
    "BadRate6M",
    "BadRate9M",
    "BadRate12M",
    "Goods1",
    "Bads1",
    "Goods3",
    "Bads3",
    "Goods6",
    "Bads6",
    "Goods9",
    "Bads9",
    "Goods12",
    "Bads12",
    "APP_Customer_Score",
    "APP_Customer_Score_CNT",
    "NEW_Customer_Score",
    "NEW_Customer_Score_CNT",
    "CAM_Customer_Score",
    "CAM_Customer_Score_CNT",
    "EST_Customer_Score",
    "EST_Customer_Score_CNT",
    "CBX_Prism_TM",
    "CBX_Prism_TM_CNT",
    "NTC_Accept_Risk_Score_V01",
    "NTC_Accept_Risk_Score_V01_CNT",
    "NTC_Accept_Final_Score_V01",
    "NTC_Accept_Final_Score_V01_CNT",
    "APP_Subscription_Limit",
    "APP_Subscription_Limit_CNT",
    "APP_Gross_Income",
    "APP_Gross_Income_CNT",
    "APP_Predicted_Income",
    "APP_Predicted_Income_CNT"
]


def Decision_Service_Waterfall(CST_Deceased, CST_Customer_Age, CST_Fraud, CST_Sequestration, CST_Dispute,
                               APP_Customer_State, CST_Emigrated, ALL_Notices5Years, CST_DebtReviewGranted,
                               All_TimeOldestTrade, APP_Gross_Income, APP_Subscriptions, Customer_State,
                               Filter_Clear_State, Filter_Responsible_State, Filter_Erratic_State,
                               Filter_Arrears_State, Filter_Channel_Outbound, Filter_Channel_Inbound,
                               Filter_Channel_Online):
    Filter_New_To_Credit = Filter_Telesales_Outbound = Filter_Telesales_Inbound = None
    Filter_Web_Service = Filter_First_Account_Applicant = Filter_XXXXXX_State = Filter_Immature_Activations = None
    Filter_Clear_Activations = Filter_Responsible_Activations = Filter_Erratic_Activations = None
    Filter_Arrears_Activations = Filter_Other_Activations = Filter_Immature_State = None

    if CST_Deceased == "Y":
        APP_Decision_Service_Waterfall = "A.01 Bureau Policy Decline (P01=Consumer Deceased)"
    elif CST_Customer_Age < 1800:
        APP_Decision_Service_Waterfall = "A.02 Bureau Policy Decline (P02=Consumer Age < 18)"
    elif CST_Fraud == "Y":
        APP_Decision_Service_Waterfall = "A.03 Bureau Policy Decline (P03=Payment Profile Fraud)"
    elif CST_Sequestration == "Y":
        APP_Decision_Service_Waterfall = "A.04 Bureau Policy Decline (P04=Consumer Sequestrated)"
    elif (CST_Dispute == "Y") and (APP_Customer_State != "CLR Clear"):
        APP_Decision_Service_Waterfall = "A.05 Bureau Policy Decline (P05=Payment Profile Dispute, except CLR)"
    elif CST_Emigrated == "Y":
        APP_Decision_Service_Waterfall = "A.06 Bureau Policy Decline (P06=Consumer Immigrated)"
    elif ALL_Notices5Years == "Y":
        APP_Decision_Service_Waterfall = "A.07 Bureau Policy Decline (P07=Public Notice Issued)"
    elif (CST_DebtReviewGranted == "Y") and (APP_Customer_State != "CLR Clear"):
        APP_Decision_Service_Waterfall = "A.08 Bureau Policy Decline (P08=Debt Review Granted, except CLR)"
    elif (All_TimeOldestTrade < 3) and (APP_Gross_Income < 1500):
        APP_Decision_Service_Waterfall = "B.01 Thin File (C05=Gross Income < R1500)"
    elif (APP_Customer_State == "") and (APP_Gross_Income < 1500):
        APP_Decision_Service_Waterfall = "B.02 New To Credit (C05=Gross Income < R1500)"
    elif (APP_Customer_State == "IMM MOB 00-05") and (APP_Gross_Income < 1500):
        APP_Decision_Service_Waterfall = "B.03 Immature Customer State (C05=Gross Income < R1500)"
    elif All_TimeOldestTrade < 3:
        APP_Decision_Service_Waterfall = "C. New To Credit (Thin File)"
        Filter_New_To_Credit = 1
    elif (APP_Customer_State in ["", "IMM MOB 00-05"]) and (Filter_Channel_Outbound is not None):
        APP_Decision_Service_Waterfall = "D. Telesales Outbound"
        Filter_Telesales_Outbound = 1
    elif (APP_Customer_State in ["", "IMM MOB 00-05"]) and (Filter_Channel_Inbound is not None):
        APP_Decision_Service_Waterfall = "E. Telesales Inbound"
        Filter_Telesales_Inbound = 1
    elif (APP_Customer_State in ["", "IMM MOB 00-05"]) and (Filter_Channel_Online is not None):
        APP_Decision_Service_Waterfall = "F. World Wide Web"
        Filter_Web_Service = 1
    elif APP_Customer_State == "":
        APP_Decision_Service_Waterfall = "G. First Account Applicant"
        Filter_First_Account_Applicant = 1
    elif APP_Customer_State == "IMM MOB 00-05":
        APP_Decision_Service_Waterfall = "H. Immature Customer State"
        Filter_Immature_State = 1
    elif APP_Customer_State == "CLR Clear":
        APP_Decision_Service_Waterfall = "I. Clear Customer State"
        Filter_Clear_State = 1
    elif APP_Customer_State == "RES Responsible":
        APP_Decision_Service_Waterfall = "J. Responsible Customer State"
        Filter_Responsible_State = 1
    elif APP_Customer_State == "ERR Erratic":
        APP_Decision_Service_Waterfall = "K. Erratic Customer State"
        Filter_Erratic_State = 1
    elif APP_Customer_State == "EXT Extended":
        APP_Decision_Service_Waterfall = "L. Arrears Customer State"
        Filter_Arrears_State = 1
    elif APP_Customer_State == "DIS Distressed":
        APP_Decision_Service_Waterfall = "L. Arrears Customer State"
        Filter_Arrears_State = 1
    elif APP_Customer_State == "DBT Doubtful Debt":
        APP_Decision_Service_Waterfall = "L. Arrears Customer State"
        Filter_Arrears_State = 1
    elif APP_Customer_State == "CRD Credit Balance":
        APP_Decision_Service_Waterfall = "X.01 Catch-All (Credit Balance)"
        Filter_XXXXXX_State = 1
    elif APP_Customer_State == "PUP Paid-Up":
        APP_Decision_Service_Waterfall = "X.02 Catch-All (Paid Up)"
        Filter_XXXXXX_State = 1
    else:
        APP_Decision_Service_Waterfall = "X.03 Catch-All"
        Filter_XXXXXX_State = 1

    if APP_Subscriptions is not None:
        if Customer_State == "IMM MOB 00-05":
            Filter_Immature_Activations = 1
        elif Filter_Clear_State == 1:
            Filter_Clear_Activations = 1
        elif Filter_Responsible_State == 1:
            Filter_Responsible_Activations = 1
        elif Filter_Erratic_State == 1:
            Filter_Erratic_Activations = 1
        elif Filter_Arrears_State == 1:
            Filter_Arrears_Activations = 1
        else:
            Filter_Other_Activations = 1

    return (APP_Decision_Service_Waterfall, Filter_New_To_Credit, Filter_Telesales_Outbound, Filter_Telesales_Inbound,
            Filter_Web_Service, Filter_First_Account_Applicant, Filter_XXXXXX_State, Filter_Immature_Activations,
            Filter_Clear_Activations, Filter_Responsible_Activations, Filter_Erratic_Activations,
            Filter_Arrears_Activations, Filter_Other_Activations, Filter_Immature_State)


schema_Decision_Service_Waterfall = t.StructType([
    t.StructField("APP_Decision_Service_Waterfall", t.StringType(), True),
    t.StructField("Filter_New_To_Credit", t.IntegerType(), True),
    t.StructField("Filter_Telesales_Outbound", t.IntegerType(), True),
    t.StructField("Filter_Telesales_Inbound", t.IntegerType(), True),
    t.StructField("Filter_Web_Service", t.IntegerType(), True),
    t.StructField("Filter_First_Account_Applicant", t.IntegerType(), True),
    t.StructField("Filter_XXXXXX_State", t.IntegerType(), True),
    t.StructField("Filter_Immature_Activations", t.IntegerType(), True),
    t.StructField("Filter_Clear_Activations", t.IntegerType(), True),
    t.StructField("Filter_Responsible_Activations", t.IntegerType(), True),
    t.StructField("Filter_Erratic_Activations", t.IntegerType(), True),
    t.StructField("Filter_Arrears_Activations", t.IntegerType(), True),
    t.StructField("Filter_Other_Activations", t.IntegerType(), True),
    t.StructField("Filter_Immature_State", t.IntegerType(), True)
])

udf_Decision_Service_Waterfall = f.udf(Decision_Service_Waterfall,
                                       returnType=schema_Decision_Service_Waterfall)


def Decision_Services_Metrics(sdf_0):
    sdf_1 = sdf_0\
        .withColumn("TMP_Applications", reduce(add, [f.col("Filter_Decision_Outcome_Approved"),
                                                     f.col("Filter_Decision_Outcome_Referred"),
                                                     f.col("Filter_Decision_Outcome_Declined")]))\
        .withColumn("Filter_Decision_Outcome_Approved",
                    f.lit(100) * f.col("Filter_Decision_Outcome_Approved") / f.col("TMP_Applications"))\
        .withColumn("Filter_Decision_Outcome_Referred",
                    f.lit(100) * f.col("Filter_Decision_Outcome_Referred") / f.col("TMP_Applications"))\
        .withColumn("Filter_Decision_Outcome_Declined",
                    f.lit(100) * f.col("Filter_Decision_Outcome_Declined") / f.col("TMP_Applications"))\
        .withColumn("Filter_Approved_With_Activations",
                    f.when(f.col("Filter_Approved_With_Activations").isNotNull(),
                           f.lit(100) * f.col("Filter_Approved_With_Activations") /
                           reduce(add, [f.col("Filter_Approved_With_Activations"),
                                        f.col("Filter_Approved_No_Activations")])))\
        .withColumn("Filter_Referred_With_Activations",
                f.when(f.col("Filter_Referred_With_Activations").isNotNull(),
                       f.lit(100) * f.col("Filter_Referred_With_Activations") /
                       reduce(add, [f.col("Filter_Referred_With_Activations"),
                                    f.col("Filter_Referred_No_Activations")]))) \
        .withColumn("Filter_Declined_With_Activations",
                    f.when(f.col("Filter_Declined_With_Activations").isNotNull(),
                           f.lit(100) * f.col("Filter_Declined_With_Activations") /
                           reduce(add, [f.col("Filter_Declined_With_Activations"),
                                        f.col("Filter_Declined_No_Activations")])))\
        .withColumn("BadRate1M",
                    (f.lit(1000) * f.col("Bads1") / reduce(add, [f.col("Goods1"), f.col("Bads1")]).astype(t.IntegerType())) / f.lit(10))\
        .withColumn("BadRate3M",
                    (f.lit(1000) * f.col("Bads3") / reduce(add, [f.col("Goods3"), f.col("Bads3")]).astype(t.IntegerType())) / f.lit(10))\
        .withColumn("BadRate6M",
                    (f.lit(1000) * f.col("Bads6") / reduce(add, [f.col("Goods6"), f.col("Bads6")]).astype(t.IntegerType())) / f.lit(10))\
        .withColumn("BadRate9M",
                    (f.lit(1000) * f.col("Bads9") / reduce(add, [f.col("Goods9"), f.col("Bads9")]).astype(t.IntegerType())) / f.lit(10))\
        .withColumn("BadRate12M",
                    (f.lit(1000) * f.col("Bads12") / reduce(add, [f.col("Goods12"), f.col("Bads12")]).astype(t.IntegerType())) / f.lit(10))\
        .withColumn("APP_Customer_Score",
                    f.when(f.col("APP_Customer_Score_CNT").isNotNull(),
                           (f.col("APP_Customer_Score") / f.col("APP_Customer_Score_CNT")).astype(t.IntegerType()))
                     .otherwise(f.lit(None)))\
        .withColumn("NEW_Customer_Score",
                    f.when(f.col("NEW_Customer_Score_CNT").isNotNull(),
                           (f.col("NEW_Customer_Score") / f.col("NEW_Customer_Score_CNT")).astype(t.IntegerType()))
                     .otherwise(f.lit(None)))\
        .withColumn("CAM_Customer_Score",
                    f.when(f.col("CAM_Customer_Score_CNT").isNotNull(),
                           (f.col("CAM_Customer_Score") / f.col("CAM_Customer_Score_CNT")).astype(t.IntegerType()))
                     .otherwise(f.lit(None)))\
        .withColumn("EST_Customer_Score",
                    f.when(f.col("EST_Customer_Score_CNT").isNotNull(),
                           (f.col("EST_Customer_Score") / f.col("EST_Customer_Score_CNT")).astype(t.IntegerType()))
                     .otherwise(f.lit(None)))\
        .withColumn("CBX_Prism_TM",
                    f.when(f.col("CBX_Prism_TM_CNT").isNotNull(),
                           (f.col("CBX_Prism_TM") / f.col("CBX_Prism_TM_CNT")).astype(t.IntegerType()))
                     .otherwise(f.lit(None))) \
        .withColumn("NTC_Accept_Risk_Score_V01",
                    f.when(f.col("NTC_Accept_Risk_Score_V01_CNT").isNotNull(),
                           (f.col("NTC_Accept_Risk_Score_V01") / f.col("NTC_Accept_Risk_Score_V01_CNT")).astype(t.IntegerType()))
                     .otherwise(f.lit(None))) \
        .withColumn("NTC_Accept_Final_Score_V01",
                    f.when(f.col("NTC_Accept_Final_Score_V01_CNT").isNotNull(),
                           (f.col("NTC_Accept_Final_Score_V01") / f.col("NTC_Accept_Final_Score_V01_CNT")).astype(t.IntegerType()))
                     .otherwise(f.lit(None))) \
        .withColumn("APP_Subscription_Limit",
                    f.when(f.col("APP_Subscription_Limit_CNT").isNotNull(),
                           (f.col("APP_Subscription_Limit") / f.col("APP_Subscription_Limit_CNT")).astype(t.IntegerType()))
                     .otherwise(f.lit(None))) \
        .withColumn("APP_Gross_Income",
                    f.when(f.col("APP_Gross_Income_CNT").isNotNull(),
                           (f.col("APP_Gross_Income") / f.col("APP_Gross_Income_CNT")).astype(t.IntegerType()))
                     .otherwise(f.lit(None))) \
        .withColumn("APP_Predicted_Income",
                    f.when(f.col("APP_Predicted_Income_CNT").isNotNull(),
                           (f.col("APP_Predicted_Income") / f.col("APP_Predicted_Income_CNT")).astype(t.IntegerType()))
                     .otherwise(f.lit(None)))
    return sdf_1


def Decision_Service_Report(sdf_0, VAR):
    sdf_0 = sdf_0\
        .withColumn("FREQ", f.lit(1))

    sdf_1 = sdf_0\
        .groupBy(*[f.col("APP_Decision_Service_Waterfall"), f.col(VAR)])\
        .agg(*[f.sum(f.col(x)).alias(x) for x in ls_ds_summary])\
        .withColumnRenamed("FREQ", "Applicants")

    sdf_2 = Decision_Services_Metrics(sdf_1)
    return sdf_2
