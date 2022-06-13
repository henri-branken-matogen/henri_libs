import pyspark.sql.types as t
import pyspark.sql.functions as f
from stallion.apps import udf_RiskGrade_Mandate, udf_Risk_Grade_Matrix
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


def Decision_Services_Waterfall(sdf_inp):
    sdf_0 = sdf_inp\
        .withColumn("Decision_Services_Segment", f.lit(None).astype(t.StringType()))\
        .withColumn("Filter_New_To_Credit", f.lit(None).astype(t.IntegerType())) \
        .withColumn("Filter_Telesales_Outbound", f.lit(None).astype(t.IntegerType())) \
        .withColumn("Filter_Telesales_Inbound", f.lit(None).astype(t.IntegerType())) \
        .withColumn("Filter_Web_Service", f.lit(None).astype(t.IntegerType())) \
        .withColumn("Filter_First_Account_Applicant", f.lit(None).astype(t.IntegerType())) \
        .withColumn("Filter_Immature_State", f.lit(None).astype(t.IntegerType())) \
        .withColumn("Filter_XXXXXX_State", f.lit(None).astype(t.IntegerType())) \
        .withColumn("Filter_Clear_State", f.lit(None).astype(t.IntegerType())) \
        .withColumn("Filter_Responsible_State", f.lit(None).astype(t.IntegerType())) \
        .withColumn("Filter_Erratic_State", f.lit(None).astype(t.IntegerType())) \
        .withColumn("Filter_Arrears_State", f.lit(None).astype(t.IntegerType())) \
        .withColumn("Decision_Services_Waterfall", f.lit(None).astype(t.StringType()))\
        .withColumn("Decision_Services_Outcome", f.lit(None).astype(t.StringType()))\
        .withColumn("Decision_Services_Matrix", f.lit(None).astype(t.StringType()))\
        .withColumn("Decision_Services_Risk_Grade", f.lit(None).astype(t.StringType()))
    # NTC
    sdf_1a = sdf_0\
        .withColumn("nest",
                    udf_dsw_NTC(f.col("ALL_TimeOldestTrade"), f.col("Decision_Services_Segment"),
                                f.col("Filter_New_To_Credit"), f.col("Decision_Services_Waterfall"),
                                f.col("CST_Deceased"), f.col("CST_CustomerAge"), f.col("CST_Fraud"),
                                f.col("CST_Sequestration"), f.col("CST_Dispute"), f.col("CST_Emigrated"),
                                f.col("ALL_Notices5Years"), f.col("CST_DebtReviewGranted"),
                                f.col("CST_DebtReviewRequested"), f.col("APP_Gross_Income"),
                                f.col("NTC_Accept_Final_Score_V01"), f.col("NTC_Accept_Risk_Score_V01")))\
        .withColumn("DECISION_SERVICES_SEGMENT", f.col("nest.DECISION_SERVICES_SEGMENT"))\
        .withColumn("FILTER_NEW_TO_CREDIT", f.col("nest.FILTER_NEW_TO_CREDIT"))\
        .withColumn("DECISION_SERVICES_WATERFALL", f.col("nest.DECISION_SERVICES_WATERFALL"))\
        .drop(*["nest"])

    sdf_1b = sdf_1a \
            .withColumn("nest",
                        f.when((f.col("All_TimeOldestTrade") < 3),
                               udf_Risk_Grade_Matrix(f.lit("RG9"), f.lit(999), f.lit(999), f.lit(999), f.lit(999),
                                                     f.lit(999), f.lit(999), f.col("NEW_Customer_Score"), f.lit(999),
                                                     f.lit(999), f.lit(999), f.lit(999), f.lit(999), f.lit(999),
                                                     f.col("Decision_Services_Waterfall"), f.col("CBX_Prism_TM"))))\
            .withColumn("Decision_Services_Outcome",
                        f.when((f.col("All_TimeOldestTrade") < 3),
                               f.col("nest.Decision_Services_Outcome"))
                         .otherwise(f.col("Decision_Services_Outcome")))\
            .withColumn("Decision_Services_Waterfall",
                        f.when((f.col("All_TimeOldestTrade") < 3),
                               f.col("nest.Decision_Services_Waterfall"))
                         .otherwise(f.col("Decision_Services_Waterfall")))\
            .withColumn("Decision_Services_Matrix",
                        f.when((f.col("All_TimeOldestTrade") < 3),
                               f.col("nest.Decision_Services_Matrix"))
                         .otherwise(f.col("Decision_Services_Matrix")))\
            .drop(*["nest"])
    # TSO
    sdf_2a = sdf_1b\
        .withColumn("nest",
                    udf_dsw_TSO(f.col("APP_Customer_State"), f.col("Filter_Channel_Outbound"),
                                f.col("Decision_Services_Segment"), f.col("Filter_Telesales_Outbound"),
                                f.col("Decision_Services_Waterfall"), f.col("CST_Deceased"), f.col("CST_CustomerAge"),
                                f.col("CST_Fraud"), f.col("CST_Sequestration"), f.col("CST_Dispute"),
                                f.col("CST_Emigrated"), f.col("ALL_Notices5Years"), f.col("CST_DebtReviewGranted"),
                                f.col("CST_DebtReviewRequested"), f.col("APP_Gross_Income"), f.col("CBX_Prism_TM"),
                                f.col("NEW_Customer_Score"), f.col("ALL_Perc0Delq90Days"),
                                f.col("ALL_NumPayments90Days")))\
        .withColumn("DECISION_SERVICES_SEGMENT", f.col("nest.DECISION_SERVICES_SEGMENT"))\
        .withColumn("FILTER_TELESALES_OUTBOUND", f.col("nest.FILTER_TELESALES_OUTBOUND"))\
        .withColumn("DECISION_SERVICES_WATERFALL", f.col("nest.DECISION_SERVICES_WATERFALL"))\
        .drop(*["nest"])

    sdf_2b = sdf_2a\
        .withColumn("nest",
                    f.when((f.col("APP_Customer_State").isin(["", "IMM MOB 00-05"]) &
                           (f.col("Filter_Channel_Outbound").isNotNull())),
                           udf_Risk_Grade_Matrix(f.lit("RG8"), f.lit(580), f.lit(640), f.lit(999), f.lit(999),
                                                 f.lit(999), f.lit(999), f.col("NEW_Customer_Score"), f.lit(555),
                                                 f.lit(585), f.lit(999), f.lit(999), f.lit(999), f.lit(999),
                                                 f.col("Decision_Services_Waterfall"), f.col("CBX_Prism_TM"))))\
        .withColumn("Decision_Services_Outcome",
                    f.when((f.col("APP_Customer_State").isin(["", "IMM MOB 00-05"]) &
                           (f.col("Filter_Channel_Outbound").isNotNull())),
                           f.col("nest.Decision_Services_Outcome"))
                    .otherwise(f.col("Decision_Services_Outcome")))\
        .withColumn("Decision_Services_Waterfall",
                    f.when((f.col("APP_Customer_State").isin(["", "IMM MOB 00-05"]) &
                           (f.col("Filter_Channel_Outbound").isNotNull())),
                           f.col("nest.Decision_Services_Waterfall"))
                    .otherwise(f.col("Decision_Services_Waterfall")))\
        .withColumn("Decision_Services_Matrix",
                    f.when((f.col("APP_Customer_State").isin(["", "IMM MOB 00-05"]) &
                           (f.col("Filter_Channel_Outbound").isNotNull())),
                           f.col("nest.Decision_Services_Matrix"))
                     .otherwise(f.col("Decision_Services_Matrix")))\
        .drop(*["nest"])

    sdf_2c = sdf_2b\
        .withColumn("Decision_Services_Risk_Grade",
                    f.when((f.col("APP_Customer_State").isin(["", "IMM MOB 00-05"]) &
                           (f.col("Filter_Channel_Outbound").isNotNull()) &
                           (f.col("Decision_Services_Matrix") == "580x555")), f.lit("9"))
                     .when((f.col("APP_Customer_State").isin(["", "IMM MOB 00-05"]) &
                           (f.col("Filter_Channel_Outbound").isNotNull()) &
                           (f.col("Decision_Services_Matrix") == "580x585")), f.lit("8"))
                     .when((f.col("APP_Customer_State").isin(["", "IMM MOB 00-05"]) &
                           (f.col("Filter_Channel_Outbound").isNotNull()) &
                           (f.col("Decision_Services_Matrix") == "640x555")), f.lit("8"))
                     .when((f.col("APP_Customer_State").isin(["", "IMM MOB 00-05"]) &
                           (f.col("Filter_Channel_Outbound").isNotNull()) &
                           (f.col("Decision_Services_Matrix") == "640x585")), f.lit("8"))
                     .otherwise(f.col("Decision_Services_Risk_Grade")))
    # TSI
    sdf_3a = sdf_2c \
        .withColumn("nest",
                    udf_dsw_TSI(f.col("APP_Customer_State"), f.col("Filter_Channel_Inbound"),
                                f.col("Decision_Services_Segment"), f.col("Filter_Telesales_Inbound"),
                                f.col("Decision_Services_Waterfall"), f.col("CST_Deceased"), f.col("CST_CustomerAge"),
                                f.col("CST_Fraud"), f.col("CST_Sequestration"), f.col("CST_Dispute"),
                                f.col("CST_Emigrated"), f.col("ALL_Notices5Years"), f.col("CST_DebtReviewGranted"),
                                f.col("CST_DebtReviewRequested"), f.col("APP_Gross_Income"), f.col("CBX_Prism_TM"),
                                f.col("NEW_Customer_Score"), f.col("ALL_Perc0Delq90Days"),
                                f.col("ALL_NumPayments90Days")))\
        .withColumn("DECISION_SERVICES_SEGMENT", f.col("nest.DECISION_SERVICES_SEGMENT"))\
        .withColumn("FILTER_TELESALES_INBOUND", f.col("nest.FILTER_TELESALES_INBOUND"))\
        .withColumn("DECISION_SERVICES_WATERFALL", f.col("nest.DECISION_SERVICES_WATERFALL"))\
        .drop(*["nest"])

    sdf_3b = sdf_3a \
        .withColumn("nest",
                    f.when((f.col("APP_Customer_State").isin(["", "IMM MOB 00-05"]) &
                           (f.col("Filter_Channel_Inbound").isNotNull())),
                           udf_Risk_Grade_Matrix(f.lit("RG8"), f.lit(580), f.lit(600), f.lit(999), f.lit(999),
                                                 f.lit(999), f.lit(999), f.col("NEW_Customer_Score"), f.lit(555),
                                                 f.lit(585), f.lit(999), f.lit(999), f.lit(999), f.lit(999),
                                                 f.col("Decision_Services_Waterfall"), f.col("CBX_Prism_TM")))) \
        .withColumn("Decision_Services_Outcome",
                    f.when((f.col("APP_Customer_State").isin(["", "IMM MOB 00-05"]) &
                           (f.col("Filter_Channel_Inbound").isNotNull())),
                           f.col("nest.Decision_Services_Outcome"))
                    .otherwise(f.col("Decision_Services_Outcome")))\
        .withColumn("Decision_Services_Waterfall",
                    f.when((f.col("APP_Customer_State").isin(["", "IMM MOB 00-05"]) &
                           (f.col("Filter_Channel_Inbound").isNotNull())),
                           f.col("nest.Decision_Services_Waterfall"))
                    .otherwise(f.col("Decision_Services_Waterfall")))\
        .withColumn("Decision_Services_Matrix",
                    f.when((f.col("APP_Customer_State").isin(["", "IMM MOB 00-05"]) &
                           (f.col("Filter_Channel_Inbound").isNotNull())),
                           f.col("nest.Decision_Services_Matrix"))
                    .otherwise(f.col("Decision_Services_Matrix"))) \
        .drop(*["nest"])

    sdf_3c = sdf_3b\
        .withColumn("Decision_Services_Risk_Grade",
                    f.when((f.col("APP_Customer_State").isin(["", "IMM MOB 00-05"]) &
                           (f.col("Filter_Channel_Inbound").isNotNull()) &
                           (f.col("Decision_Services_Matrix") == "580x555")), f.lit("9"))
                     .when((f.col("APP_Customer_State").isin(["", "IMM MOB 00-05"]) &
                           (f.col("Filter_Channel_Inbound").isNotNull()) &
                           (f.col("Decision_Services_Matrix") == "580x585")), f.lit("8"))
                     .when((f.col("APP_Customer_State").isin(["", "IMM MOB 00-05"]) &
                           (f.col("Filter_Channel_Inbound").isNotNull()) &
                           (f.col("Decision_Services_Matrix") == "600x555")), f.lit("8"))
                     .when((f.col("APP_Customer_State").isin(["", "IMM MOB 00-05"]) &
                           (f.col("Filter_Channel_Inbound").isNotNull()) &
                           (f.col("Decision_Services_Matrix") == "600x585")), f.lit("8"))
                     .otherwise(f.col("Decision_Services_Risk_Grade")))
    # WWW
    sdf_4a = sdf_3c \
        .withColumn("nest",
                    udf_dsw_WWW(f.col("APP_Customer_State"), f.col("Filter_Channel_Online"),
                                f.col("Decision_Services_Segment"), f.col("Filter_Web_Service"),
                                f.col("Decision_Services_Waterfall"), f.col("CST_Deceased"), f.col("CST_CustomerAge"),
                                f.col("CST_Fraud"), f.col("CST_Sequestration"), f.col("CST_Dispute"),
                                f.col("CST_Emigrated"), f.col("ALL_Notices5Years"), f.col("CST_DebtReviewGranted"),
                                f.col("CST_DebtReviewRequested"), f.col("APP_Gross_Income"), f.col("CBX_Prism_TM"),
                                f.col("NEW_Customer_Score"), f.col("ALL_Perc0Delq90Days"),
                                f.col("ALL_NumPayments90Days")))\
        .withColumn("DECISION_SERVICES_SEGMENT", f.col("nest.DECISION_SERVICES_SEGMENT"))\
        .withColumn("FILTER_WEB_SERVICE", f.col("nest.FILTER_WEB_SERVICE"))\
        .withColumn("DECISION_SERVICES_WATERFALL", f.col("nest.DECISION_SERVICES_WATERFALL"))\
        .drop(*["nest"])

    sdf_4b = sdf_4a\
        .withColumn("nest",
                    f.when((f.col("APP_Customer_State").isin(["", "IMM MOB 00-05"]) &
                           (f.col("Filter_Channel_Online").isNotNull())),
                           udf_Risk_Grade_Matrix(f.lit("RG5"), f.lit(580), f.lit(999), f.lit(999), f.lit(999),
                                                 f.lit(999), f.lit(999), f.col("NEW_Customer_Score"), f.lit(555),
                                                 f.lit(999), f.lit(999), f.lit(999), f.lit(999), f.lit(999),
                                                 f.col("Decision_Services_Waterfall"), f.col("CBX_Prism_TM"))))\
        .withColumn("Decision_Services_Outcome",
                    f.when((f.col("APP_Customer_State").isin(["", "IMM MOB 00-05"]) &
                           (f.col("Filter_Channel_Online").isNotNull())),
                           f.col("nest.Decision_Services_Outcome"))
                     .otherwise(f.col("Decision_Services_Outcome")))\
        .withColumn("Decision_Services_Waterfall",
                    f.when((f.col("APP_Customer_State").isin(["", "IMM MOB 00-05"]) &
                           (f.col("Filter_Channel_Online").isNotNull())),
                           f.col("nest.Decision_Services_Waterfall"))
                     .otherwise(f.col("Decision_Services_Waterfall")))\
        .withColumn("Decision_Services_Matrix",
                    f.when((f.col("APP_Customer_State").isin(["", "IMM MOB 00-05"]) &
                           (f.col("Filter_Channel_Online").isNotNull())),
                           f.col("nest.Decision_Services_Matrix"))
                     .otherwise(f.col("Decision_Services_Matrix")))\
        .drop(*["nest"])
    # FAA
    sdf_5a = sdf_4b \
        .withColumn("nest",
                    udf_dsw_FAA(f.col("APP_Customer_State"),
                                f.col("Decision_Services_Segment"), f.col("Filter_First_Account_Applicant"),
                                f.col("Decision_Services_Waterfall"), f.col("CST_Deceased"), f.col("CST_CustomerAge"),
                                f.col("CST_Fraud"), f.col("CST_Sequestration"), f.col("CST_Dispute"),
                                f.col("CST_Emigrated"), f.col("ALL_Notices5Years"), f.col("CST_DebtReviewGranted"),
                                f.col("CST_DebtReviewRequested"), f.col("APP_Gross_Income"), f.col("CBX_Prism_TM"),
                                f.col("NEW_Customer_Score"), f.col("ALL_Perc0Delq90Days"),
                                f.col("ALL_NumPayments90Days"))) \
        .withColumn("DECISION_SERVICES_SEGMENT", f.col("nest.DECISION_SERVICES_SEGMENT")) \
        .withColumn("FILTER_FIRST_ACCOUNT_APPLICANT", f.col("nest.FILTER_FIRST_ACCOUNT_APPLICANT")) \
        .withColumn("DECISION_SERVICES_WATERFALL", f.col("nest.DECISION_SERVICES_WATERFALL")) \
        .drop(*["nest"])

    sdf_5b = sdf_5a\
        .withColumn("nest",
                    f.when((f.col("APP_Customer_State") == ""),
                           udf_Risk_Grade_Matrix(f.lit("RG5"), f.lit(580), f.lit(640), f.lit(999), f.lit(999),
                                                 f.lit(999), f.lit(999), f.col("NEW_Customer_Score"), f.lit(555),
                                                 f.lit(565), f.lit(585), f.lit(590), f.lit(615), f.lit(999),
                                                 f.col("Decision_Services_Waterfall"), f.col("CBX_Prism_TM"))))\
        .withColumn("Decision_Services_Outcome",
                    f.when((f.col("APP_Customer_State") == ""),
                           f.col("nest.Decision_Services_Outcome"))
                     .otherwise(f.col("Decision_Services_Outcome")))\
        .withColumn("Decision_Services_Waterfall",
                    f.when((f.col("APP_Customer_State") == ""),
                           f.col("nest.Decision_Services_Waterfall"))
                     .otherwise(f.col("Decision_Services_Waterfall")))\
        .withColumn("Decision_Services_Matrix",
                    f.when((f.col("APP_Customer_State") == ""),
                           f.col("nest.Decision_Services_Matrix"))
                     .otherwise(f.col("Decision_Services_Matrix")))\
        .drop(*["nest"])

    sdf_5c = sdf_5b\
        .withColumn("Decision_Services_Risk_Grade",
                    f.when((f.col("APP_Customer_State") == "") &
                           (f.col("Decision_Services_Matrix") == "580x555"), f.lit("9"))
                     .when((f.col("APP_Customer_State") == "") &
                           (f.col("Decision_Services_Matrix") == "580x565"), f.lit("8"))
                     .when((f.col("APP_Customer_State") == "") &
                           (f.col("Decision_Services_Matrix") == "580x585"), f.lit("8"))
                     .when((f.col("APP_Customer_State") == "") &
                           (f.col("Decision_Services_Matrix") == "580x590"), f.lit("5"))
                     .when((f.col("APP_Customer_State") == "") &
                           (f.col("Decision_Services_Matrix") == "580x615"), f.lit("4"))
                     .when((f.col("APP_Customer_State") == "") &
                           (f.col("Decision_Services_Matrix") == "640x5555"), f.lit("8"))
                     .when((f.col("APP_Customer_State") == "") &
                           (f.col("Decision_Services_Matrix") == "640x565"), f.lit("7"))
                     .when((f.col("APP_Customer_State") == "") &
                           (f.col("Decision_Services_Matrix") == "640x585"), f.lit("5"))
                     .when((f.col("APP_Customer_State") == "") &
                           (f.col("Decision_Services_Matrix") == "640x590"), f.lit("4"))
                     .when((f.col("APP_Customer_State") == "") &
                           (f.col("Decision_Services_Matrix") == "640x615"), f.lit("3"))
                     .otherwise(f.col("Decision_Services_Risk_Grade")))
    # IMM
    sdf_6a = sdf_5c\
        .withColumn("nest",
                    udf_dsw_IMM(f.col("APP_Customer_State"),
                                f.col("Decision_Services_Segment"), f.col("Filter_Immature_State"),
                                f.col("Decision_Services_Waterfall"), f.col("CST_Deceased"), f.col("CST_CustomerAge"),
                                f.col("CST_Fraud"), f.col("CST_Sequestration"), f.col("CST_Dispute"),
                                f.col("CST_Emigrated"), f.col("ALL_Notices5Years"), f.col("CST_DebtReviewGranted"),
                                f.col("CST_DebtReviewRequested"), f.col("APP_Gross_Income"), f.col("CBX_Prism_TM"),
                                f.col("NEW_Customer_Score"), f.col("ALL_Perc0Delq90Days"),
                                f.col("ALL_NumPayments90Days")))\
        .withColumn("DECISION_SERVICES_SEGMENT", f.col("nest.DECISION_SERVICES_SEGMENT"))\
        .withColumn("FILTER_IMMATURE_STATE", f.col("nest.FILTER_IMMATURE_STATE"))\
        .withColumn("DECISION_SERVICES_WATERFALL", f.col("nest.DECISION_SERVICES_WATERFALL")) \
        .drop(*["nest"])

    sdf_6b = sdf_6a\
        .withColumn("nest",
                    f.when((f.col("APP_Customer_State") == ""),
                           udf_Risk_Grade_Matrix(f.lit("RG5"), f.lit(580), f.lit(635), f.lit(999), f.lit(999),
                                                 f.lit(999), f.lit(999), f.col("NEW_Customer_Score"), f.lit(555),
                                                 f.lit(590), f.lit(605), f.lit(999), f.lit(999), f.lit(999),
                                                 f.col("Decision_Services_Waterfall"), f.col("CBX_Prism_TM"))))\
        .withColumn("Decision_Services_Outcome",
                    f.when((f.col("APP_Customer_State") == "IMM MOB 00-05"),
                           f.col("nest.Decision_Services_Outcome"))
                     .otherwise(f.col("Decision_Services_Outcome")))\
        .withColumn("Decision_Services_Waterfall",
                    f.when((f.col("APP_Customer_State") == "IMM MOB 00-05"),
                           f.col("nest.Decision_Services_Waterfall"))
                     .otherwise(f.col("Decision_Services_Waterfall")))\
        .withColumn("Decision_Services_Matrix",
                    f.when((f.col("APP_Customer_State") == "IMM MOB 00-05"),
                           f.col("nest.Decision_Services_Matrix"))
                     .otherwise(f.col("Decision_Services_Matrix")))\
        .drop(*["nest"])

    sdf_6c = sdf_6b \
        .withColumn("Decision_Services_Risk_Grade",
                    f.when((f.col("APP_Customer_State") == "IMM MOB 00-05") &
                           (f.col("Decision_Services_Matrix") == "580x555"), f.lit("8"))
                    .when((f.col("APP_Customer_State") == "IMM MOB 00-05") &
                          (f.col("Decision_Services_Matrix") == "580x590"), f.lit("5"))
                    .when((f.col("APP_Customer_State") == "IMM MOB 00-05") &
                          (f.col("Decision_Services_Matrix") == "580x605"), f.lit("5"))
                    .when((f.col("APP_Customer_State") == "IMM MOB 00-05") &
                          (f.col("Decision_Services_Matrix") == "635x555"), f.lit("6"))
                    .when((f.col("APP_Customer_State") == "IMM MOB 00-05") &
                          (f.col("Decision_Services_Matrix") == "635x590"), f.lit("5"))
                    .when((f.col("APP_Customer_State") == "IMM MOB 00-05") &
                          (f.col("Decision_Services_Matrix") == "635x605"), f.lit("4"))
                    .otherwise(f.col("Decision_Services_Risk_Grade")))
    # PUP
    sdf_7a = sdf_6c \
        .withColumn("nest",
                    udf_dsw_PUP(f.col("APP_Customer_State"),
                                f.col("Decision_Services_Segment"), f.col("Filter_XXXXXX_State"),
                                f.col("Decision_Services_Waterfall"), f.col("CST_Deceased"), f.col("CST_CustomerAge"),
                                f.col("CST_Fraud"), f.col("CST_Sequestration"), f.col("CST_Dispute"),
                                f.col("CST_Emigrated"), f.col("ALL_Notices5Years"), f.col("CST_DebtReviewGranted"),
                                f.col("CST_DebtReviewRequested"), f.col("CBX_Prism_TM"),
                                f.col("EST_Customer_Score"), f.col("ALL_Perc0Delq90Days"),
                                f.col("ALL_NumPayments90Days")))\
        .withColumn("DECISION_SERVICES_SEGMENT", f.col("nest.DECISION_SERVICES_SEGMENT"))\
        .withColumn("FILTER_XXXXXX_STATE", f.col("nest.FILTER_XXXXXX_STATE"))\
        .withColumn("DECISION_SERVICES_WATERFALL", f.col("nest.DECISION_SERVICES_WATERFALL")) \
        .drop(*["nest"])

    sdf_7b = sdf_7a\
        .withColumn("nest",
                    f.when((f.col("APP_Customer_State") == "PUP Paid-Up"),
                           udf_Risk_Grade_Matrix(f.lit("RG6"), f.lit(570), f.lit(999), f.lit(999), f.lit(999),
                                                 f.lit(999), f.lit(999), f.col("EST_Customer_Score"), f.lit(540),
                                                 f.lit(615), f.lit(630), f.lit(999), f.lit(999), f.lit(999),
                                                 f.col("Decision_Services_Waterfall"), f.col("CBX_Prism_TM"))))\
        .withColumn("Decision_Services_Outcome",
                    f.when((f.col("APP_Customer_State") == "PUP Paid-Up"),
                           f.col("nest.Decision_Services_Outcome"))
                     .otherwise(f.col("Decision_Services_Outcome")))\
        .withColumn("Decision_Services_Waterfall",
                    f.when((f.col("APP_Customer_State") == "PUP Paid-Up"),
                           f.col("nest.Decision_Services_Waterfall"))
                     .otherwise(f.col("Decision_Services_Waterfall")))\
        .withColumn("Decision_Services_Matrix",
                    f.when((f.col("APP_Customer_State") == "PUP Paid-Up"),
                           f.col("nest.Decision_Services_Matrix"))
                     .otherwise(f.col("Decision_Services_Matrix")))\
        .drop(*["nest"])

    sdf_7c = sdf_7b\
        .withColumn("Decision_Services_Risk_Grade",
                    f.when((f.col("APP_Customer_State") == "PUP Paid-Up") &
                           (f.col("Decision_Services_Matrix") == "570x540"), f.lit("7"))
                     .when((f.col("APP_Customer_State") == "PUP Paid-Up") &
                          (f.col("Decision_Services_Matrix") == "570x615"), f.lit("6"))
                     .when((f.col("APP_Customer_State") == "PUP Paid-Up") &
                          (f.col("Decision_Services_Matrix") == "570x630"), f.lit("5"))
                     .otherwise(f.col("Decision_Services_Risk_Grade")))
    # CRD
    sdf_8a = sdf_7c \
        .withColumn("nest",
                    udf_dsw_PUP(f.col("APP_Customer_State"),
                                f.col("Decision_Services_Segment"), f.col("Filter_XXXXXX_State"),
                                f.col("Decision_Services_Waterfall"), f.col("CST_Deceased"), f.col("CST_CustomerAge"),
                                f.col("CST_Fraud"), f.col("CST_Sequestration"), f.col("CST_Dispute"),
                                f.col("CST_Emigrated"), f.col("ALL_Notices5Years"), f.col("CST_DebtReviewGranted"),
                                f.col("CST_DebtReviewRequested"), f.col("CBX_Prism_TM"),
                                f.col("EST_Customer_Score"), f.col("ALL_Perc0Delq90Days"),
                                f.col("ALL_NumPayments90Days"))) \
        .withColumn("DECISION_SERVICES_SEGMENT", f.col("nest.DECISION_SERVICES_SEGMENT")) \
        .withColumn("FILTER_XXXXXX_STATE", f.col("nest.FILTER_XXXXXX_STATE")) \
        .withColumn("DECISION_SERVICES_WATERFALL", f.col("nest.DECISION_SERVICES_WATERFALL")) \
        .drop(*["nest"])

    sdf_8b = sdf_8a\
        .withColumn("nest",
                    f.when((f.col("APP_Customer_State") == "CRD Credit Balance"),
                           udf_Risk_Grade_Matrix(f.lit("RG3"), f.lit(570), f.lit(625), f.lit(999), f.lit(999),
                                                 f.lit(999), f.lit(999), f.col("EST_Customer_Score"), f.lit(540),
                                                 f.lit(600), f.lit(635), f.lit(999), f.lit(999), f.lit(999),
                                                 f.col("Decision_Services_Waterfall"), f.col("CBX_Prism_TM"))))\
        .withColumn("Decision_Services_Outcome",
                    f.when((f.col("APP_Customer_State") == "CRD Credit Balance"),
                           f.col("nest.Decision_Services_Outcome"))
                     .otherwise(f.col("Decision_Services_Outcome")))\
        .withColumn("Decision_Services_Waterfall",
                    f.when((f.col("APP_Customer_State") == "CRD Credit Balance"),
                           f.col("nest.Decision_Services_Waterfall"))
                     .otherwise(f.col("Decision_Services_Waterfall")))\
        .withColumn("Decision_Services_Matrix",
                    f.when((f.col("APP_Customer_State") == "CRD Credit Balance"),
                           f.col("nest.Decision_Services_Matrix"))
                     .otherwise(f.col("Decision_Services_Matrix")))\
        .drop(*["nest"])

    sdf_8c = sdf_8b\
        .withColumn("Decision_Services_Risk_Grade",
                    f.when((f.col("APP_Customer_State") == "CRD Credit Balance") &
                           (f.col("Decision_Services_Matrix") == "570x540"), f.lit("4"))
                     .when((f.col("APP_Customer_State") == "CRD Credit Balance") &
                           (f.col("Decision_Services_Matrix") == "570x600"), f.lit("4"))
                     .when((f.col("APP_Customer_State") == "CRD Credit Balance") &
                           (f.col("Decision_Services_Matrix") == "570x635"), f.lit("3"))
                     .when((f.col("APP_Customer_State") == "CRD Credit Balance") &
                           (f.col("Decision_Services_Matrix") == "625x540"), f.lit("4"))
                     .when((f.col("APP_Customer_State") == "CRD Credit Balance") &
                           (f.col("Decision_Services_Matrix") == "625x600"), f.lit("3"))
                     .when((f.col("APP_Customer_State") == "CRD Credit Balance") &
                           (f.col("Decision_Services_Matrix") == "625x635"), f.lit("2"))
                     .otherwise(f.col("Decision_Services_Risk_Grade")))
    # CLR
    sdf_9a = sdf_8c \
        .withColumn("nest",
                    udf_dsw_PUP(f.col("APP_Customer_State"),
                                f.col("Decision_Services_Segment"), f.col("Filter_Clear_State"),
                                f.col("Decision_Services_Waterfall"), f.col("CST_Deceased"), f.col("CST_CustomerAge"),
                                f.col("CST_Fraud"), f.col("CST_Sequestration"), f.col("CST_Dispute"),
                                f.col("CST_Emigrated"), f.col("ALL_Notices5Years"), f.col("CST_DebtReviewGranted"),
                                f.col("EST_Customer_Score"))) \
        .withColumn("DECISION_SERVICES_SEGMENT", f.col("nest.DECISION_SERVICES_SEGMENT")) \
        .withColumn("FILTER_CLEAR_STATE", f.col("nest.FILTER_CLEAR_STATE")) \
        .withColumn("DECISION_SERVICES_WATERFALL", f.col("nest.DECISION_SERVICES_WATERFALL")) \
        .drop(*["nest"])

    sdf_9b = sdf_9a\
        .withColumn("nest",
                    f.when((f.col("APP_Customer_State") == "CLR Clear"),
                           udf_Risk_Grade_Matrix(f.lit("RG2"), f.lit(100), f.lit(640), f.lit(680), f.lit(999),
                                                 f.lit(999), f.lit(999), f.col("EST_Customer_Score"), f.lit(540),
                                                 f.lit(585), f.lit(610), f.lit(635), f.lit(660), f.lit(999),
                                                 f.col("Decision_Services_Waterfall"), f.col("CBX_Prism_TM"))))\
        .withColumn("Decision_Services_Outcome",
                    f.when((f.col("APP_Customer_State") == "CLR Clear"),
                           f.col("nest.Decision_Services_Outcome"))
                     .otherwise(f.col("Decision_Services_Outcome")))\
        .withColumn("Decision_Services_Waterfall",
                    f.when((f.col("APP_Customer_State") == "CLR Clear"),
                           f.col("nest.Decision_Services_Waterfall"))
                     .otherwise(f.col("Decision_Services_Waterfall")))\
        .withColumn("Decision_Services_Matrix",
                    f.when((f.col("APP_Customer_State") == "CLR Clear"),
                           f.col("nest.Decision_Services_Matrix"))
                     .otherwise(f.col("Decision_Services_Matrix")))\
        .drop(*["nest"])

    sdf_9c = sdf_9b \
        .withColumn("Decision_Services_Risk_Grade",
                    f.when((f.col("APP_Customer_State") == "CLR Clear") &
                           (f.col("Decision_Services_Matrix") == "100x540"), f.lit("5"))
                    .when((f.col("APP_Customer_State") == "CLR Clear") &
                          (f.col("Decision_Services_Matrix") == "100x585"), f.lit("4"))
                    .when((f.col("APP_Customer_State") == "CLR Clear") &
                          (f.col("Decision_Services_Matrix") == "100x610"), f.lit("3"))
                    .when((f.col("APP_Customer_State") == "CLR Clear") &
                          (f.col("Decision_Services_Matrix") == "100x635"), f.lit("3"))
                    .when((f.col("APP_Customer_State") == "CLR Clear") &
                          (f.col("Decision_Services_Matrix") == "100x660"), f.lit("2"))
                    .when((f.col("APP_Customer_State") == "CLR Clear") &
                          (f.col("Decision_Services_Matrix") == "640x540"), f.lit("4"))
                    .when((f.col("APP_Customer_State") == "CLR Clear") &
                          (f.col("Decision_Services_Matrix") == "640x585"), f.lit("3"))
                    .when((f.col("APP_Customer_State") == "CLR Clear") &
                          (f.col("Decision_Services_Matrix") == "640x610"), f.lit("3"))
                    .when((f.col("APP_Customer_State") == "CLR Clear") &
                          (f.col("Decision_Services_Matrix") == "640x635"), f.lit("2"))
                    .when((f.col("APP_Customer_State") == "CLR Clear") &
                          (f.col("Decision_Services_Matrix") == "640x660"), f.lit("1"))
                    .when((f.col("APP_Customer_State") == "CLR Clear") &
                          (f.col("Decision_Services_Matrix") == "680x540"), f.lit("3"))
                    .when((f.col("APP_Customer_State") == "CLR Clear") &
                          (f.col("Decision_Services_Matrix") == "680x585"), f.lit("2"))
                    .when((f.col("APP_Customer_State") == "CLR Clear") &
                          (f.col("Decision_Services_Matrix") == "680x610"), f.lit("1"))
                    .when((f.col("APP_Customer_State") == "CLR Clear") &
                          (f.col("Decision_Services_Matrix") == "680x635"), f.lit("1"))
                    .when((f.col("APP_Customer_State") == "CLR Clear") &
                          (f.col("Decision_Services_Matrix") == "680x660"), f.lit("1"))
                    .otherwise(f.col("Decision_Services_Risk_Grade")))
    # RES
    sdf_10a = sdf_9c \
        .withColumn("nest",
                    udf_dsw_RES(f.col("APP_Customer_State"),
                                f.col("Decision_Services_Segment"), f.col("Filter_Responsible_State"),
                                f.col("Decision_Services_Waterfall"), f.col("CST_Deceased"), f.col("CST_CustomerAge"),
                                f.col("CST_Fraud"), f.col("CST_Sequestration"), f.col("CST_Dispute"),
                                f.col("CST_Emigrated"), f.col("ALL_Notices5Years"), f.col("CST_DebtReviewGranted"),
                                f.col("EST_Customer_Score"))) \
        .withColumn("DECISION_SERVICES_SEGMENT", f.col("nest.DECISION_SERVICES_SEGMENT"))\
        .withColumn("FILTER_RESPONSIBLE_STATE", f.col("nest.FILTER_RESPONSIBLE_STATE"))\
        .withColumn("DECISION_SERVICES_WATERFALL", f.col("nest.DECISION_SERVICES_WATERFALL"))\
        .drop(*["nest"])

    sdf_10b = sdf_10a \
        .withColumn("nest",
                    f.when((f.col("APP_Customer_State") == "RES Responsible"),
                           udf_Risk_Grade_Matrix(f.lit("RG5"), f.lit(100), f.lit(645), f.lit(670), f.lit(999),
                                                 f.lit(999), f.lit(999), f.col("EST_Customer_Score"), f.lit(540),
                                                 f.lit(585), f.lit(630), f.lit(655), f.lit(999), f.lit(999),
                                                 f.col("Decision_Services_Waterfall"), f.col("CBX_Prism_TM")))) \
        .withColumn("Decision_Services_Outcome",
                    f.when((f.col("APP_Customer_State") == "RES Responsible"),
                           f.col("nest.Decision_Services_Outcome"))
                    .otherwise(f.col("Decision_Services_Outcome"))) \
        .withColumn("Decision_Services_Waterfall",
                    f.when((f.col("APP_Customer_State") == "RES Responsible"),
                           f.col("nest.Decision_Services_Waterfall"))
                    .otherwise(f.col("Decision_Services_Waterfall"))) \
        .withColumn("Decision_Services_Matrix",
                    f.when((f.col("APP_Customer_State") == "RES Responsible"),
                           f.col("nest.Decision_Services_Matrix"))
                    .otherwise(f.col("Decision_Services_Matrix"))) \
        .drop(*["nest"])

    sdf_10c = sdf_10b \
        .withColumn("Decision_Services_Risk_Grade",
                    f.when((f.col("APP_Customer_State") == "RES Responsible") &
                           (f.col("Decision_Services_Matrix") == "100x540"), f.lit("7"))
                    .when((f.col("APP_Customer_State") == "RES Responsible") &
                          (f.col("Decision_Services_Matrix") == "100x585"), f.lit("5"))
                    .when((f.col("APP_Customer_State") == "RES Responsible") &
                          (f.col("Decision_Services_Matrix") == "100x630"), f.lit("4"))
                    .when((f.col("APP_Customer_State") == "RES Responsible") &
                          (f.col("Decision_Services_Matrix") == "100x655"), f.lit("4"))
                    .when((f.col("APP_Customer_State") == "RES Responsible") &
                          (f.col("Decision_Services_Matrix") == "645x540"), f.lit("5"))
                    .when((f.col("APP_Customer_State") == "RES Responsible") &
                          (f.col("Decision_Services_Matrix") == "645x585"), f.lit("5"))
                    .when((f.col("APP_Customer_State") == "RES Responsible") &
                          (f.col("Decision_Services_Matrix") == "645x630"), f.lit("4"))
                    .when((f.col("APP_Customer_State") == "RES Responsible") &
                          (f.col("Decision_Services_Matrix") == "645x655"), f.lit("3"))
                    .when((f.col("APP_Customer_State") == "RES Responsible") &
                          (f.col("Decision_Services_Matrix") == "670x540"), f.lit("4"))
                    .when((f.col("APP_Customer_State") == "RES Responsible") &
                          (f.col("Decision_Services_Matrix") == "670x585"), f.lit("4"))
                    .when((f.col("APP_Customer_State") == "RES Responsible") &
                          (f.col("Decision_Services_Matrix") == "670x630"), f.lit("3"))
                    .when((f.col("APP_Customer_State") == "RES Responsible") &
                          (f.col("Decision_Services_Matrix") == "670x655"), f.lit("3"))
                    .otherwise(f.col("Decision_Services_Risk_Grade")))
    #  ERR
    sdf_11a = sdf_10c \
        .withColumn("nest",
                    udf_dsw_RES(f.col("APP_Customer_State"),
                                f.col("Decision_Services_Segment"), f.col("Filter_Erratic_State"),
                                f.col("Decision_Services_Waterfall"), f.col("CST_Deceased"), f.col("CST_CustomerAge"),
                                f.col("CST_Fraud"), f.col("CST_Sequestration"), f.col("CST_Dispute"),
                                f.col("CST_Emigrated"), f.col("ALL_Notices5Years"), f.col("CST_DebtReviewGranted"),
                                f.col("CBX_Prism_TM"), f.col("EST_Customer_Score")))\
        .withColumn("DECISION_SERVICES_SEGMENT", f.col("nest.DECISION_SERVICES_SEGMENT"))\
        .withColumn("FILTER_ERRATIC_STATE", f.col("nest.FILTER_ERRATIC_STATE"))\
        .withColumn("DECISION_SERVICES_WATERFALL", f.col("nest.DECISION_SERVICES_WATERFALL"))\
        .drop(*["nest"])

    sdf_11b = sdf_11a \
        .withColumn("nest",
                    f.when((f.col("APP_Customer_State") == "ERR Erratic"),
                           udf_Risk_Grade_Matrix(f.lit("RG7"), f.lit(580), f.lit(615), f.lit(640), f.lit(999),
                                                 f.lit(999), f.lit(999), f.col("EST_Customer_Score"), f.lit(555),
                                                 f.lit(570), f.lit(590), f.lit(615), f.lit(625), f.lit(999),
                                                 f.col("Decision_Services_Waterfall"), f.col("CBX_Prism_TM")))) \
        .withColumn("Decision_Services_Outcome",
                    f.when((f.col("APP_Customer_State") == "ERR Erratic"),
                           f.col("nest.Decision_Services_Outcome"))
                    .otherwise(f.col("Decision_Services_Outcome"))) \
        .withColumn("Decision_Services_Waterfall",
                    f.when((f.col("APP_Customer_State") == "ERR Erratic"),
                           f.col("nest.Decision_Services_Waterfall"))
                    .otherwise(f.col("Decision_Services_Waterfall"))) \
        .withColumn("Decision_Services_Matrix",
                    f.when((f.col("APP_Customer_State") == "ERR Erratic"),
                           f.col("nest.Decision_Services_Matrix"))
                    .otherwise(f.col("Decision_Services_Matrix"))) \
        .drop(*["nest"])

    sdf_11c = sdf_11b \
        .withColumn("Decision_Services_Risk_Grade",
                    f.when((f.col("APP_Customer_State") == "ERR Erratic") &
                           (f.col("Decision_Services_Matrix") == "580x555"), f.lit("9"))
                    .when((f.col("APP_Customer_State") == "ERR Erratic") &
                          (f.col("Decision_Services_Matrix") == "580x570"), f.lit("8"))
                    .when((f.col("APP_Customer_State") == "ERR Erratic") &
                          (f.col("Decision_Services_Matrix") == "580x590"), f.lit("8"))
                    .when((f.col("APP_Customer_State") == "ERR Erratic") &
                          (f.col("Decision_Services_Matrix") == "580x615"), f.lit("7"))
                    .when((f.col("APP_Customer_State") == "ERR Erratic") &
                          (f.col("Decision_Services_Matrix") == "580x625"), f.lit("6"))
                    .when((f.col("APP_Customer_State") == "ERR Erratic") &
                          (f.col("Decision_Services_Matrix") == "615x555"), f.lit("9"))
                    .when((f.col("APP_Customer_State") == "ERR Erratic") &
                          (f.col("Decision_Services_Matrix") == "615x570"), f.lit("8"))
                    .when((f.col("APP_Customer_State") == "ERR Erratic") &
                          (f.col("Decision_Services_Matrix") == "615x590"), f.lit("7"))
                    .when((f.col("APP_Customer_State") == "ERR Erratic") &
                          (f.col("Decision_Services_Matrix") == "615x615"), f.lit("6"))
                    .when((f.col("APP_Customer_State") == "ERR Erratic") &
                          (f.col("Decision_Services_Matrix") == "615x625"), f.lit("5"))
                    .when((f.col("APP_Customer_State") == "ERR Erratic") &
                          (f.col("Decision_Services_Matrix") == "640x555"), f.lit("8"))
                    .when((f.col("APP_Customer_State") == "ERR Erratic") &
                          (f.col("Decision_Services_Matrix") == "640x570"), f.lit("7"))
                    .when((f.col("APP_Customer_State") == "ERR Erratic") &
                          (f.col("Decision_Services_Matrix") == "640x590"), f.lit("6"))
                    .when((f.col("APP_Customer_State") == "ERR Erratic") &
                          (f.col("Decision_Services_Matrix") == "640x615"), f.lit("5"))
                    .when((f.col("APP_Customer_State") == "ERR Erratic") &
                          (f.col("Decision_Services_Matrix") == "640x625"), f.lit("4"))
                    .otherwise(f.col("Decision_Services_Risk_Grade")))
    # ARR
    sdf_12a = sdf_11c\
        .withColumn("nest",
                    udf_dsw_ARR(f.col("APP_Customer_State"),
                                f.col("Decision_Services_Segment"), f.col("Filter_Arrears_State"),
                                f.col("Decision_Services_Waterfall"), f.col("CST_Deceased"), f.col("CST_CustomerAge"),
                                f.col("CST_Fraud"), f.col("CST_Sequestration"), f.col("CST_Dispute"),
                                f.col("CST_Emigrated"), f.col("ALL_Notices5Years"), f.col("CST_DebtReviewGranted"),
                                f.col("CST_DebtReviewRequested"), f.col("CBX_Prism_TM"), f.col("EST_Customer_Score"),
                                f.col("ALL_Perc0Delq90Days")))\
        .withColumn("DECISION_SERVICES_SEGMENT", f.col("nest.DECISION_SERVICES_SEGMENT"))\
        .withColumn("FILTER_ARREARS_STATE", f.col("nest.FILTER_ARREARS_STATE"))\
        .withColumn("DECISION_SERVICES_WATERFALL", f.col("nest.DECISION_SERVICES_WATERFALL"))\
        .drop(*["nest"])

    sdf_12b = sdf_12a\
        .withColumn("nest",
                    f.when((f.col("APP_Customer_State").isin(["EXT Extended", "DIS Distressed", "DBT Doubtful Debt"])),
                           udf_Risk_Grade_Matrix(f.lit("RG8"), f.lit(615), f.lit(650), f.lit(999), f.lit(999),
                                                 f.lit(999), f.lit(999), f.col("EST_Customer_Score"), f.lit(590),
                                                 f.lit(605), f.lit(630), f.lit(655), f.lit(999), f.lit(999),
                                                 f.col("Decision_Services_Waterfall"), f.col("CBX_Prism_TM"))))\
        .withColumn("Decision_Services_Outcome",
                    f.when((f.col("APP_Customer_State").isin(["EXT Extended", "DIS Distressed", "DBT Doubtful Debt"])),
                           f.col("nest.Decision_Services_Outcome"))
                    .otherwise(f.col("Decision_Services_Outcome"))) \
        .withColumn("Decision_Services_Waterfall",
                    f.when((f.col("APP_Customer_State").isin(["EXT Extended", "DIS Distressed", "DBT Doubtful Debt"])),
                           f.col("nest.Decision_Services_Waterfall"))
                    .otherwise(f.col("Decision_Services_Waterfall"))) \
        .withColumn("Decision_Services_Matrix",
                    f.when((f.col("APP_Customer_State").isin(["EXT Extended", "DIS Distressed", "DBT Doubtful Debt"])),
                           f.col("nest.Decision_Services_Matrix"))
                    .otherwise(f.col("Decision_Services_Matrix"))) \
        .drop(*["nest"])

    sdf_12c = sdf_12b \
        .withColumn("Decision_Services_Risk_Grade",
                    f.when((f.col("APP_Customer_State").isin(["EXT Extended", "DIS Distressed", "DBT Doubtful Debt"])) &
                           (f.col("Decision_Services_Matrix") == "615x590"), f.lit("9"))
                    .when((f.col("APP_Customer_State").isin(["EXT Extended", "DIS Distressed", "DBT Doubtful Debt"])) &
                          (f.col("Decision_Services_Matrix") == "615x605"), f.lit("8"))
                    .when((f.col("APP_Customer_State").isin(["EXT Extended", "DIS Distressed", "DBT Doubtful Debt"])) &
                          (f.col("Decision_Services_Matrix") == "615x630"), f.lit("8"))
                    .when((f.col("APP_Customer_State").isin(["EXT Extended", "DIS Distressed", "DBT Doubtful Debt"])) &
                          (f.col("Decision_Services_Matrix") == "615x655"), f.lit("7"))
                    .when((f.col("APP_Customer_State").isin(["EXT Extended", "DIS Distressed", "DBT Doubtful Debt"])) &
                          (f.col("Decision_Services_Matrix") == "650x590"), f.lit("8"))
                    .when((f.col("APP_Customer_State").isin(["EXT Extended", "DIS Distressed", "DBT Doubtful Debt"])) &
                          (f.col("Decision_Services_Matrix") == "650x605"), f.lit("8"))
                    .when((f.col("APP_Customer_State").isin(["EXT Extended", "DIS Distressed", "DBT Doubtful Debt"])) &
                          (f.col("Decision_Services_Matrix") == "650x630"), f.lit("7"))
                    .when((f.col("APP_Customer_State").isin(["EXT Extended", "DIS Distressed", "DBT Doubtful Debt"])) &
                          (f.col("Decision_Services_Matrix") == "650x655"), f.lit("6"))
                    .otherwise(f.col("Decision_Services_Risk_Grade")))
    # Catch-All
    sdf_13 = sdf_12c\
        .withColumn("Decision_Services_Segment",
                    f.when(f.col("Decision_Services_Segment").isNull(),
                           f.lit("XXX"))
                     .otherwise(f.col("Decision_Services_Segment")))\
        .withColumn("Decision_Services_Outcome",
                    f.when(f.col("Decision_Services_Segment") == "XXX",
                           f.lit("Decline"))
                     .otherwise(f.col("Decision_Services_Outcome")))\
        .withColumn("Filter_XXXXXX_State",
                    f.when(f.col("Decision_Services_Segment") == "XXX",
                           f.lit(1))
                     .otherwise(f.col("Filter_XXXXXX_State")))
    # Flag which filter was activated
    sdf_14 = sdf_13\
        .withColumn("nest",
                    udf_dsw_flag_filter(f.col("APP_Subscriptions"), f.col("Customer_State"),
                                        f.col("Filter_Clear_State"), f.col("Filter_Responsible_state"),
                                        f.col("Filter_Erratic_State"), f.col("Filter_Arrears_State")))\
        .withColumn("Filter_Immature_Activations", f.col("nest.Filter_Immature_Activations"))\
        .withColumn("Filter_Clear_Activations", f.col("nest.Filter_Clear_Activations"))\
        .withColumn("Filter_Responsible_Activations", f.col("nest.Filter_Responsible_Activations"))\
        .withColumn("Filter_Erratic_Activations", f.col("nest.Filter_Erratic_Activations"))\
        .withColumn("Filter_Arrears_Activations", f.col("nest.Filter_Arrears_Activations"))\
        .withColumn("Filter_Other_Activations", f.col("nest.Filter_Other_Activations"))\
        .drop(*["nest"])

    return sdf_14


def dsw_flag_filter(APP_Subscriptions, Customer_State, Filter_Clear_State, Filter_Responsible_State,
                    Filter_Erratic_State, Filter_Arrears_State):
    Filter_Immature_Activations = Filter_Clear_Activations = Filter_Responsible_Activations = None
    Filter_Erratic_Activations = Filter_Arrears_Activations = Filter_Other_Activations = None
    if Customer_State is None:
        Customer_State = ""
    if Filter_Clear_State is None:
        Filter_Clear_State = 0
    if Filter_Responsible_State is None:
        Filter_Responsible_State = 0
    if Filter_Erratic_State is None:
        Filter_Erratic_State = 0
    if Filter_Arrears_State is None:
        Filter_Arrears_State = 0

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
    return (Filter_Immature_Activations, Filter_Clear_Activations, Filter_Responsible_Activations,
            Filter_Erratic_Activations, Filter_Arrears_Activations, Filter_Other_Activations)


schema_dsw_flag_filter = t.StructType([
    t.StructField("Filter_Immature_Activations", t.IntegerType(), True),
    t.StructField("Filter_Clear_Activations", t.IntegerType(), True),
    t.StructField("Filter_Responsible_Activations", t.IntegerType(), True),
    t.StructField("Filter_Erratic_Activations", t.IntegerType(), True),
    t.StructField("Filter_Arrears_Activations", t.IntegerType(), True),
    t.StructField("Filter_Other_Activations", t.IntegerType(), True)
])

udf_dsw_flag_filter = f.udf(dsw_flag_filter,
                            returnType=schema_dsw_flag_filter)


def dsw_ARR(APP_Customer_State, Decision_Services_Segment, Filter_Arrears_State, Decision_Services_Waterfall,
            CST_Deceased, CST_CustomerAge, CST_Fraud, CST_Sequestration, CST_Dispute, CST_Emigrated, ALL_Notices5Years,
            CST_DebtReviewGranted, CST_DebtReviewRequested, CBX_Prism_TM, EST_Customer_Score, ALL_Perc0Delq90Days):
    if APP_Customer_State in ["EXT Extended", "DIS Distressed", "DBT Doubtful Debt"]:
        Decision_Services_Segment = "ARR"
        Filter_Arrears_State = 1
        if CST_Deceased == 'Y':
            Decision_Services_Waterfall = "P01 Consumer Deceased"
        elif CST_CustomerAge < 1800:
            Decision_Services_Waterfall = "P02 Consumer Age < 18"
        elif CST_Fraud == "Y":
            Decision_Services_Waterfall = "P03 Payment Profile Fraud"
        elif CST_Sequestration == "Y":
            Decision_Services_Waterfall = "P04 Consumer Sequestrated"
        elif CST_Dispute == 'Y':
            Decision_Services_Waterfall = "P05 Payment Profile Dispute"
        elif CST_Emigrated == "Y":
            Decision_Services_Waterfall = "P06 Consumer Emigrated"
        elif ALL_Notices5Years == "Y":
            Decision_Services_Waterfall = "P07 Public Notice Issued"
        elif CST_DebtReviewGranted == "Y":
            Decision_Services_Waterfall = "P08 Debt Review Granted"
        elif CST_DebtReviewRequested == "Y":
            Decision_Services_Waterfall = "G01 Debt Review Requested"
        elif CBX_Prism_TM < 615:
            Decision_Services_Waterfall = "C01 Prism TM < 615"
        elif EST_Customer_Score < 590:
            Decision_Services_Waterfall = "C01 Established Customer Score < 590"
        elif ALL_Perc0Delq90Days < 50:
            Decision_Services_Waterfall = "G02 UTD bureau trades last 90 days < 50%"
        else:
            pass
    return Decision_Services_Segment, Filter_Arrears_State, Decision_Services_Waterfall


schema_dsw_ARR = t.StructType([
    t.StructField("DECISION_SERVICES_SEGMENT", t.StringType(), True),
    t.StructField("FILTER_ARREARS_STATE", t.IntegerType(), True),
    t.StructField("DECISION_SERVICES_WATERFALL", t.StringType(), True)
])
udf_dsw_ARR = f.udf(dsw_ARR, returnType=schema_dsw_ARR)


def dsw_ERR(APP_Customer_State, Decision_Services_Segment, Filter_Erratic_State, Decision_Services_Waterfall,
            CST_Deceased, CST_CustomerAge, CST_Fraud, CST_Sequestration, CST_Dispute, CST_Emigrated, ALL_Notices5Years,
            CST_DebtReviewGranted, CBX_Prism_TM, EST_Customer_Score):
    if APP_Customer_State == "ERR Erratic":
        Decision_Services_Segment = "ERR"
        Filter_Erratic_State = 1
        if CST_Deceased == 'Y':
            Decision_Services_Waterfall = "P01 Consumer Deceased"
        elif CST_CustomerAge < 1800:
            Decision_Services_Waterfall = "P02 Consumer Age < 18"
        elif CST_Fraud == "Y":
            Decision_Services_Waterfall = "P03 Payment Profile Fraud"
        elif CST_Sequestration == "Y":
            Decision_Services_Waterfall = "P04 Consumer Sequestrated"
        elif CST_Dispute == 'Y':
            Decision_Services_Waterfall = "P05 Payment Profile Dispute"
        elif CST_Emigrated == "Y":
            Decision_Services_Waterfall = "P06 Consumer Emigrated"
        elif ALL_Notices5Years == "Y":
            Decision_Services_Waterfall = "P07 Public Notice Issued"
        elif CST_DebtReviewGranted == "Y":
            Decision_Services_Waterfall = "P08 Debt Review Granted"
        elif CBX_Prism_TM < 580:
            Decision_Services_Waterfall = "C01 Prism TM < 580"
        elif EST_Customer_Score < 555:
            Decision_Services_Waterfall = "C01 Established Customer Score < 555"
        else:
            pass
    return Decision_Services_Segment, Filter_Erratic_State, Decision_Services_Waterfall


schema_dsw_ERR = t.StructType([
    t.StructField("DECISION_SERVICES_SEGMENT", t.StringType(), True),
    t.StructField("FILTER_ERRATIC_STATE", t.IntegerType(), True),
    t.StructField("DECISION_SERVICES_WATERFALL", t.StringType(), True)
])
udf_dsw_ERR = f.udf(dsw_ERR, returnType=schema_dsw_ERR)


def dsw_RES(APP_Customer_State, Decision_Services_Segment, Filter_Responsible_State, Decision_Services_Waterfall,
            CST_Deceased, CST_CustomerAge, CST_Fraud, CST_Sequestration, CST_Dispute, CST_Emigrated, ALL_Notices5Years,
            CST_DebtReviewGranted, EST_Customer_Score):
    if APP_Customer_State == "RES Responsible":
        Decision_Services_Segment = "RES"
        Filter_Responsible_State = 1
        if CST_Deceased == 'Y':
            Decision_Services_Waterfall = "P01 Consumer Deceased"
        elif CST_CustomerAge < 1800:
            Decision_Services_Waterfall = "P02 Consumer Age < 18"
        elif CST_Fraud == "Y":
            Decision_Services_Waterfall = "P03 Payment Profile Fraud"
        elif CST_Sequestration == "Y":
            Decision_Services_Waterfall = "P04 Consumer Sequestrated"
        elif CST_Dispute == 'Y':
            Decision_Services_Waterfall = "P05 Payment Profile Dispute"
        elif CST_Emigrated == "Y":
            Decision_Services_Waterfall = "P06 Consumer Emigrated"
        elif ALL_Notices5Years == "Y":
            Decision_Services_Waterfall = "P07 Public Notice Issued"
        elif CST_DebtReviewGranted == "Y":
            Decision_Services_Waterfall = "P08 Debt Review Granted"
        elif EST_Customer_Score < 540:
            Decision_Services_Waterfall = "C01 Established Customer Score < 540"
        else:
            pass
    return Decision_Services_Segment, Filter_Responsible_State, Decision_Services_Waterfall


schema_dsw_RES = t.StructType([
    t.StructField("DECISION_SERVICES_SEGMENT", t.StringType(), True),
    t.StructField("FILTER_RESPONSIBLE_STATE", t.IntegerType(), True),
    t.StructField("DECISION_SERVICES_WATERFALL", t.StringType(), True)
])
udf_dsw_RES = f.udf(dsw_RES, returnType=schema_dsw_RES)


def dsw_CLR(APP_Customer_State, Decision_Services_Segment, Filter_Clear_State, Decision_Services_Waterfall,
            CST_Deceased, CST_CustomerAge, CST_Fraud, CST_Sequestration, CST_Dispute, CST_Emigrated, ALL_Notices5Years,
            CST_DebtReviewGranted, EST_Customer_Score):
    if APP_Customer_State == "CLR Clear":
        Decision_Services_Segment = "CRD"
        Filter_Clear_State = 1
        if CST_Deceased == 'Y':
            Decision_Services_Waterfall = "P01 Consumer Deceased"
        elif CST_CustomerAge < 1800:
            Decision_Services_Waterfall = "P02 Consumer Age < 18"
        elif CST_Fraud == "Y":
            Decision_Services_Waterfall = "P03 Payment Profile Fraud"
        elif CST_Sequestration == "Y":
            Decision_Services_Waterfall = "P04 Consumer Sequestrated"
        elif CST_Dispute == 'Y':
            Decision_Services_Waterfall = "P05 Payment Profile Dispute"
        elif CST_Emigrated == "Y":
            Decision_Services_Waterfall = "P06 Consumer Emigrated"
        elif ALL_Notices5Years == "Y":
            Decision_Services_Waterfall = "P07 Public Notice Issued"
        elif CST_DebtReviewGranted == "Y":
            Decision_Services_Waterfall = "P08 Debt Review Granted"
        elif EST_Customer_Score < 540:
            Decision_Services_Waterfall = "C01 Established Customer Score < 540"
        else:
            pass
    return Decision_Services_Segment, Filter_Clear_State, Decision_Services_Waterfall


schema_dsw_CLR = t.StructType([
    t.StructField("DECISION_SERVICES_SEGMENT", t.StringType(), True),
    t.StructField("FILTER_CLEAR_STATE", t.IntegerType(), True),
    t.StructField("DECISION_SERVICES_WATERFALL", t.StringType(), True)
])
udf_dsw_CLR = f.udf(dsw_CLR, returnType=schema_dsw_CLR)


def dsw_CRD(APP_Customer_State, Decision_Services_Segment, Filter_XXXXXX_State, Decision_Services_Waterfall,
            CST_Deceased, CST_CustomerAge, CST_Fraud, CST_Sequestration, CST_Dispute, CST_Emigrated, ALL_Notices5Years,
            CST_DebtReviewGranted, CST_DebtReviewRequested, CBX_Prism_TM, EST_Customer_Score,
            ALL_Perc0Delq90Days, ALL_NumPayments90Days):
    if APP_Customer_State == "CRD Credit Balance":
        Decision_Services_Segment = "CRD"
        Filter_XXXXXX_State = 1
        if CST_Deceased == 'Y':
            Decision_Services_Waterfall = "P01 Consumer Deceased"
        elif CST_CustomerAge < 1800:
            Decision_Services_Waterfall = "P02 Consumer Age < 18"
        elif CST_Fraud == "Y":
            Decision_Services_Waterfall = "P03 Payment Profile Fraud"
        elif CST_Sequestration == "Y":
            Decision_Services_Waterfall = "P04 Consumer Sequestrated"
        elif CST_Dispute == 'Y':
            Decision_Services_Waterfall = "P05 Payment Profile Dispute"
        elif CST_Emigrated == "Y":
            Decision_Services_Waterfall = "P06 Consumer Emigrated"
        elif ALL_Notices5Years == "Y":
            Decision_Services_Waterfall = "P07 Public Notice Issued"
        elif CST_DebtReviewGranted == "Y":
            Decision_Services_Waterfall = "P08 Debt Review Granted"
        elif CST_DebtReviewRequested == "Y":
            Decision_Services_Waterfall = "G01 Debt Review Requested"
        elif CBX_Prism_TM < 570:
            Decision_Services_Waterfall = "C01 Prism TM < 570"
        elif EST_Customer_Score < 540:
            Decision_Services_Waterfall = "C01 Established Customer Score < 540"
        elif ALL_Perc0Delq90Days < 10:
            Decision_Services_Waterfall = "G02 UTD bureau trades last 90 days < 10%"
        elif ALL_NumPayments90Days == 0:
            Decision_Services_Waterfall = "G04 Number bureau payments last 90 days = 0"
        else:
            pass
    return Decision_Services_Segment, Filter_XXXXXX_State, Decision_Services_Waterfall


schema_dsw_CRD = t.StructType([
    t.StructField("DECISION_SERVICES_SEGMENT", t.StringType(), True),
    t.StructField("FILTER_XXXXXX_STATE", t.IntegerType(), True),
    t.StructField("DECISION_SERVICES_WATERFALL", t.StringType(), True)
])
udf_dsw_CRD = f.udf(dsw_CRD, returnType=schema_dsw_CRD)


def dsw_PUP(APP_Customer_State, Decision_Services_Segment, Filter_XXXXXX_State, Decision_Services_Waterfall,
            CST_Deceased, CST_CustomerAge, CST_Fraud, CST_Sequestration, CST_Dispute, CST_Emigrated, ALL_Notices5Years,
            CST_DebtReviewGranted, CST_DebtReviewRequested, CBX_Prism_TM, EST_Customer_Score,
            ALL_Perc0Delq90Days, ALL_NumPayments90Days):
    if APP_Customer_State == "PUP Paid-Up":
        Decision_Services_Segment = "PUP"
        Filter_XXXXXX_State = 1
        if CST_Deceased == 'Y':
            Decision_Services_Waterfall = "P01 Consumer Deceased"
        elif CST_CustomerAge < 1800:
            Decision_Services_Waterfall = "P02 Consumer Age < 18"
        elif CST_Fraud == "Y":
            Decision_Services_Waterfall = "P03 Payment Profile Fraud"
        elif CST_Sequestration == "Y":
            Decision_Services_Waterfall = "P04 Consumer Sequestrated"
        elif CST_Dispute == 'Y':
            Decision_Services_Waterfall = "P05 Payment Profile Dispute"
        elif CST_Emigrated == "Y":
            Decision_Services_Waterfall = "P06 Consumer Emigrated"
        elif ALL_Notices5Years == "Y":
            Decision_Services_Waterfall = "P07 Public Notice Issued"
        elif CST_DebtReviewGranted == "Y":
            Decision_Services_Waterfall = "P08 Debt Review Granted"
        elif CST_DebtReviewRequested == "Y":
            Decision_Services_Waterfall = "G01 Debt Review Requested"
        elif CBX_Prism_TM < 570:
            Decision_Services_Waterfall = "C01 Prism TM < 570"
        elif EST_Customer_Score < 540:
            Decision_Services_Waterfall = "C01 Established Customer Score < 540"
        elif ALL_Perc0Delq90Days < 10:
            Decision_Services_Waterfall = "G02 UTD bureau trades last 90 days < 10%"
        elif ALL_NumPayments90Days == 0:
            Decision_Services_Waterfall = "G04 Number bureau payments last 90 days = 0"
        else:
            pass
    return Decision_Services_Segment, Filter_XXXXXX_State, Decision_Services_Waterfall


schema_dsw_PUP = t.StructType([
    t.StructField("DECISION_SERVICES_SEGMENT", t.StringType(), True),
    t.StructField("FILTER_XXXXXX_STATE", t.IntegerType(), True),
    t.StructField("DECISION_SERVICES_WATERFALL", t.StringType(), True)
])
udf_dsw_PUP = f.udf(dsw_PUP, returnType=schema_dsw_PUP)


def dsw_IMM(APP_Customer_State, Decision_Services_Segment, Filter_Immature_State, Decision_Services_Waterfall,
            CST_Deceased, CST_CustomerAge, CST_Fraud, CST_Sequestration, CST_Dispute, CST_Emigrated, ALL_Notices5Years,
            CST_DebtReviewGranted, CST_DebtReviewRequested, APP_Gross_Income, CBX_Prism_TM, NEW_Customer_Score,
            ALL_Perc0Delq90Days, ALL_NumPayments90Days):
    if APP_Customer_State == "IMM MOB 00-05":
        Decision_Services_Segment = "IMM"
        Filter_Immature_State = 1
        if CST_Deceased == 'Y':
            Decision_Services_Waterfall = "P01 Consumer Deceased"
        elif CST_CustomerAge < 1800:
            Decision_Services_Waterfall = "P02 Consumer Age < 18"
        elif CST_Fraud == "Y":
            Decision_Services_Waterfall = "P03 Payment Profile Fraud"
        elif CST_Sequestration == "Y":
            Decision_Services_Waterfall = "P04 Consumer Sequestrated"
        elif CST_Dispute == 'Y':
            Decision_Services_Waterfall = "P05 Payment Profile Dispute"
        elif CST_Emigrated == "Y":
            Decision_Services_Waterfall = "P06 Consumer Emigrated"
        elif ALL_Notices5Years == "Y":
            Decision_Services_Waterfall = "P07 Public Notice Issued"
        elif CST_DebtReviewGranted == "Y":
            Decision_Services_Waterfall = "P08 Debt Review Granted"
        elif CST_DebtReviewRequested == "Y":
            Decision_Services_Waterfall = "G01 Debt Review Requested"
        elif APP_Gross_Income < 1500:
            Decision_Services_Waterfall = "C06 Gross Income < R1500"
        elif CBX_Prism_TM < 580:
            Decision_Services_Waterfall = "C01 Prism TM < 580"
        elif NEW_Customer_Score < 555:
            Decision_Services_Waterfall = "C01 New Customer Score < 555"
        elif ALL_Perc0Delq90Days < 20:
            Decision_Services_Waterfall = "G02 UTD bureau trades last 90 days < 20%"
        elif ALL_NumPayments90Days == 0:
            Decision_Services_Waterfall = "G04 Number bureau payments last 90 days = 0"
        else:
            pass
    return Decision_Services_Segment, Filter_Immature_State, Decision_Services_Waterfall


schema_dsw_IMM = t.StructType([
    t.StructField("DECISION_SERVICES_SEGMENT", t.StringType(), True),
    t.StructField("FILTER_IMMATURE_STATE", t.IntegerType(), True),
    t.StructField("DECISION_SERVICES_WATERFALL", t.StringType(), True)
])
udf_dsw_IMM = f.udf(dsw_IMM, returnType=schema_dsw_IMM)


def dsw_FAA(APP_Customer_State, Decision_Services_Segment, Filter_First_Account_Applicant,
            Decision_Services_Waterfall, CST_Deceased, CST_CustomerAge, CST_Fraud, CST_Sequestration, CST_Dispute,
            CST_Emigrated, ALL_Notices5Years, CST_DebtReviewGranted, CST_DebtReviewRequested, APP_Gross_Income,
            CBX_Prism_TM, NEW_Customer_Score, ALL_Perc0Delq90Days, ALL_NumPayments90Days):
    if APP_Customer_State == "":
        Decision_Services_Segment = "FAA"
        Filter_First_Account_Applicant = 1
        if CST_Deceased == 'Y':
            Decision_Services_Waterfall = "P01 Consumer Deceased"
        elif CST_CustomerAge < 1800:
            Decision_Services_Waterfall = "P02 Consumer Age < 18"
        elif CST_Fraud == "Y":
            Decision_Services_Waterfall = "P03 Payment Profile Fraud"
        elif CST_Sequestration == "Y":
            Decision_Services_Waterfall = "P04 Consumer Sequestrated"
        elif CST_Dispute == 'Y':
            Decision_Services_Waterfall = "P05 Payment Profile Dispute"
        elif CST_Emigrated == "Y":
            Decision_Services_Waterfall = "P06 Consumer Emigrated"
        elif ALL_Notices5Years == "Y":
            Decision_Services_Waterfall = "P07 Public Notice Issued"
        elif CST_DebtReviewGranted == "Y":
            Decision_Services_Waterfall = "P08 Debt Review Granted"
        elif CST_DebtReviewRequested == "Y":
            Decision_Services_Waterfall = "G01 Debt Review Requested"
        elif APP_Gross_Income < 1500:
            Decision_Services_Waterfall = "C06 Gross Income < R1500"
        elif CBX_Prism_TM < 580:
            Decision_Services_Waterfall = "C01 Prism TM < 580"
        elif NEW_Customer_Score < 555:
            Decision_Services_Waterfall = "C01 New Customer Score < 555"
        elif ALL_Perc0Delq90Days < 20:
            Decision_Services_Waterfall = "G02 UTD bureau trades last 90 days < 20%"
        elif ALL_NumPayments90Days == 0:
            Decision_Services_Waterfall = "G04 Number bureau payments last 90 days = 0"
        else:
            pass
    return Decision_Services_Segment, Filter_First_Account_Applicant, Decision_Services_Waterfall


schema_dsw_FAA = t.StructType([
    t.StructField("DECISION_SERVICES_SEGMENT", t.StringType(), True),
    t.StructField("FILTER_FIRST_ACCOUNT_APPLICANT", t.IntegerType(), True),
    t.StructField("DECISION_SERVICES_WATERFALL", t.StringType(), True)
])
udf_dsw_FAA = f.udf(dsw_FAA, returnType=schema_dsw_FAA)


def dsw_WWW(APP_Customer_State, Filter_Channel_Online, Decision_Services_Segment, Filter_Web_Service,
            Decision_Services_Waterfall, CST_Deceased, CST_CustomerAge, CST_Fraud, CST_Sequestration, CST_Dispute,
            CST_Emigrated, ALL_Notices5Years, CST_DebtReviewGranted, CST_DebtReviewRequested, APP_Gross_Income,
            CBX_Prism_TM, NEW_Customer_Score, ALL_Perc0Delq90Days, ALL_NumPayments90Days):
    if (APP_Customer_State in ["", "IMM MOB 00-05"]) and (Filter_Channel_Online is not None):
        Decision_Services_Segment = "WWW"
        Filter_Web_Service = 1
        if CST_Deceased == 'Y':
            Decision_Services_Waterfall = "P01 Consumer Deceased"
        elif CST_CustomerAge < 1800:
            Decision_Services_Waterfall = "P02 Consumer Age < 18"
        elif CST_Fraud == "Y":
            Decision_Services_Waterfall = "P03 Payment Profile Fraud"
        elif CST_Sequestration == "Y":
            Decision_Services_Waterfall = "P04 Consumer Sequestrated"
        elif CST_Dispute == 'Y':
            Decision_Services_Waterfall = "P05 Payment Profile Dispute"
        elif CST_Emigrated == "Y":
            Decision_Services_Waterfall = "P06 Consumer Emigrated"
        elif ALL_Notices5Years == "Y":
            Decision_Services_Waterfall = "P07 Public Notice Issued"
        elif CST_DebtReviewGranted == "Y":
            Decision_Services_Waterfall = "P08 Debt Review Granted"
        elif CST_DebtReviewRequested == "Y":
            Decision_Services_Waterfall = "G01 Debt Review Requested"
        elif APP_Gross_Income < 1500:
            Decision_Services_Waterfall = "C06 Gross Income < R1500"
        elif CBX_Prism_TM < 580:
            Decision_Services_Waterfall = "C01 Prism TM < 580"
        elif NEW_Customer_Score < 555:
            Decision_Services_Waterfall = "C01 New Customer Score < 555"
        elif ALL_Perc0Delq90Days < 20:
            Decision_Services_Waterfall = "G02 UTD bureau trades last 90 days < 20%"
        elif ALL_NumPayments90Days == 0:
            Decision_Services_Waterfall = "G04 Number bureau payments last 90 days = 0"
        else:
            pass
    return Decision_Services_Segment, Filter_Web_Service, Decision_Services_Waterfall


schema_dsw_WWW = t.StructType([
    t.StructField("DECISION_SERVICES_SEGMENT", t.StringType(), True),
    t.StructField("FILTER_WEB_SERVICE", t.IntegerType(), True),
    t.StructField("DECISION_SERVICES_WATERFALL", t.StringType(), True)
])
udf_dsw_WWW = f.udf(dsw_WWW, returnType=schema_dsw_WWW)


def dsw_TSI(APP_Customer_State, Filter_Channel_Inbound, Decision_Services_Segment, Filter_Telesales_Inbound,
            Decision_Services_Waterfall, CST_Deceased, CST_CustomerAge, CST_Fraud, CST_Sequestration, CST_Dispute,
            CST_Emigrated, ALL_Notices5Years, CST_DebtReviewGranted, CST_DebtReviewRequested, APP_Gross_Income,
            CBX_Prism_TM, NEW_Customer_Score, ALL_Perc0Delq90Days, ALL_NumPayments90Days):
    if (APP_Customer_State in ["", "IMM MOB 00-05"]) and (Filter_Channel_Inbound is not None):
        Decision_Services_Segment = "TSI"
        Filter_Telesales_Inbound = 1
        if CST_Deceased == 'Y':
            Decision_Services_Waterfall = "P01 Consumer Deceased"
        elif CST_CustomerAge < 1800:
            Decision_Services_Waterfall = "P02 Consumer Age < 18"
        elif CST_Fraud == "Y":
            Decision_Services_Waterfall = "P03 Payment Profile Fraud"
        elif CST_Sequestration == "Y":
            Decision_Services_Waterfall = "P04 Consumer Sequestrated"
        elif CST_Dispute == 'Y':
            Decision_Services_Waterfall = "P05 Payment Profile Dispute"
        elif CST_Emigrated == "Y":
            Decision_Services_Waterfall = "P06 Consumer Emigrated"
        elif ALL_Notices5Years == "Y":
            Decision_Services_Waterfall = "P07 Public Notice Issued"
        elif CST_DebtReviewGranted == "Y":
            Decision_Services_Waterfall = "P08 Debt Review Granted"
        elif CST_DebtReviewRequested == "Y":
            Decision_Services_Waterfall = "G01 Debt Review Requested"
        elif APP_Gross_Income < 1500:
            Decision_Services_Waterfall = "C06 Gross Income < R1500"
        elif CBX_Prism_TM < 580:
            Decision_Services_Waterfall = "C01 Prism TM < 580"
        elif NEW_Customer_Score < 555:
            Decision_Services_Waterfall = "C01 New Customer Score < 555"
        elif ALL_Perc0Delq90Days < 40:
            Decision_Services_Waterfall = "G02 UTD bureau trades last 90 days < 40%"
        elif ALL_NumPayments90Days == 0:
            Decision_Services_Waterfall = "G04 Number bureau payments last 90 days = 0"
        else:
            pass
    return Decision_Services_Segment, Filter_Telesales_Inbound, Decision_Services_Waterfall


schema_dsw_TSI = t.StructType([
    t.StructField("DECISION_SERVICES_SEGMENT", t.StringType(), True),
    t.StructField("FILTER_TELESALES_INBOUND", t.IntegerType(), True),
    t.StructField("DECISION_SERVICES_WATERFALL", t.StringType(), True)
])
udf_dsw_TSI = f.udf(dsw_TSI, returnType=schema_dsw_TSI)


def dsw_NTC(ALL_TimeOldestTrade, Decision_Services_Segment, Filter_New_To_Credit, Decision_Services_Waterfall,
            CST_Deceased, CST_CustomerAge, CST_Fraud, CST_Sequestration, CST_Dispute, CST_Emigrated, ALL_Notices5Years,
            CST_DebtReviewGranted, CST_DebtReviewRequested, APP_Gross_Income, NTC_Accept_Final_Score_V01,
            NTC_Accept_Risk_Score_V01):
    if ALL_TimeOldestTrade < 3:
        Decision_Services_Segment = "NTC"
        Filter_New_To_Credit = 1
        if CST_Deceased == "Y":
            Decision_Services_Waterfall = "P01 Consumer Deceased"
        elif CST_CustomerAge < 1800:
            Decision_Services_Waterfall = "P02 Consumer Age < 18"
        elif CST_Fraud == "Y":
            Decision_Services_Waterfall = "P03 Payment Profile Fraud"
        elif CST_Sequestration == "Y":
            Decision_Services_Waterfall = "P04 Consumer Sequestrated"
        elif CST_Dispute == "Y":
            Decision_Services_Waterfall = "P05 Payment Profile Dispute"
        elif CST_Emigrated == "Y":
            Decision_Services_Waterfall = "P06 Consumer Emigrated"
        elif ALL_Notices5Years == "Y":
            Decision_Services_Waterfall = "P07 Public Notice Issued"
        elif CST_DebtReviewGranted == "Y":
            Decision_Services_Waterfall = "P08 Debt Review Granted"
        elif CST_DebtReviewRequested == "Y":
            Decision_Services_Waterfall = "G01 Debt Review Requested"
        elif APP_Gross_Income < 1500:
            Decision_Services_Waterfall = "C06 Gross Income < R1500"
        elif NTC_Accept_Final_Score_V01 < 580:
            Decision_Services_Waterfall = "C01 Accept Final Score V01 < 580"
        elif NTC_Accept_Risk_Score_V01 < 580:
            Decision_Services_Waterfall = "C01 Accept Risk Score V01 < 580"
        else:
            pass
    return Decision_Services_Segment, Filter_New_To_Credit, Decision_Services_Waterfall


schema_dsw_NTC = t.StructType([
    t.StructField("DECISION_SERVICES_SEGMENT", t.StringType(), True),
    t.StructField("FILTER_NEW_TO_CREDIT", t.IntegerType(), True),
    t.StructField("DECISION_SERVICES_WATERFALL", t.StringType(), True)
])
udf_dsw_NTC = f.udf(dsw_NTC, returnType=schema_dsw_NTC)


def dsw_TSO(APP_Customer_State, Filter_Channel_Outbound, Decision_Services_Segment, Filter_Telesales_Outbound,
            Decision_Services_Waterfall, CST_Deceased, CST_CustomerAge, CST_Fraud, CST_Sequestration, CST_Dispute,
            CST_Emigrated, ALL_Notices5Years, CST_DebtReviewGranted, CST_DebtReviewRequested, APP_Gross_Income,
            CBX_Prism_TM, NEW_Customer_Score, ALL_Perc0Delq90Days, ALL_NumPayments90Days):
    if (APP_Customer_State in ["", "IMM MOB 00-05"]) and (Filter_Channel_Outbound is not None):
        Decision_Services_Segment = "TSO"
        Filter_Telesales_Outbound = 1
        if CST_Deceased == "Y":
            Decision_Services_Waterfall = "P01 Consumer Deceased"
        elif CST_CustomerAge < 1800:
            Decision_Services_Waterfall = "P02 Consumer Age < 18"
        elif CST_Fraud == "Y":
            Decision_Services_Waterfall = "P03 Payment Profile Fraud"
        elif CST_Sequestration == "Y":
            Decision_Services_Waterfall = "P04 Consumer Sequestrated"
        elif CST_Dispute == "Y":
            Decision_Services_Waterfall = "P05 Payment Profile Dispute"
        elif CST_Emigrated == "Y":
            Decision_Services_Waterfall = "P06 Consumer Emigrated"
        elif ALL_Notices5Years == "Y":
            Decision_Services_Waterfall = "P07 Public Notice Issued"
        elif CST_DebtReviewGranted == "Y":
            Decision_Services_Waterfall = "P08 Debt Review Granted"
        elif CST_DebtReviewRequested == "Y":
            Decision_Services_Waterfall = "G01 Debt Review Requested"
        elif APP_Gross_Income < 1500:
            Decision_Services_Waterfall = "C06 Gross Income < R1500"
        elif CBX_Prism_TM < 580:
            Decision_Services_Waterfall = "C01 Prism TM < 580"
        elif NEW_Customer_Score < 555:
            Decision_Services_Waterfall = "C01 New Customer Score < 555"
        elif ALL_Perc0Delq90Days < 60:
            Decision_Services_Waterfall = "G02 UTD bureau trades last 90 days < 60%"
        elif ALL_NumPayments90Days == 0:
            Decision_Services_Waterfall = "G04 Number bureau payments last 90 days = 0"
        else:
            pass

    return Decision_Services_Segment, Filter_Telesales_Outbound, Decision_Services_Waterfall


schema_dsw_TSO = t.StructType([
    t.StructField("DECISION_SERVICES_SEGMENT", t.StringType(), True),
    t.StructField("FILTER_TELESALES_OUTBOUND", t.IntegerType(), True),
    t.StructField("DECISION_SERVICES_WATERFALL", t.StringType(), True)
])
udf_dsw_TSO = f.udf(dsw_TSO, returnType=schema_dsw_TSO)


# def Decision_Service_Waterfallxxx(sdf_inp):
#     sdf_0 = sdf_inp \
#         .withColumn("Decision_Services_Segment",
#                     f.when((f.col("All_TimeOldestTrade") < 3),
#                            f.lit("NTC"))
#                      .otherwise(f.lit(None)))\
#         .withColumn("Filter_New_To_Credit",
#                     f.when((f.col("All_TimeOldestTrade") < 3),
#                            f.lit(1))
#                      .otherwise(f.lit(None)))\
#         .withColumn("Decision_Services_Waterfall",
#                     f.when((f.col("All_TimeOldestTrade") < 3) &
#                            (f.col("CST_Deceased") == "Y"), f.lit("P01 Consumer Deceased"))
#                      .when((f.col("All_TimeOldestTrade") < 3) &
#                            (f.col("CST_CustomerAge") < 1800), f.lit("P02 Consumer Age < 18"))
#                      .when((f.col("All_TimeOldestTrade") < 3) &
#                            (f.col("CST_Fraud") == "Y"), f.lit("P03 Payment Profile Fraud"))
#                      .when((f.col("All_TimeOldestTrade") < 3) &
#                            (f.col("CST_Sequestration") == "Y"), f.lit("P04 Consumer Sequestrated"))
#                      .when((f.col("All_TimeOldestTrade") < 3) &
#                            (f.col("CST_Dispute") == "Y"), f.lit("P05 Payment Profile Dispute"))
#                      .when((f.col("All_TimeOldestTrade") < 3) &
#                            (f.col("CST_Emigrated") == "Y"), f.lit("P06 Consumer Emigrated"))
#                      .when((f.col("All_TimeOldestTrade") < 3) &
#                            (f.col("ALL_Notices5Years") == "Y"), f.lit("P07 Public Notice Issued"))
#                      .when((f.col("All_TimeOldestTrade") < 3) &
#                            (f.col("CST_DebtReviewGranted") == "Y"), f.lit("P08 Debt Review Granted"))
#                      .when((f.col("All_TimeOldestTrade") < 3) &
#                            (f.col("CST_DebtReviewRequested") == "Y"), f.lit("G01 Debt Review Requested"))
#                      .when((f.col("All_TimeOldestTrade") < 3) &
#                            (f.col("APP_Gross_Income") < 1500), f.lit("G06 Gross Income < R1500"))
#                      .when((f.col("All_TimeOldestTrade") < 3) &
#                            (f.col("NTC_Accept_Final_Score_V01") < 580), f.lit("C01 Accept Final Score V01 < 580"))
#                      .when((f.col("All_TimeOldestTrade") < 3) &
#                            (f.col("NTC_Accept_Risk_Score_V01") < 580), f.lit("C01 Accept Risk Score V01 < 580")))\
#         .withColumn("nest",
#                     f.when((f.col("All_TimeOldestTrade") < 3),
#                            udf_Risk_Grade_Matrix(f.lit("RG9"), f.lit(999), f.lit(999), f.lit(999), f.lit(999),
#                                                  f.lit(999), f.lit(999), f.col("NEW_Customer_Score"), f.lit(999),
#                                                  f.lit(999), f.lit(999), f.lit(999), f.lit(999), f.lit(999),
#                                                  f.col("Decision_Services_Waterfall"), f.col("CBX_Prism_TM"))))\
#         .withColumn("Decision_Services_Outcome",
#                     f.when((f.col("All_TimeOldestTrade") < 3),
#                            f.col("nest.Decision_Services_Outcome"))
#                      .otherwise(f.col("Decision_Services_Outcome")))\
#         .withColumn("Decision_Services_Waterfall",
#                     f.when((f.col("All_TimeOldestTrade") < 3),
#                            f.col("nest.Decision_Services_Waterfall"))
#                      .otherwise(f.col("Decision_Services_Waterfall")))\
#         .withColumn("Decision_Services_Matrix",
#                     f.when((f.col("All_TimeOldestTrade") < 3),
#                            f.col("nest.Decision_Services_Matrix"))
#                      .otherwise(f.col("Decision_Services_Matrix")))\
#         .drop(*["nest"])
#
#     sdf_1 = sdf_0\
#         .withColumn("Decision_Services_Segment",
#                     f.when((f.col("APP_Customer_State").isin(["", "IMM MOB 00-05"]) &
#                            f.col("Filter_Channel_Outbound").isNotNull()),
#                            f.lit("TSO"))
#                      .otherwise(f.col("Decision_Services_Segment")))\
#         .withColumn("Filter_Telesales_Outbound",
#                     f.when((f.col("APP_Customer_State").isin(["", "IMM MOB 00-05"]) &
#                            f.col("Filter_Channel_Outbound").isNotNull()),
#                            f.lit("1"))
#                      .otherwise(f.lit(None)))\
#         .withColumn("Decision_Services_Waterfall",
#                     f.when((f.col("APP_Customer_State").isin(["", "IMM MOB 00-05"]) &
#                            f.col("Filter_Channel_Outbound").isNotNull()) &
#                            (f.col("CST_Deceased") == "Y"),
#                            f.lit("P01 Customer Deceased"))
#                      .when((f.col("APP_Customer_State").isin(["", "IMM MOB 00-05"]) &
#                            f.col("Filter_Channel_Outbound").isNotNull()) &
#                            (f.col("CST_CustomerAge") < 1800),
#                            f.lit("P02 Customer Age < 18"))
#                      .when((f.col("APP_Customer_State").isin(["", "IMM MOB 00-05"]) &
#                            f.col("Filter_Channel_Outbound").isNotNull()) &
#                            (f.col("CST_Fraud") == "Y"),
#                            f.lit("P03 Payment Profile Fraud"))
#                      .when((f.col("APP_Customer_State").isin(["", "IMM MOB 00-05"]) &
#                            f.col("Filter_Channel_Outbound").isNotNull()) &
#                            (f.col("CST_Sequestration") == "Y"),
#                            f.lit("P04 Consumer Sequestrated"))
#                      .when((f.col("APP_Customer_State").isin(["", "IMM MOB 00-05"]) &
#                            f.col("Filter_Channel_Outbound").isNotNull()) &
#                            (f.col("CST_Dispute") == "Y"),
#                            f.lit("P05 Payment Profile Dispute"))
#                      .when((f.col("APP_Customer_State").isin(["", "IMM MOB 00-05"]) &
#                            f.col("Filter_Channel_Outbound").isNotNull()) &
#                            (f.col("CST_Emigrated") == "Y"),
#                            f.lit("P06 Consumer Emigrated"))
#                      .when((f.col("APP_Customer_State").isin(["", "IMM MOB 00-05"]) &
#                            f.col("Filter_Channel_Outbound").isNotNull()) &
#                            (f.col("CST_Notices5Years") == "Y"),
#                            f.lit("P07 Public Notice Issued"))
#                      .when((f.col("APP_Customer_State").isin(["", "IMM MOB 00-05"]) &
#                            f.col("Filter_Channel_Outbound").isNotNull()) &
#                            (f.col("CST_DebtReviewGranted") == "Y"),
#                            f.lit("P08 Debt Review Granted"))
#                      .when((f.col("APP_Customer_State").isin(["", "IMM MOB 00-05"]) &
#                            f.col("Filter_Channel_Outbound").isNotNull()) &
#                            (f.col("CST_DebtReviewRequested") == "Y"),
#                            f.lit("G01 Debt Review Requested"))
#                      .when((f.col("APP_Customer_State").isin(["", "IMM MOB 00-05"]) &
#                            f.col("Filter_Channel_Outbound").isNotNull()) &
#                            (f.col("APP_Gross_Income") < 1500),
#                            f.lit("C06 Gross Income < R1500"))
#                      .when((f.col("APP_Customer_State").isin(["", "IMM MOB 00-05"]) &
#                            f.col("Filter_Channel_Outbound").isNotNull()) &
#                            (f.col("CBX_Prism_TM") < 580),
#                            f.lit("C01 Prism TM < 580"))
#                      .when((f.col("APP_Customer_State").isin(["", "IMM MOB 00-05"]) &
#                            f.col("Filter_Channel_Outbound").isNotNull()) &
#                            (f.col("NEW_Customer_Score") < 555),
#                            f.lit("C01 New Customer Score < 555"))
#                      .when((f.col("APP_Customer_State").isin(["", "IMM MOB 00-05"]) &
#                            f.col("Filter_Channel_Outbound").isNotNull()) &
#                            (f.col("ALL_Perc0Delq90Days") < 60),
#                            f.lit("G02 UTD bureau trades last 90 days < 60%"))
#                      .when((f.col("APP_Customer_State").isin(["", "IMM MOB 00-05"]) &
#                            f.col("Filter_Channel_Outbound").isNotNull()) &
#                            (f.col("ALL_NumPayments90Days") == 0),
#                            f.lit("G04 Number bureau payments last 90 days = 0")))\
#         .withColumn("nest",
#                     f.when((f.col("APP_Customer_State").isin(["", "IMM MOB 00-05"]) &
#                            f.col("Filter_Channel_Outbound").isNotNull()),
#                            udf_Risk_Grade_Matrix(f.lit("RG8"), f.lit(580), f.lit(640), f.lit(999), f.lit(999),
#                                                  f.lit(999), f.lit(999), f.col("NEW_Customer_Score"), f.lit(555),
#                                                  f.lit(585), f.lit(999), f.lit(999), f.lit(999), f.lit(999),
#                                                  f.col("Decision_Services_Waterfall"), f.col("CBX_Prism_TM")))) \
#         .withColumn("Decision_Services_Outcome",
#                     f.when((f.col("APP_Customer_State").isin(["", "IMM MOB 00-05"]) &
#                            f.col("Filter_Channel_Outbound").isNotNull()),
#                            f.col("nest.Decision_Services_Outcome"))
#                      .otherwise(f.col("Decision_Services_Outcome")))\
#         .withColumn("Decision_Services_Waterfall",
#                     f.when((f.col("APP_Customer_State").isin(["", "IMM MOB 00-05"]) &
#                            f.col("Filter_Channel_Outbound").isNotNull()),
#                            f.col("nest.Decision_Services_Waterfall"))
#                      .otherwise(f.col("Decision_Services_Waterfall")))\
#         .withColumn("Decision_Services_Matrix",
#                     f.when((f.col("APP_Customer_State").isin(["", "IMM MOB 00-05"]) &
#                            f.col("Filter_Channel_Outbound").isNotNull()),
#                            f.col("nest.Decision_Services_Matrix"))
#                      .otherwise(f.col("Decision_Services_Matrix")))\
#         .drop(*["nest"])\
#         .withColumn("Decision_Services_Risk_Grade",
#                     f.when((f.col("APP_Customer_State").isin(["", "IMM MOB 00-05"]) &
#                            f.col("Filter_Channel_Outbound").isNotNull() &
#                            f.col("Decision_Services_Matrix") == "580x555"),
#                            f.lit("9"))
#                      .when((f.col("APP_Customer_State").isin(["", "IMM MOB 00-05"]) &
#                            f.col("Filter_Channel_Outbound").isNotNull() &
#                            f.col("Decision_Services_Matrix") == "580x585"),
#                            f.lit("8"))
#                      .when((f.col("APP_Customer_State").isin(["", "IMM MOB 00-05"]) &
#                            f.col("Filter_Channel_Outbound").isNotNull() &
#                            f.col("Decision_Services_Matrix") == "640x555"),
#                            f.lit("8"))
#                      .when((f.col("APP_Customer_State").isin(["", "IMM MOB 00-05"]) &
#                            f.col("Filter_Channel_Outbound").isNotNull() &
#                            f.col("Decision_Services_Matrix") == "640x585"),
#                            f.lit("8")))


# def Decision_Service_Waterfall(CST_Deceased, CST_Customer_Age, CST_Fraud, CST_Sequestration, CST_Dispute,
#                                APP_Customer_State, CST_Emigrated, ALL_Notices5Years, CST_DebtReviewGranted,
#                                All_TimeOldestTrade, APP_Gross_Income, APP_Subscriptions, Customer_State,
#                                Filter_Clear_State, Filter_Responsible_State, Filter_Erratic_State,
#                                Filter_Arrears_State, Filter_Channel_Outbound, Filter_Channel_Inbound,
#                                Filter_Channel_Online):
#     Filter_New_To_Credit = Filter_Telesales_Outbound = Filter_Telesales_Inbound = None
#     Filter_Web_Service = Filter_First_Account_Applicant = Filter_XXXXXX_State = Filter_Immature_Activations = None
#     Filter_Clear_Activations = Filter_Responsible_Activations = Filter_Erratic_Activations = None
#     Filter_Arrears_Activations = Filter_Other_Activations = Filter_Immature_State = None
#
#     if CST_Deceased == "Y":
#         APP_Decision_Service_Waterfall = "A.01 Bureau Policy Decline (P01=Consumer Deceased)"
#     elif CST_Customer_Age < 1800:
#         APP_Decision_Service_Waterfall = "A.02 Bureau Policy Decline (P02=Consumer Age < 18)"
#     elif CST_Fraud == "Y":
#         APP_Decision_Service_Waterfall = "A.03 Bureau Policy Decline (P03=Payment Profile Fraud)"
#     elif CST_Sequestration == "Y":
#         APP_Decision_Service_Waterfall = "A.04 Bureau Policy Decline (P04=Consumer Sequestrated)"
#     elif (CST_Dispute == "Y") and (APP_Customer_State != "CLR Clear"):
#         APP_Decision_Service_Waterfall = "A.05 Bureau Policy Decline (P05=Payment Profile Dispute, except CLR)"
#     elif CST_Emigrated == "Y":
#         APP_Decision_Service_Waterfall = "A.06 Bureau Policy Decline (P06=Consumer Immigrated)"
#     elif ALL_Notices5Years == "Y":
#         APP_Decision_Service_Waterfall = "A.07 Bureau Policy Decline (P07=Public Notice Issued)"
#     elif (CST_DebtReviewGranted == "Y") and (APP_Customer_State != "CLR Clear"):
#         APP_Decision_Service_Waterfall = "A.08 Bureau Policy Decline (P08=Debt Review Granted, except CLR)"
#     elif (All_TimeOldestTrade < 3) and (APP_Gross_Income < 1500):
#         APP_Decision_Service_Waterfall = "B.01 Thin File (C05=Gross Income < R1500)"
#     elif (APP_Customer_State == "") and (APP_Gross_Income < 1500):
#         APP_Decision_Service_Waterfall = "B.02 New To Credit (C05=Gross Income < R1500)"
#     elif (APP_Customer_State == "IMM MOB 00-05") and (APP_Gross_Income < 1500):
#         APP_Decision_Service_Waterfall = "B.03 Immature Customer State (C05=Gross Income < R1500)"
#     elif All_TimeOldestTrade < 3:
#         APP_Decision_Service_Waterfall = "C. New To Credit (Thin File)"
#         Filter_New_To_Credit = 1
#     elif (APP_Customer_State in ["", "IMM MOB 00-05"]) and (Filter_Channel_Outbound is not None):
#         APP_Decision_Service_Waterfall = "D. Telesales Outbound"
#         Filter_Telesales_Outbound = 1
#     elif (APP_Customer_State in ["", "IMM MOB 00-05"]) and (Filter_Channel_Inbound is not None):
#         APP_Decision_Service_Waterfall = "E. Telesales Inbound"
#         Filter_Telesales_Inbound = 1
#     elif (APP_Customer_State in ["", "IMM MOB 00-05"]) and (Filter_Channel_Online is not None):
#         APP_Decision_Service_Waterfall = "F. World Wide Web"
#         Filter_Web_Service = 1
#     elif APP_Customer_State == "":
#         APP_Decision_Service_Waterfall = "G. First Account Applicant"
#         Filter_First_Account_Applicant = 1
#     elif APP_Customer_State == "IMM MOB 00-05":
#         APP_Decision_Service_Waterfall = "H. Immature Customer State"
#         Filter_Immature_State = 1
#     elif APP_Customer_State == "CLR Clear":
#         APP_Decision_Service_Waterfall = "I. Clear Customer State"
#         Filter_Clear_State = 1
#     elif APP_Customer_State == "RES Responsible":
#         APP_Decision_Service_Waterfall = "J. Responsible Customer State"
#         Filter_Responsible_State = 1
#     elif APP_Customer_State == "ERR Erratic":
#         APP_Decision_Service_Waterfall = "K. Erratic Customer State"
#         Filter_Erratic_State = 1
#     elif APP_Customer_State == "EXT Extended":
#         APP_Decision_Service_Waterfall = "L. Arrears Customer State"
#         Filter_Arrears_State = 1
#     elif APP_Customer_State == "DIS Distressed":
#         APP_Decision_Service_Waterfall = "L. Arrears Customer State"
#         Filter_Arrears_State = 1
#     elif APP_Customer_State == "DBT Doubtful Debt":
#         APP_Decision_Service_Waterfall = "L. Arrears Customer State"
#         Filter_Arrears_State = 1
#     elif APP_Customer_State == "CRD Credit Balance":
#         APP_Decision_Service_Waterfall = "X.01 Catch-All (Credit Balance)"
#         Filter_XXXXXX_State = 1
#     elif APP_Customer_State == "PUP Paid-Up":
#         APP_Decision_Service_Waterfall = "X.02 Catch-All (Paid Up)"
#         Filter_XXXXXX_State = 1
#     else:
#         APP_Decision_Service_Waterfall = "X.03 Catch-All"
#         Filter_XXXXXX_State = 1
#
#     if APP_Subscriptions is not None:
#         if Customer_State == "IMM MOB 00-05":
#             Filter_Immature_Activations = 1
#         elif Filter_Clear_State == 1:
#             Filter_Clear_Activations = 1
#         elif Filter_Responsible_State == 1:
#             Filter_Responsible_Activations = 1
#         elif Filter_Erratic_State == 1:
#             Filter_Erratic_Activations = 1
#         elif Filter_Arrears_State == 1:
#             Filter_Arrears_Activations = 1
#         else:
#             Filter_Other_Activations = 1
#
#     return (APP_Decision_Service_Waterfall, Filter_New_To_Credit, Filter_Telesales_Outbound, Filter_Telesales_Inbound,
#             Filter_Web_Service, Filter_First_Account_Applicant, Filter_XXXXXX_State, Filter_Immature_Activations,
#             Filter_Clear_Activations, Filter_Responsible_Activations, Filter_Erratic_Activations,
#             Filter_Arrears_Activations, Filter_Other_Activations, Filter_Immature_State)
#
#
# schema_Decision_Service_Waterfall = t.StructType([
#     t.StructField("APP_Decision_Service_Waterfall", t.StringType(), True),
#     t.StructField("Filter_New_To_Credit", t.IntegerType(), True),
#     t.StructField("Filter_Telesales_Outbound", t.IntegerType(), True),
#     t.StructField("Filter_Telesales_Inbound", t.IntegerType(), True),
#     t.StructField("Filter_Web_Service", t.IntegerType(), True),
#     t.StructField("Filter_First_Account_Applicant", t.IntegerType(), True),
#     t.StructField("Filter_XXXXXX_State", t.IntegerType(), True),
#     t.StructField("Filter_Immature_Activations", t.IntegerType(), True),
#     t.StructField("Filter_Clear_Activations", t.IntegerType(), True),
#     t.StructField("Filter_Responsible_Activations", t.IntegerType(), True),
#     t.StructField("Filter_Erratic_Activations", t.IntegerType(), True),
#     t.StructField("Filter_Arrears_Activations", t.IntegerType(), True),
#     t.StructField("Filter_Other_Activations", t.IntegerType(), True),
#     t.StructField("Filter_Immature_State", t.IntegerType(), True)
# ])
#
# udf_Decision_Service_Waterfall = f.udf(Decision_Service_Waterfall,
#                                        returnType=schema_Decision_Service_Waterfall)


def Decision_Services_Metrics(sdf_0):
    sdf_1 = sdf_0\
        .withColumn("TMP_Applications", reduce(add, [f.col("Filter_Decision_Outcome_Approved"),
                                                     f.col("Filter_Decision_Outcome_Referred"),
                                                     f.col("Filter_Decision_Outcome_Declined")]))\
        .withColumn("Filter_Decision_Outcome_Approved",
                    f.when(f.col("TMP_Applications").isNotNull(),
                           f.lit(100) * f.col("Filter_Decision_Outcome_Approved") / f.col("TMP_Applications"))
                     .otherwise(f.lit(None)))\
        .withColumn("Filter_Decision_Outcome_Referred",
                    f.when(f.col("TMP_Applications").isNotNull(),
                           f.lit(100) * f.col("Filter_Decision_Outcome_Referred") / f.col("TMP_Applications"))
                     .otherwise(f.lit(None)))\
        .withColumn("Filter_Decision_Outcome_Declined",
                    f.when(f.col("TMP_Applications").isNotNull(),
                           f.lit(100) * f.col("Filter_Decision_Outcome_Declined") / f.col("TMP_Applications"))
                     .otherwise(f.lit(None)))\
        .withColumn("Filter_Approved_With_Activations",
                    f.when(f.col("Filter_Approved_With_Activations").isNotNull(),
                           f.lit(100) * f.col("Filter_Approved_With_Activations") /
                           reduce(add, [f.col("Filter_Approved_With_Activations"),
                                        f.col("Filter_Approved_No_Activations")]))
                     .otherwise(f.lit(None)))\
        .withColumn("Filter_Referred_With_Activations",
                    f.when(f.col("Filter_Referred_With_Activations").isNotNull(),
                           f.lit(100) * f.col("Filter_Referred_With_Activations") /
                           reduce(add, [f.col("Filter_Referred_With_Activations"),
                                        f.col("Filter_Referred_No_Activations")]))
                     .otherwise(f.lit(None)))\
        .withColumn("Filter_Declined_With_Activations",
                    f.when(f.col("Filter_Declined_With_Activations").isNotNull(),
                           f.lit(100) * f.col("Filter_Declined_With_Activations") /
                           reduce(add, [f.col("Filter_Declined_With_Activations"),
                                        f.col("Filter_Declined_No_Activations")]))
                     .otherwise(f.lit(None)))\
        .withColumn("BadRate1M",
                    f.when(((f.col("Goods1") + f.col("Bads1") > 50) & (f.col("APP_Accounts") >= 50)),
                           (f.lit(1000) * f.col("Bads1") / reduce(add, [f.col("Goods1"), f.col("Bads1")]).astype(t.IntegerType())) / f.lit(10))
                     .otherwise(f.lit(None)))\
        .withColumn("BadRate3M",
                    f.when(((f.col("Goods3") + f.col("Bads3") > 50) & (f.col("APP_Accounts") >= 50)),
                           (f.lit(1000) * f.col("Bads3") / reduce(add, [f.col("Goods3"), f.col("Bads3")]).astype(t.IntegerType())) / f.lit(10))
                     .otherwise(f.lit(None)))\
        .withColumn("BadRate6M",
                    f.when(((f.col("Goods6") + f.col("Bads6") > 50) & (f.col("APP_Accounts") >= 50)),
                           (f.lit(1000) * f.col("Bads6") / reduce(add, [f.col("Goods6"), f.col("Bads6")]).astype(t.IntegerType())) / f.lit(10))
                     .otherwise(f.lit(None)))\
        .withColumn("BadRate9M",
                    f.when(((f.col("Goods9") + f.col("Bads9") > 50) & (f.col("APP_Accounts") >= 50)),
                           (f.lit(1000) * f.col("Bads9") / reduce(add, [f.col("Goods9"), f.col("Bads9")]).astype(t.IntegerType())) / f.lit(10))
                     .otherwise(f.lit(None)))\
        .withColumn("BadRate12M",
                    f.when(((f.col("Goods12") + f.col("Bads12") > 50) & (f.col("APP_Accounts") >= 50)),
                           (f.lit(1000) * f.col("Bads12") / reduce(add, [f.col("Goods12"), f.col("Bads12")]).astype(t.IntegerType())) / f.lit(10))
                     .otherwise(f.lit(None)))\
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
        .groupBy(*[f.col("Decision_Services_Segment"), f.col("APP_Decision_Service_Waterfall"),
                   f.col("Decision_Services_Outcome"), f.col(VAR)])\
        .agg(*[f.sum(f.col(x)).alias(x) for x in ls_ds_summary])\
        .withColumnRenamed("FREQ", "Applicants")

    sdf_2 = Decision_Services_Metrics(sdf_1)
    sdf_3 = sdf_2\
        .withColumn("Risk_Grade_Mandate",
                    udf_RiskGrade_Mandate(f.col("Applicants"),
                                          f.col("APP_Accounts"),
                                          f.col("BadRate12M"),
                                          f.col("BadRate9M"),
                                          f.col("BadRate6M")))
    return sdf_3
