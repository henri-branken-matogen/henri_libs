import pyspark.sql.functions as f
import pyspark.sql.types as t


def ECL_Features(SME, CME, sdf_0):
    """
    Generate the Account Level ECL Features.
    """
    sdf_1 = sdf_0\
        .withColumn("ECL_Bad_Debt_Month", f.col("BAD_Month"))\
        .withColumn("ECL_Bad_Debt_Events", f.col("BAD_Events"))\
        .withColumn("ECL_Bad_Debt_Subscriptions", f.col("BAD_Subscriptions"))

    return sdf_1


def Portfolio_Segmentation(Analysis_A, Analysis_B, CRD_NBR, PUP_NBR, Filter_Waterfall):
    Analysis_A = Analysis_A.upper()
    Analysis_B = Analysis_B.upper()

    A_Current_NBR = None  # UTD Account
    A_Collections_NBR = None  # Collections Account
    A_Internal_Allocation_NBR = None  # EDC Internal Allocation
    A_External_Allocation_NBR = None  # EDC External Allocation
    A_Churn_Bad_Debt_NBR = None  # Involuntary Churn (Bad Debt)
    A_Churn_Voluntary_NBR = None  # Voluntary Churn (Attrition)
    if Analysis_A == "CURRENT":
        A_Current_NBR = 1
    elif Analysis_A == "COLLECTION":
        A_Collections_NBR = 1
    elif Analysis_A == "INT UNALLO":
        A_Internal_Allocation_NBR = 1
    elif Analysis_A == "EXT ALLOC":
        A_External_Allocation_NBR = 1
    elif Analysis_A == "PREWRITEOF":
        A_Churn_Bad_Debt_NBR = 1
    elif Analysis_A == "PRECLOSED":
        A_Churn_Voluntary_NBR = 1
    else:
        pass

    B_Churn_Bad_Debt_NBR = None
    B_Churn_Voluntary_NBR = None
    if Analysis_B == "PRE_LEGAL":
        B_Churn_Bad_Debt_NBR = 1
    elif Analysis_B == "ARCHIVED":
        B_Churn_Voluntary_NBR = 1
    else:
        pass

    Segment_TMP = ""
    if (A_Churn_Bad_Debt_NBR == 1) or (B_Churn_Bad_Debt_NBR == 1):
        Segment_TMP = "91 Bad Debt (120-239 Days)"
    elif (A_Churn_Voluntary_NBR == 1) or (B_Churn_Voluntary_NBR == 1):
        Segment_TMP = "92 Voluntary Churn (Attrition)"
    elif (A_Current_NBR == 1) and (CRD_NBR == 1):
        Segment_TMP = "93 Voluntary Churn (Credit Balance)"
    elif (A_Current_NBR == 1) and (PUP_NBR == 1):
        Segment_TMP = "94 Voluntary Churn (Paid Up)"
    else:
        pass

    if Filter_Waterfall == "":
        Filter_Waterfall = Segment_TMP

    return (A_Current_NBR, A_Collections_NBR, A_Internal_Allocation_NBR, A_External_Allocation_NBR,
            A_Churn_Bad_Debt_NBR, A_Churn_Voluntary_NBR, B_Churn_Bad_Debt_NBR, B_Churn_Voluntary_NBR,
            Filter_Waterfall)


schema_Portfolio_Segmentation = t.StructType([
    t.StructField("A_Current_NBR", t.IntegerType(), True),
    t.StructField("A_Collections_NBR", t.IntegerType(), True),
    t.StructField("A_Internal_Allocation_NBR", t.IntegerType(), True),
    t.StructField("A_External_Allocation_NBR", t.IntegerType(), True),
    t.StructField("A_Churn_Bad_Debt_NBR", t.IntegerType(), True),
    t.StructField("A_Churn_Voluntary_NBR", t.IntegerType(), True),
    t.StructField("B_Churn_Bad_Debt_NBR", t.IntegerType(), True),
    t.StructField("B_Churn_Voluntary_NBR", t.IntegerType(), True),
    t.StructField("Filter_Waterfall", t.StringType(), True)
])

udf_Portfolio_Segmentation = f.udf(Portfolio_Segmentation,
                                   returnType=schema_Portfolio_Segmentation)
