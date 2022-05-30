import pyspark.sql.functions as f
import pyspark.sql.types as t
from functools import reduce
from operator import add
from pyspark.sql import DataFrame


def Portfolio_Performance_Summary(sdf, VAR, PER):
    PER = str(PER)[0:4]
    # Step 1:  Summarise the data file either at `Account` or `Customer` View.

    ls_summary_vars = [
        "Records", "Records_DC0", "Records_DC1", "Records_DC2", "Records_DC3", "Records_DC4", "Balance", "Instalment",
        "NextCycle30Days", "SecondCycle60Days", "ThirdCycle90days",
        "Goods1", "Bads1", "Goods3", "Bads3", "Goods6", "Bads6", "Goods9", "Bads9", "Goods12", "Bads12",
        "DoubtfulDebt1M", "DoubtfulDebt3M", "DoubtfulDebt6M", "DoubtfulDebt9M", "DoubtfulDebt12M",
        "DC0_Credit_Paidup", "DC0_UTD", "DC0_Forward_Roll",
        "DC1_Cured", "DC1_30Days", "DC1_Forward_Roll",
        "DC2_Cured", "DC2_Backward_Roll", "DC2_60Days", "DC2_Forward_Roll",
        "DC3_Cured", "DC3_Backward_Roll", "DC3_90Days", "DC3_Forward_Roll",
        "DC4_Backward_Roll", "DC4_120Days"
    ]

    sdf_1 = sdf \
        .groupBy(*[VAR, "YOY_Label"]) \
        .agg(*[f.sum(f.col(x)).alias(x) for x in ls_summary_vars])

    # Step 2:  Update the Metrics
    sdf_2 = sdf_1 \
        .withColumn(f"Balance_{PER}", (f.col("Balance") / f.col("Records")).astype(t.IntegerType())) \
        .withColumn(f"Instalment_{PER}", (f.col("Instalment") / f.col("Records")).astype(t.IntegerType())) \
        .withColumn(f"NextCycle30Days_{PER}",
                    (f.col("NextCycle30Days") / f.col("Records") * f.lit(100)).astype(t.FloatType())) \
        .withColumn(f"SecondCycle60Days_{PER}",
                    (f.col("SecondCycle60Days") / f.col("Records") * f.lit(100)).astype(t.FloatType())) \
        .withColumn(f"ThirdCycle90Days_{PER}",
                    (f.col("ThirdCycle90Days") / f.col("Records") * f.lit(100)).astype(t.FloatType())) \
        .withColumn(f"BadRate1M_{PER}",
                    ((f.col("Bads1") / reduce(add, [f.col("Goods1"), f.col("Bads1")])) * f.lit(100)).astype(
                        t.FloatType())) \
        .withColumn(f"BadRate3M_{PER}",
                    ((f.col("Bads3") / reduce(add, [f.col("Goods3"), f.col("Bads3")])) * f.lit(100)).astype(
                        t.FloatType())) \
        .withColumn(f"BadRate6M_{PER}",
                    ((f.col("Bads6") / reduce(add, [f.col("Goods6"), f.col("Bads6")])) * f.lit(100)).astype(
                        t.FloatType())) \
        .withColumn(f"BadRate9M_{PER}",
                    ((f.col("Bads9") / reduce(add, [f.col("Goods9"), f.col("Bads9")])) * f.lit(100)).astype(
                        t.FloatType())) \
        .withColumn(f"BadRate12M_{PER}",
                    ((f.col("Bads12") / reduce(add, [f.col("Goods12"), f.col("Bads12")])) * f.lit(100)).astype(
                        t.FloatType())) \
        .withColumn(f"DoubtfulDebt1M_{PER}",
                    (f.col("DoubtfulDebt1M") / f.col("Records") * f.lit(100)).astype(t.FloatType())) \
        .withColumn(f"DoubtfulDebt3M_{PER}",
                    (f.col("DoubtfulDebt3M") / f.col("Records") * f.lit(100)).astype(t.FloatType())) \
        .withColumn(f"DoubtfulDebt6M_{PER}",
                    (f.col("DoubtfulDebt6M") / f.col("Records") * f.lit(100)).astype(t.FloatType())) \
        .withColumn(f"DoubtfulDebt9M_{PER}",
                    (f.col("DoubtfulDebt9M") / f.col("Records") * f.lit(100)).astype(t.FloatType())) \
        .withColumn(f"DoubtfulDebt12M_{PER}",
                    (f.col("DoubtfulDebt12M") / f.col("Records") * f.lit(100)).astype(t.FloatType())) \
        .withColumn(f"Records_DC0_{PER}", f.col("Records_DC0").astype(t.IntegerType())) \
        .withColumn(f"Records_DC1_{PER}", f.col("Records_DC1").astype(t.IntegerType())) \
        .withColumn(f"Records_DC2_{PER}", f.col("Records_DC2").astype(t.IntegerType())) \
        .withColumn(f"Records_DC3_{PER}", f.col("Records_DC3").astype(t.IntegerType())) \
        .withColumn(f"Records_DC4_{PER}", f.col("Records_DC4").astype(t.IntegerType())) \
        .withColumn(f"DC0_Credit_Paidup_{PER}",
                    (f.col("DC0_Credit_Paidup") / f.col("Records_DC0") * f.lit(100)).astype(t.FloatType())) \
        .withColumn(f"DC0_UTD_{PER}", (f.col("DC0_UTD") / f.col("Records_DC0") * f.lit(100)).astype(t.FloatType())) \
        .withColumn(f"DC0_Forward_Roll_{PER}",
                    (f.col("DC0_Forward_Roll") / f.col("Records_DC0") * f.lit(100)).astype(t.FloatType())) \
        .withColumn(f"DC1_Cured_{PER}", (f.col("DC1_Cured") / f.col("Records_DC1") * f.lit(100)).astype(t.FloatType())) \
        .withColumn(f"DC1_30Days_{PER}", (f.col("DC1_30Days") / f.col("Records_DC1") * f.lit(100)).astype(t.FloatType())) \
        .withColumn(f"DC1_Forward_Roll_{PER}",
                    (f.col("DC1_Forward_Roll") / f.col("Records_DC1") * f.lit(100)).astype(t.FloatType())) \
        .withColumn(f"DC2_Cured_{PER}", (f.col("DC2_Cured") / f.col("Records_DC2") * f.lit(100)).astype(t.FloatType())) \
        .withColumn(f"DC2_Backward_Roll_{PER}",
                    (f.col("DC2_Backward_Roll") / f.col("Records_DC2") * f.lit(100)).astype(t.FloatType())) \
        .withColumn(f"DC2_60Days_{PER}", (f.col("DC2_60Days") / f.col("Records_DC2") * f.lit(100)).astype(t.FloatType())) \
        .withColumn(f"DC2_Forward_Roll_{PER}",
                    (f.col("DC2_Forward_Roll") / f.col("Records_DC2") * f.lit(100)).astype(t.FloatType())) \
        .withColumn(f"DC3_Cured_{PER}", (f.col("DC3_Cured") / f.col("Records_DC3") * f.lit(100)).astype(t.FloatType())) \
        .withColumn(f"DC3_Backward_Roll_{PER}",
                    (f.col("DC3_Backward_Roll") / f.col("Records_DC3") * f.lit(100)).astype(t.FloatType())) \
        .withColumn(f"DC3_90Days_{PER}", (f.col("DC3_90Days") / f.col("Records_DC3") * f.lit(100)).astype(t.FloatType())) \
        .withColumn(f"DC3_Forward_Roll_{PER}",
                    (f.col("DC3_Forward_Roll") / f.col("Records_DC3") * f.lit(100)).astype(t.FloatType())) \
        .withColumn(f"DC4_Backward_Roll_{PER}",
                    (f.col("DC4_Backward_Roll") / f.col("Records_DC4") * f.lit(100)).astype(t.FloatType())) \
        .withColumn(f"DC4_120Days_{PER}", (f.col("DC4_120Days") / f.col("Records_DC4") * f.lit(100)).astype(t.FloatType()))

    sdf_3 = sdf_2\
        .drop(*ls_summary_vars)
    return sdf_3


def create_temporary_data_file(sdf, DIM, YOY, ACC):
    """
    DIM = Dimension = Account or Customer
    FIL = Source file = ACC if Account Dimension, IDX if Customer Dimension
    MTH = the month view
    YOY = The Year on Year labels
    ACC = Number of accounts counter to be used
    AGE = Generate Account Delinquency Metrics and Transition metrics (roll rates using the account ageing field)
    """
    DIM_State = DIM + "_State"
    YOY_DIM = "YOY_" + DIM

    ls_keep = [
        "Balance", "Instalment", ACC, "Account_M00", "Account_New", "Account_AGE", DIM_State,
        "NextCycle30Days", "SecondCycle60Days", "ThirdCycle90Days",
        "Goods1", "Bads1", "Goods3", "Bads3", "Goods6", "Bads6", "Goods9", "Bads9", "Goods12", "Bads12",
        "DoubtfulDebt1M", "DoubtfulDebt3M", "DoubtfulDebt6M", "DoubtfulDebt9M", "DoubtfulDebt12M",
        "TRANSITION_1_AGE"
    ]

    ls_KEEP = [x.upper() for x in ls_keep]

    ls_sdf_cols = list(sdf.columns)

    ls_remain = [c for c in ls_sdf_cols if c.upper() in ls_KEEP]

    sdf_1 = sdf \
        .select(*ls_remain)

    sdf_2 = sdf_1 \
        .withColumn("Records", f.col(ACC)) \
        .withColumn("YOY_Label", f.lit(YOY)) \
        .withColumn("YOY_Portfolio", f.lit("99. No Data")) \
        .withColumn(YOY_DIM, f.lit("99. No Data")) \
        .withColumn("YOY_Aging", f.lit("99. No Data"))
    return sdf_2


def primary_portfolio_segmentation(Account_M00, Account_New, DIM_STATE):
    """
    :param Account_M00: An Account that has been opened in the latest month such that the Months on Book = 0.
    :param Account_New: A New Account that is still immature.  In other words, the Months on Book fall within
    [0, 1, 2, 3, 4, 5].
    :param DIM_STATE: An Account feature we analyse in the code below.  We engineer other (binary) fields from this
    Account feature.
    :return: Based on the waterfall logic in this function, we assign values to `YOY_DIM_val` and `YOY_Portfolio_val`
    and then return these.  They are of type String.
    """
    if Account_M00 is not None:
        if Account_M00 >= 1:
            YOY_DIM_val = "01. New (MOB=00)"
            YOY_Portfolio_val = "00. Active (01 to 07)"
            return YOY_DIM_val, YOY_Portfolio_val
    elif Account_New is not None:
        if Account_New >= 1:
            YOY_DIM_val = "02. Immature (MOB=01-05)"
            YOY_Portfolio_val = "00. Active (01 to 07)"
            return YOY_DIM_val, YOY_Portfolio_val
    elif "CLR" in DIM_STATE.upper():
        YOY_DIM_val = "03. Clear Behaviour"
        YOY_Portfolio_val = "00. Active (01 to 07)"
        return YOY_DIM_val, YOY_Portfolio_val
    elif "RES" in DIM_STATE.upper():
        YOY_DIM_val = "04. Responsible Behaviour"
        YOY_Portfolio_val = "00. Active (01 to 07)"
        return YOY_DIM_val, YOY_Portfolio_val
    elif "ERR" in DIM_STATE.upper():
        YOY_DIM_val = "05. Erratic Behaviour"
        YOY_Portfolio_val = "00. Active (01 to 07)"
        return YOY_DIM_val, YOY_Portfolio_val
    elif "EXT" in DIM_STATE.upper():
        YOY_DIM_val = "06. Extended Behaviour"
        YOY_Portfolio_val = "00. Active (01 to 07)"
        return YOY_DIM_val, YOY_Portfolio_val
    elif "DIS" in DIM_STATE.upper():
        YOY_DIM_val = "07. Distressed Behaviour"
        YOY_Portfolio_val = "00. Active (01 to 07)"
        return YOY_DIM_val, YOY_Portfolio_val
    elif "CRD" in DIM_STATE.upper():
        YOY_DIM_val = "80. Voluntary Churn"
        YOY_Portfolio_val = "99. Churned (80 to 90)"
        return YOY_DIM_val, YOY_Portfolio_val
    elif "C01" in DIM_STATE.upper():
        YOY_DIM_val = "80. Voluntary Churn"
        YOY_Portfolio_val = "99. Churned (80 to 90)"
        return YOY_DIM_val, YOY_Portfolio_val
    elif "PUP" in DIM_STATE.upper():
        YOY_DIM_val = "80. Voluntary Churn"
        YOY_Portfolio_val = "99. Churned (80 to 90)"
        return YOY_DIM_val, YOY_Portfolio_val
    elif "P01" in DIM_STATE.upper():
        YOY_DIM_val = "80. Voluntary Churn"
        YOY_Portfolio_val = "99. Churned (80 to 90)"
        return YOY_DIM_val, YOY_Portfolio_val
    elif "P03" in DIM_STATE.upper():
        YOY_DIM_val = "80. Voluntary Churn"
        YOY_Portfolio_val = "99. Churned (80 to 90)"
        return YOY_DIM_val, YOY_Portfolio_val
    elif "DBT" in DIM_STATE.upper():
        YOY_DIM_val = "90. Involuntary Churn"
        YOY_Portfolio_val = "99. Churned (80 to 90)"
        return YOY_DIM_val, YOY_Portfolio_val
    elif "EXC" in DIM_STATE.upper():
        YOY_DIM_val = "90. Involuntary Churn"
        YOY_Portfolio_val = "99. Churned (80 to 90)"
        return YOY_DIM_val, YOY_Portfolio_val
    else:
        YOY_DIM_val = ""
        YOY_Portfolio_val = ""
        return YOY_DIM_val, YOY_Portfolio_val


schema_primary_portfolio_segmentation = t.StructType([
    t.StructField("YOY_DIM", t.StringType(), True),
    t.StructField("YOY_Portfolio", t.StringType(), True)
])

udf_primary_portfolio_segmentation = f.udf(primary_portfolio_segmentation,
                                           returnType=schema_primary_portfolio_segmentation)


def unpack_primary_portfolio_segmentation(sdf, DIM):
    """
    Set the primary portfolio segmentation based on whether new, immature, or establish account/customer state.
    """
    YOY_DIM = "YOY_" + DIM
    DIM_STATE = DIM + "_State"
    sdf_2 = sdf\
        .withColumn("nest",
                    udf_primary_portfolio_segmentation(f.col("Account_M00"),
                                                       f.col("Account_New"),
                                                       f.col(DIM_STATE)))\
        .withColumn(YOY_DIM, f.col("nest.YOY_DIM"))\
        .withColumn("YOY_Portfolio", f.col("nest.YOY_Portfolio"))\
        .drop(*["nest"])
    return sdf_2


def segment_by_aging(sdf, ACC, AGE):
    """
    Set the account aging segmentation and create additional account-only metrics.
    Segment the portfolio by the aging bucket, and record the number of accounts in each bucket.
    """
    if AGE.upper() == "Y":
        sdf_2 = sdf \
            .withColumn("Records_DC0", f.when(f.col("Account_AGE") == "0", f.col(ACC))
                                        .otherwise(f.lit(None))) \
            .withColumn("Records_DC1", f.when(f.col("Account_AGE") == "1", f.col(ACC))
                                        .otherwise(f.lit(None))) \
            .withColumn("Records_DC2", f.when(f.col("Account_AGE") == "2", f.col(ACC))
                                        .otherwise(f.lit(None))) \
            .withColumn("Records_DC3", f.when(f.col("Account_AGE") == "3", f.col(ACC))
                                        .otherwise(f.lit(None))) \
            .withColumn("Records_DC4", f.when(f.col("Account_AGE") == "4", f.col(ACC))
                                        .otherwise(f.lit(None)))

        sdf_3 = sdf_2 \
            .withColumn("YOY_Aging", f.when(f.col("Account_AGE") == "0", f.lit("10. UTD Accounts"))
                        .when(f.col("Account_AGE") == "1", f.lit("11. 30 Day Accounts"))
                        .when(f.col("Account_AGE") == "2", f.lit("12. 60 Day Accounts"))
                        .when(f.col("Account_AGE") == "3", f.lit("13. 90 Day Accounts"))
                        .when(f.col("Account_AGE") == "4", f.lit("14. 120+ Day Accounts"))
                        .when(f.col("Account_AGE") == "C", f.lit("60. Voluntary Churn (Aging)"))
                        .when(f.col("Account_AGE") == "P", f.lit("60. Voluntary Churn (Aging)"))
                        .otherwise(f.lit("70. Exclusions (Aging)")))
        return sdf_3
    else:
        sdf_2 = sdf \
            .withColumn("Records_DC0", f.lit(None)) \
            .withColumn("Records_DC1", f.lit(None)) \
            .withColumn("Records_DC2", f.lit(None)) \
            .withColumn("Records_DC3", f.lit(None)) \
            .withColumn("Records_DC4", f.lit(None)) \
            .withColumn("YOY_Aging", f.lit(None))
        return sdf_2


def create_roll_rate_metrics(sdf, AGE):
    """
    Create account aging transition metrics, i.e. roll rate metrics.
    """
    if AGE.upper() == "Y":
        sdf_2 = sdf\
            .withColumn("DC0_Credit_Paidup",
                        f.when(f.regexp_replace(f.upper(f.col("Transition_1_AGE")), " ", "") == "0(+)UTD:CREDITBALANCEORPAID-UPNEXTPERIOD",
                               f.lit(1))\
                         .otherwise(f.lit(None)))\
            .withColumn("DC0_UTD",
                        f.when(f.regexp_replace(f.upper(f.col("Transition_1_AGE")), " ", "") == "0(=)UTDBOTHPERIODS",
                               f.lit(1))\
                         .otherwise(f.lit(None)))\
            .withColumn("DC0_Forward_Roll",
                        f.when(f.regexp_replace(f.upper(f.col("Transition_1_AGE")), " ", "") == "0(-)UTD:ROLLEDFORWARDNEXTPERIOD",
                               f.lit(1))\
                         .otherwise(f.lit(None)))\
            .withColumn("DC1_Cured",
                        f.when(f.regexp_replace(f.upper(f.col("Transition_1_AGE")), " ", "") == "1(+)30DAYS:CUREDORCREDITBALANCEORPAID-UPNEXTPERIOD",
                               f.lit(1))\
                         .otherwise(f.lit(None)))\
            .withColumn("DC1_30Days",
                        f.when(f.regexp_replace(f.upper(f.col("Transition_1_AGE")), " ", "") == "1(=)30DAYSBOTHPERIODS",
                               f.lit(1))\
                         .otherwise(f.lit(None)))\
            .withColumn("DC1_Forward_Roll",
                        f.when(f.regexp_replace(f.upper(f.col("Transition_1_AGE")), " ", "") == "1(-)30DAYS:ROLLEDFORWARDNEXTPERIOD",
                               f.lit(1))\
                         .otherwise(f.lit(None)))\
            .withColumn("DC2_Cured",
                        f.when(f.regexp_replace(f.upper(f.col("Transition_1_AGE")), " ", "") == "2(+)60DAYS:CUREDORCREDITBALANCENEXTPERIOD",
                               f.lit(1))\
                         .otherwise(f.lit(None)))\
            .withColumn("DC2_Backward_Roll",
                        f.when(f.regexp_replace(f.upper(f.col("Transition_1_AGE")), " ", "") == "2(+)60DAYS:ROLLEDBACKWARDORPAID-UPNEXTPERIOD",
                               f.lit(1))\
                         .otherwise(f.lit(None)))\
            .withColumn("DC2_60Days",
                        f.when(f.regexp_replace(f.upper(f.col("Transition_1_AGE")), " ", "") == "2(=)60DAYSBOTHPERIODS",
                               f.lit(1))\
                         .otherwise(f.lit(None)))\
            .withColumn("DC2_Forward_Roll",
                        f.when(f.regexp_replace(f.upper(f.col("Transition_1_AGE")), " ", "") == "2(-)60DAYS:ROLLEDFORWARDNEXTPERIOD",
                               f.lit(1))\
                         .otherwise(f.lit(None)))\
            .withColumn("DC3_Cured",
                        f.when(f.regexp_replace(f.upper(f.col("Transition_1_AGE")), " ", "") == "3(+)90DAYS:CUREDORCREDITBALANCENEXTPERIOD",
                               f.lit(1))\
                         .otherwise(f.lit(None)))\
            .withColumn("DC3_Backward_Roll",
                        f.when(f.regexp_replace(f.upper(f.col("Transition_1_AGE")), " ", "") == "3(+)90DAYS:ROLLEDBACKWARDORPAID-UPNEXTPERIOD",
                               f.lit(1))\
                         .otherwise(f.lit(None)))\
            .withColumn("DC3_90Days",
                        f.when(f.regexp_replace(f.upper(f.col("Transition_1_AGE")), " ", "") == "3(=)90DAYSBOTHPERIODS",
                               f.lit(1))\
                         .otherwise(f.lit(None)))\
            .withColumn("DC3_Forward_Roll",
                        f.when(f.regexp_replace(f.upper(f.col("Transition_1_AGE")), " ", "") == "3(-)90DAYS:ROLLEDFORWARDNEXTPERIOD",
                               f.lit(1))\
                         .otherwise(f.lit(None)))\
            .withColumn("DC4_Backward_Roll",
                        f.when(f.regexp_replace(f.upper(f.col("Transition_1_AGE")), " ", "") == "4(+)120DAYS:ROLLEDBACKWARDORCREDITBALANCEORPAID-UPNEXTPERIOD",
                               f.lit(1))\
                         .otherwise(f.lit(None)))\
            .withColumn("DC4_120Days",
                        f.when(f.regexp_replace(f.upper(f.col("Transition_1_AGE")), " ", "") == "4(=)120DAYSBOTHPERIODS",
                               f.lit(1))\
                         .otherwise(f.lit(None)))
        return sdf_2
    else:
        sdf_2 = sdf\
            .withColumn("DC0_Credit_Paidup", f.lit(None))\
            .withColumn("DC0_UTD", f.lit(None))\
            .withColumn("DC0_Forward_Roll", f.lit(None))\
            .withColumn("DC1_Cured", f.lit(None))\
            .withColumn("DC1_30Days", f.lit(None))\
            .withColumn("DC1_Forward_Roll", f.lit(None))\
            .withColumn("DC2_Cured", f.lit(None))\
            .withColumn("DC2_Backward_Roll", f.lit(None))\
            .withColumn("DC2_60Days", f.lit(None))\
            .withColumn("DC2_Forward_Roll", f.lit(None))\
            .withColumn("DC3_Cured", f.lit(None))\
            .withColumn("DC3_Backward_Roll", f.lit(None))\
            .withColumn("DC3_90Days", f.lit(None))\
            .withColumn("DC3_Forward_Roll", f.lit(None))\
            .withColumn("DC4_Backward_Roll", f.lit(None))\
            .withColumn("DC4_120Days", f.lit(None))
        return sdf_2


def summarise_and_combine(sdf, DIM, PER):
    YOY_DIM = "YOY_" + DIM
    # Step 2: Summarise at the granular account/customer view.
    sdf_yoy_dim = Portfolio_Performance_Summary(sdf, YOY_DIM, PER)

    # Step 3: Summarise at the account aging view.
    sdf_yoy_aging = Portfolio_Performance_Summary(sdf, "YOY_Aging", PER)\
        .withColumnRenamed("YOY_Aging", YOY_DIM)

    # Step 4: Summarise at the portfolio view.
    sdf_yoy_portfolio = Portfolio_Performance_Summary(sdf, "YOY_Portfolio", PER)\
        .withColumnRenamed("YOY_Portfolio", YOY_DIM)

    # Step 5: Combine the three summary files and sort.
    ls_cols = list(sdf_yoy_dim.columns)
    ls_sdfs = [sdf.select(*ls_cols) for sdf in [sdf_yoy_dim, sdf_yoy_aging, sdf_yoy_portfolio]]
    sdf_1 = reduce(DataFrame.unionAll, ls_sdfs)

    sdf_2 = sdf_1 \
        .orderBy(*[f.col(YOY_DIM)])
    return sdf_2


def Portfolio_Performance_Period(sdf, DIM, YOY, ACC, AGE, PER):
    sdf_1 = create_temporary_data_file(sdf, DIM, YOY, ACC)
    sdf_2 = unpack_primary_portfolio_segmentation(sdf_1, DIM)
    sdf_3 = segment_by_aging(sdf_2, ACC, AGE)
    sdf_4 = create_roll_rate_metrics(sdf_3, AGE)
    sdf_5 = summarise_and_combine(sdf_4, DIM, PER)
    return sdf_5
