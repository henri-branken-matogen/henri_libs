import pyspark.sql.functions as f
import pyspark.sql.types as t


def primary_portfolio_segmentation(Account_M00, Account_New, DIM_STATE):
    """
    :param Account_M00: An Account that has been opened in the latest month such that the Months on Book = 0.
    :param Account_NEW: A New Account that is still immature.  In other words, the Months on Book fall within
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
