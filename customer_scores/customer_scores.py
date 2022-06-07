import math
import pyspark.sql.functions as f
import pyspark.sql.types as t


def CampaignCustomerScore(ALL_Num0Delq1Year, ALL_PercPayments2Years, ALL_MaxDelq180DaysLT24M, AIL_AvgMonthsOnBook,
                          UNN_PercTradesUtilisedLT100MR60, ALL_MaxDelqEver, ALL_Perc0Delq90Days, COM_NumTrades2Years,
                          PDO=20, CONSTANT_EST=582):
    CAM1 = CAM2 = CAM3 = CAM4 = CAM5 = CAM6 = CAM7 = CAM8 = None  # Initialise the `CAM<x>` variables.
    """
    `CAM1` calculation.  Derived from `ALL_Num0Delq1Year`.
    """
    if ALL_Num0Delq1Year is None:
        CAM1 = 73
    elif ALL_Num0Delq1Year < -6:
        CAM1 = 0
    elif ALL_Num0Delq1Year < 0:
        CAM1 = 53
    elif ALL_Num0Delq1Year < 10:
        CAM1 = 53
    elif ALL_Num0Delq1Year < 20:
        CAM1 = 59
    elif ALL_Num0Delq1Year < 30:
        CAM1 = 67
    elif ALL_Num0Delq1Year < 40:
        CAM1 = 73
    elif ALL_Num0Delq1Year < 65:
        CAM1 = 83
    elif ALL_Num0Delq1Year < 100:
        CAM1 = 100
    elif ALL_Num0Delq1Year >= 100:
        CAM1 = 112
    else:
        pass

    """
    `CAM2` calculation.  Derived from `ALL_PercPayments2Years`.
    """
    if ALL_PercPayments2Years is None:
        CAM2 = 73
    elif ALL_PercPayments2Years < -6:
        CAM2 = 0
    elif ALL_PercPayments2Years < 0:
        CAM2 = 51
    elif ALL_PercPayments2Years < 30:
        CAM2 = 51
    elif ALL_PercPayments2Years < 55:
        CAM2 = 58
    elif ALL_PercPayments2Years < 65:
        CAM2 = 64
    elif ALL_PercPayments2Years < 75:
        CAM2 = 68
    elif ALL_PercPayments2Years < 85:
        CAM2 = 74
    elif ALL_PercPayments2Years >= 85:
        CAM2 = 80
    else:
        pass

    """
    `CAM3` calculation.  Derived from `ALL_MaxDelq180DaysLT24M`.
    """
    try:
        ALL_MaxDelq180DaysLT24M = ALL_MaxDelq180DaysLT24M.replace(" ", "").upper()
    except Exception as e:
        print(str(e))
        return None
    if ALL_MaxDelq180DaysLT24M == "":
        CAM3 = 73
    elif ALL_MaxDelq180DaysLT24M in ["!", "$", "-", "="]:
        CAM3 = 0
    elif ALL_MaxDelq180DaysLT24M in [".", "-1", "?", "@"]:
        CAM3 = 72
    elif ALL_MaxDelq180DaysLT24M == "0":
        CAM3 = 76
    elif ALL_MaxDelq180DaysLT24M == "1":
        CAM3 = 65
    elif ALL_MaxDelq180DaysLT24M == "2":
        CAM3 = 58
    elif ALL_MaxDelq180DaysLT24M in ["3", "4", "5"]:
        CAM3 = 54  # Corrected 29/07/2021.  Was 58 points.
    elif ALL_MaxDelq180DaysLT24M in ["6", "7", "8"]:
        CAM3 = 54  # corrected 29/07/2021.
    elif ALL_MaxDelq180DaysLT24M in ["9", "I", "J", "L", "W"]:
        CAM3 = 54  # corrected 29/07/2021.
    else:
        pass

    """
    `CAM4` calculation.  Derived from `AIL_AvgMonthsOnBook`.
    """
    if AIL_AvgMonthsOnBook is None:
        CAM4 = 73
    elif AIL_AvgMonthsOnBook < -6:
        CAM4 = 0
    elif AIL_AvgMonthsOnBook < 0:
        CAM4 = 68
    elif AIL_AvgMonthsOnBook < 6:
        CAM4 = 65
    elif AIL_AvgMonthsOnBook < 18:
        CAM4 = 72
    elif AIL_AvgMonthsOnBook < 60:
        CAM4 = 79
    elif AIL_AvgMonthsOnBook >= 60:
        CAM4 = 86
    else:
        pass

    """
    `CAM5` calculation.  Derived from `UNN_PercTradesUtilisedLT100MR60`.
    """
    if UNN_PercTradesUtilisedLT100MR60 is None:
        CAM5 = 73
    elif UNN_PercTradesUtilisedLT100MR60 < -6:
        CAM5 = 0
    elif UNN_PercTradesUtilisedLT100MR60 < 0:
        CAM5 = 65
    elif UNN_PercTradesUtilisedLT100MR60 < 55:
        CAM5 = 69
    elif UNN_PercTradesUtilisedLT100MR60 >= 55:
        CAM5 = 74
    else:
        pass

    """
    `CAM6` calculation.  Derived from `ALL_MaxDelqEver`.
    """
    try:
        ALL_MaxDelqEver = ALL_MaxDelqEver.replace(" ", "").upper()
    except Exception as e:
        print(e)
        return None
    if ALL_MaxDelqEver == "":
        CAM6 = 73
    elif ALL_MaxDelqEver in ["!", "$", "-", "="]:
        CAM6 = 0
    elif ALL_MaxDelqEver in [".", "-1", "?", "@"]:
        CAM6 = 73
    elif ALL_MaxDelqEver in ["0", "1", "2"]:
        CAM6 = 73
    elif ALL_MaxDelqEver in ["3", "4", "5"]:
        CAM6 = 73
    elif ALL_MaxDelqEver in ["6", "7", "8"]:
        CAM6 = 70
    elif ALL_MaxDelqEver in ["9", "I", "J", "L", "W"]:
        CAM6 = 66
    else:
        pass

    """
    `CAM7` calculation.  Derived from `ALL_Perc0Delq90Days`.
    """
    if ALL_Perc0Delq90Days is None:
        CAM7 = 73
    elif ALL_Perc0Delq90Days < -6:
        CAM7 = 0
    elif ALL_Perc0Delq90Days < 0:
        CAM7 = 64
    elif ALL_Perc0Delq90Days < 55:
        CAM7 = 64
    elif ALL_Perc0Delq90Days < 70:
        CAM7 = 67
    elif ALL_Perc0Delq90Days < 85:
        CAM7 = 70
    elif ALL_Perc0Delq90Days >= 85:
        CAM7 = 73
    else:
        pass

    """
    `CAM8` calculation.  Derived from `COM_NumTrades2Years`.
    """
    if COM_NumTrades2Years is None:
        CAM8 = 73
    elif COM_NumTrades2Years < -6:
        CAM8 = 0
    elif COM_NumTrades2Years < 0:
        CAM8 = 115
    elif COM_NumTrades2Years < 1:
        CAM8 = 115
    elif COM_NumTrades2Years < 2:
        CAM8 = 82
    elif COM_NumTrades2Years >= 2:
        CAM8 = 63
    else:
        pass

    ls_CAM = [CAM1, CAM2, CAM3, CAM4, CAM5, CAM6, CAM7, CAM8]
    ls_CAM_eval = [type(x) == int for x in ls_CAM]
    if all(ls_CAM_eval):
        CAM_CustomerScore = sum(ls_CAM)
    else:
        ls_CAM_norm = [0 if x is None else x for x in ls_CAM]
        CAM_CustomerScore = sum(ls_CAM_norm)

    CAM_CustomerScore_LNODDS = (CAM_CustomerScore - CONSTANT_EST) / (PDO / math.log(2))
    CAM_CustomerScore_PROB = math.exp(CAM_CustomerScore_LNODDS) / (1 + math.exp(CAM_CustomerScore_LNODDS))

    return CAM_CustomerScore, CAM_CustomerScore_LNODDS, CAM_CustomerScore_PROB


schema_CampaignCustomerScore = t.StructType([
    t.StructField("CAM_CustomerScore", t.IntegerType(), True),
    t.StructField("CAM_CustomerScore_LNODDS", t.DoubleType(), True),
    t.StructField("CAM_CustomerScore_PROB", t.DoubleType(), True)
])


udf_CampaignCustomerScore = f.udf(CampaignCustomerScore, returnType=schema_CampaignCustomerScore)


def EstablishedCustomerScore(ALL_Perc0Delq90Days, ALL_NumTrades180Days, ALL_Num0Delq1Year, UNS_MaxDelq1YearLT24M,
                             ALL_PercPayments2Years, RCG_NumTradesUtilisedLT10, UNN_PercTradesUtilisedLT100MR60,
                             ALL_AvgMonthsOnBook, PDO=20, CONSTANT_EST=582):
    EST1 = EST2 = EST3 = EST4 = EST5 = EST6 = EST7 = EST8 = None  # Initialise the `EST<x>` variables.

    """
    `EST1` Calculation.  Derived from `ALL_Perc0Delq90Days`.
    """
    if ALL_Perc0Delq90Days is None:
        EST1 = 73
    elif ALL_Perc0Delq90Days < -6:
        EST1 = 0
    elif ALL_Perc0Delq90Days < 0:
        EST1 = 52
    elif ALL_Perc0Delq90Days < 50:
        EST1 = 52
    elif ALL_Perc0Delq90Days < 70:
        EST1 = 60
    elif ALL_Perc0Delq90Days < 80:
        EST1 = 67
    elif ALL_Perc0Delq90Days < 85:
        EST1 = 70
    elif ALL_Perc0Delq90Days < 90:
        EST1 = 75
    elif ALL_Perc0Delq90Days < 95:
        EST1 = 80
    elif ALL_Perc0Delq90Days >= 95:
        EST1 = 91
    else:
        pass

    """
    `EST2` Calculation.  Derived from `ALL_Perc0Delq90Days`.
    """
    if ALL_NumTrades180Days is None:
        EST2 = 73
    elif ALL_NumTrades180Days < -6:
        EST2 = 0
    elif ALL_NumTrades180Days < 0:
        EST2 = 86
    elif ALL_NumTrades180Days < 1:
        EST2 = 86
    elif ALL_NumTrades180Days < 2:
        EST2 = 80
    elif ALL_NumTrades180Days < 3:
        EST2 = 70
    elif ALL_NumTrades180Days < 4:
        EST2 = 61
    elif ALL_NumTrades180Days < 5:
        EST2 = 53
    elif ALL_NumTrades180Days >= 5:
        EST2 = 44
    else:
        pass

    """
    `EST3` Calculation.  Derived from `ALL_Perc0Delq90Days`.
    """
    if ALL_Num0Delq1Year is None:
        EST3 = 73
    elif ALL_Num0Delq1Year <= -6:
        EST3 = 0
    elif ALL_Num0Delq1Year < 0:
        EST3 = 54
    elif ALL_Num0Delq1Year < 20:
        EST3 = 54
    elif ALL_Num0Delq1Year < 35:
        EST3 = 65
    elif ALL_Num0Delq1Year < 45:
        EST3 = 71
    elif ALL_Num0Delq1Year < 65:
        EST3 = 78
    elif ALL_Num0Delq1Year < 80:
        EST3 = 84
    elif ALL_Num0Delq1Year >= 80:
        EST3 = 93
    else:
        pass

    """
    `EST4` Calculation.  Derived from `ALL_Perc0Delq90Days`.
    """
    try:
        UNS_MaxDelq1YearLT24M = UNS_MaxDelq1YearLT24M.replace(" ", "").upper()
    except Exception as e:
        print(str(e))
        return None
    if UNS_MaxDelq1YearLT24M == "":
        EST4 =73
    elif UNS_MaxDelq1YearLT24M in ["!", "$", "-", "="]:
        EST4 = 0
    elif UNS_MaxDelq1YearLT24M in [".", "-1", "?", "@"]:
        EST4 = 89
    elif UNS_MaxDelq1YearLT24M == "0":
        EST4 = 85
    elif UNS_MaxDelq1YearLT24M in ["1", "2"]:
        EST4 = 67
    elif UNS_MaxDelq1YearLT24M in ["3", "4", "5"]:
        EST4 = 58
    elif UNS_MaxDelq1YearLT24M in ["6", "7", "8"]:
        EST4 = 58
    elif UNS_MaxDelq1YearLT24M in ["9", "I", "J", "L", "W"]:
        EST4 = 58
    else:
        pass

    """
    `EST5` Calculation.  Derived from `ALL_Perc0Delq90Days`.
    """
    if ALL_PercPayments2Years is None:
        EST5 = 73
    elif ALL_PercPayments2Years < -6:
        EST5 = 0
    elif ALL_PercPayments2Years < 0:
        EST5 = 69
    elif ALL_PercPayments2Years < 65:
        EST5 = 69
    elif ALL_PercPayments2Years < 70:
        EST5 = 72
    elif ALL_PercPayments2Years < 75:
        EST5 = 74
    elif ALL_PercPayments2Years < 80:
        EST5 = 76
    elif ALL_PercPayments2Years < 85:
        EST5 = 78
    elif ALL_PercPayments2Years < 90:
        EST5 = 81
    elif ALL_PercPayments2Years < 95:
        EST5 = 86
    elif ALL_PercPayments2Years >= 95:
        EST5 = 88
    else:
        pass

    """
    `EST6` Calculation.  Derived from `ALL_Perc0Delq90Days`.
    """
    if RCG_NumTradesUtilisedLT10 is None:
        EST6 = 73
    elif RCG_NumTradesUtilisedLT10 < -6:
        EST6 = 0
    elif RCG_NumTradesUtilisedLT10 < 0:
        EST6 = 73
    elif RCG_NumTradesUtilisedLT10 < 1:
        EST6 = 75
    elif RCG_NumTradesUtilisedLT10 < 2:
        EST6 = 87
    elif RCG_NumTradesUtilisedLT10 >= 2:
        EST6 = 96
    else:
        pass

    """
    `EST7` Calculation.  Derived from `ALL_Perc0Delq90Days`.
    """
    if UNN_PercTradesUtilisedLT100MR60 is None:
        EST7 = 73
    elif UNN_PercTradesUtilisedLT100MR60 < -6:
        EST7 = 0
    elif UNN_PercTradesUtilisedLT100MR60 < 0:
        EST7 = 77
    elif UNN_PercTradesUtilisedLT100MR60 < 2:
        EST7 = 77
    elif UNN_PercTradesUtilisedLT100MR60 < 30:
        EST7 = 73
    elif UNN_PercTradesUtilisedLT100MR60 < 50:
        EST7 = 75
    elif UNN_PercTradesUtilisedLT100MR60 < 65:
        EST7 = 78
    elif UNN_PercTradesUtilisedLT100MR60 < 80:
        EST7 = 79
    elif UNN_PercTradesUtilisedLT100MR60 >= 80:
        EST7 = 81
    else:
        pass

    """
    `EST8` Calculation.  Derived from `ALL_Perc0Delq90Days`.
    """
    if ALL_AvgMonthsOnBook is None:
        EST8 = 73
    elif ALL_AvgMonthsOnBook < -6:
        EST8 = 0
    elif ALL_AvgMonthsOnBook < 0:
        EST8 = 52
    elif ALL_AvgMonthsOnBook < 18:
        EST8 = 52
    elif ALL_AvgMonthsOnBook < 24:
        EST8 = 59
    elif ALL_AvgMonthsOnBook < 30:
        EST8 = 62
    elif ALL_AvgMonthsOnBook < 36:
        EST8 = 67
    elif ALL_AvgMonthsOnBook < 42:
        EST8 = 71
    elif ALL_AvgMonthsOnBook < 54:
        EST8 = 75
    elif ALL_AvgMonthsOnBook < 72:
        EST8 = 83
    elif ALL_AvgMonthsOnBook < 96:
        EST8 = 90
    elif ALL_AvgMonthsOnBook >= 96:
        EST8 = 99
    else:
        pass

    ls_EST = [EST1, EST2, EST3, EST4, EST5, EST6, EST7, EST8]
    ls_EST_eval = [type(x) == int for x in ls_EST]
    if all(ls_EST_eval):
        EST_CustomerScore = sum(ls_EST)
    else:
        ls_EST_norm = [0 if x is None else x for x in ls_EST]
        EST_CustomerScore = sum(ls_EST_norm)

    EST_CustomerScore_LNODDS = (EST_CustomerScore - CONSTANT_EST) / (PDO / math.log(2))
    EST_CustomerScore_PROB = math.exp(EST_CustomerScore_LNODDS) / (1 + math.exp(EST_CustomerScore_LNODDS))
    return EST_CustomerScore, EST_CustomerScore_LNODDS, EST_CustomerScore_PROB


schema_EstablishedCustomerScore = t.StructType([
    t.StructField("EST_CustomerScore", t.IntegerType(), True),
    t.StructField("EST_CustomerScore_LNODDS", t.DoubleType(), True),
    t.StructField("EST_CustomerScore_PROB", t.DoubleType(), True)
])


udf_EstablishedCustomerScore = f.udf(EstablishedCustomerScore, returnType=schema_EstablishedCustomerScore)


def NewCustomerScore(ALL_Num0Delq1Year, ALL_PercPayments2Years, ALL_AvgMonthsOnBook, ALL_MaxDelq180DaysLT24M,
                     ALL_NumTrades180Days, RCG_NumTradesUtilisedLT10, ALL_Perc0Delq90Days, ALL_MaxDelqEver,
                     PDO=20, CONSTANT_NEW=582):
    NEW1 = NEW2 = NEW3 = NEW4 = NEW5 = NEW6 = NEW7 = NEW8 = None  # Initialise the `NEW<x>` variables.

    """
    `NEW1` calculation.  Derived from `ALL_Num0Delq1Year`.
    """
    if ALL_Num0Delq1Year is None:
        NEW1 = 73
    elif ALL_Num0Delq1Year < -6:
        NEW1 = 0
    elif ALL_Num0Delq1Year < 0:
        NEW1 = 54
    elif ALL_Num0Delq1Year < 5:
        NEW1 = 54
    elif ALL_Num0Delq1Year < 10:
        NEW1 = 63
    elif ALL_Num0Delq1Year < 20:
        NEW1 = 67
    elif ALL_Num0Delq1Year < 30:
        NEW1 = 73
    elif ALL_Num0Delq1Year < 35:
        NEW1 = 76
    elif ALL_Num0Delq1Year < 45:
        NEW1 = 80
    elif ALL_Num0Delq1Year < 55:
        NEW1 = 84
    elif ALL_Num0Delq1Year < 70:
        NEW1 = 89
    elif ALL_Num0Delq1Year < 100:
        NEW1 = 96
    elif ALL_Num0Delq1Year >= 100:
        NEW1 = 103
    else:
        pass

    """
    `NEW2` Calculation.  Derived from `ALL_PercPayments2Years`.
    """
    if ALL_PercPayments2Years is None:
        NEW2 = 73
    elif ALL_PercPayments2Years < -6:
        NEW2 = 0
    elif ALL_PercPayments2Years < 0:
        NEW2 = 55
    elif ALL_PercPayments2Years < 30:
        NEW2 = 55
    elif ALL_PercPayments2Years < 40:
        NEW2 = 59
    elif ALL_PercPayments2Years < 50:
        NEW2 = 62
    elif ALL_PercPayments2Years < 60:
        NEW2 = 66
    elif ALL_PercPayments2Years < 65:
        NEW2 = 69
    elif ALL_PercPayments2Years < 70:
        NEW2 = 72
    elif ALL_PercPayments2Years < 75:
        NEW2 = 75
    elif ALL_PercPayments2Years < 80:
        NEW2 = 77
    elif ALL_PercPayments2Years < 85:
        NEW2 = 79
    elif ALL_PercPayments2Years < 90:
        NEW2 = 84
    elif ALL_PercPayments2Years >= 90:
        NEW2 = 85
    else:
        pass

    """
    `NEW3` Calculation.  Derived from `ALL_AvgMonthsOnBook`.
    """
    if ALL_AvgMonthsOnBook is None:
        NEW3 = 73
    elif ALL_AvgMonthsOnBook <= -6:
        NEW3 = 0
    elif ALL_AvgMonthsOnBook < 0:
        NEW3 = 52
    elif ALL_AvgMonthsOnBook < 1:
        NEW3 = 52
    elif ALL_AvgMonthsOnBook < 6:
        NEW3 = 63
    elif ALL_AvgMonthsOnBook < 12:
        NEW3 = 66
    elif ALL_AvgMonthsOnBook < 24:
        NEW3 = 68
    elif ALL_AvgMonthsOnBook < 36:
        NEW3 = 71
    elif ALL_AvgMonthsOnBook < 42:
        NEW3 = 73
    elif ALL_AvgMonthsOnBook < 48:
        NEW3 = 75
    elif ALL_AvgMonthsOnBook < 54:
        NEW3 = 78
    elif ALL_AvgMonthsOnBook < 60:
        NEW3 = 79
    elif ALL_AvgMonthsOnBook < 66:
        NEW3 = 82
    elif ALL_AvgMonthsOnBook < 84:
        NEW3 = 86
    elif ALL_AvgMonthsOnBook < 120:
        NEW3 = 94
    elif ALL_AvgMonthsOnBook >= 120:
        NEW3 = 107
    else:
        pass

    """
    `NEW4` Calculation.  Derived from `ALL_MaxDelq180DaysLT24M`.
    """
    try:
        ALL_MaxDelq180DaysLT24M = ALL_MaxDelq180DaysLT24M.replace(" ", "").upper()
    except Exception as e:
        print(str(e))
        NEW4 = None
    if ALL_MaxDelq180DaysLT24M == "":
        NEW4 = 73
    elif ALL_MaxDelq180DaysLT24M in ["!", "$", "-", "="]:
        NEW4 = 0
    elif ALL_MaxDelq180DaysLT24M in [".", "-1", "?", "@"]:
        NEW4 = 76
    elif ALL_MaxDelq180DaysLT24M == "0":
        NEW4 = 76
    elif ALL_MaxDelq180DaysLT24M == "1":
        NEW4 = 70
    elif ALL_MaxDelq180DaysLT24M == "2":
        NEW4 = 66
    elif ALL_MaxDelq180DaysLT24M in ["3", "4", "5"]:
        NEW4 = 61
    elif ALL_MaxDelq180DaysLT24M in ["6", "7", "8"]:
        NEW4 = 56
    elif ALL_MaxDelq180DaysLT24M in ["9", "I", "J", "L", "W"]:
        NEW4 = 49
    else:
        pass

    """
    `NEW5` Calculation.  Derived from `ALL_NumTrades180Days`.
    """
    if ALL_NumTrades180Days is None:
        NEW5 = 73
    elif ALL_NumTrades180Days < -6:
        NEW5 = 0
    elif ALL_NumTrades180Days < 0:
        NEW5 = 77
    elif ALL_NumTrades180Days < 1:
        NEW5 = 77
    elif ALL_NumTrades180Days < 3:
        NEW5 = 73
    elif ALL_NumTrades180Days < 5:
        NEW5 = 62
    elif ALL_NumTrades180Days >= 5:
        NEW5 = 50
    else:
        pass

    """
    `NEW6` calculation.  Derived from `RCG_NumTradesUtilisedLT10`.
    """
    if RCG_NumTradesUtilisedLT10 is None:
        NEW6 = 73
    elif RCG_NumTradesUtilisedLT10 < -6:
        NEW6 = 0
    elif RCG_NumTradesUtilisedLT10 < 0:
        NEW6 = 63
    elif RCG_NumTradesUtilisedLT10 < 1:
        NEW6 = 72
    elif RCG_NumTradesUtilisedLT10 < 2:
        NEW6 = 83
    elif RCG_NumTradesUtilisedLT10 >= 2:
        NEW6 = 95
    else:
        pass

    """
    `NEW7` Calculation.  Derived from `ALL_Perc0Delq90Days`.
    """
    if ALL_Perc0Delq90Days is None:
        NEW7 = 73
    elif ALL_Perc0Delq90Days < -6:
        NEW7 = 0
    elif ALL_Perc0Delq90Days < 0:
        NEW7 = 63
    elif ALL_Perc0Delq90Days < 5:
        NEW7 = 63
    elif ALL_Perc0Delq90Days < 30:
        NEW7 = 64
    elif ALL_Perc0Delq90Days < 55:
        NEW7 = 67
    elif ALL_Perc0Delq90Days < 70:
        NEW7 = 70
    elif ALL_Perc0Delq90Days < 80:
        NEW7 = 72
    elif ALL_Perc0Delq90Days < 85:
        NEW7 = 73
    elif ALL_Perc0Delq90Days < 90:
        NEW7 = 75
    elif ALL_Perc0Delq90Days >= 90:
        NEW7 = 76
    else:
        pass

    """
    `NEW8` calculation.  Derived from `ALL_MaxDelqEver`.
    """
    try:
        ALL_MaxDelqEver = ALL_MaxDelqEver.replace(" ", "").upper()
    except Exception as e:
        print(str(e))
        NEW8 = None
    if ALL_MaxDelqEver == "":
        NEW8 = 73
    elif ALL_MaxDelqEver in ["!", "$", "-", "="]:
        NEW8 = 0
    elif ALL_MaxDelqEver in [".", "-1", "?", "@"]:
        NEW8 = 77
    elif ALL_MaxDelqEver in ["0", "1"]:
        NEW8 = 77
    elif ALL_MaxDelqEver == "2":
        NEW8 = 75
    elif ALL_MaxDelqEver == "3":
        NEW8 = 74
    elif ALL_MaxDelqEver == "4":
        NEW8 = 72
    elif ALL_MaxDelqEver in ["5", "6", "7", "8"]:
        NEW8 = 70
    elif ALL_MaxDelqEver in ["9", "I", "J", "L", "W"]:
        NEW8 = 66
    else:
        pass

    ls_NEW = [NEW1, NEW2, NEW3, NEW4, NEW5, NEW6, NEW7, NEW8]
    ls_NEW_eval = [type(x) == int for x in ls_NEW]
    if all(ls_NEW_eval):
        NEW_CustomerScore = sum(ls_NEW)
    else:
        ls_NEW_norm = [0 if x is None else x for x in ls_NEW]
        NEW_CustomerScore = sum(ls_NEW_norm)

    NEW_CustomerScore_LNODDS = (NEW_CustomerScore - CONSTANT_NEW) / (PDO / math.log(2))
    NEW_CustomerScore_PROB = math.exp(NEW_CustomerScore_LNODDS) / (1 + math.exp(NEW_CustomerScore_LNODDS))
    return NEW_CustomerScore, NEW_CustomerScore_LNODDS, NEW_CustomerScore_PROB


schema_NewCustomerScore = t.StructType([
    t.StructField("NEW_CustomerScore", t.IntegerType(), True),
    t.StructField("NEW_CustomerScore_LNODDS", t.DoubleType(), True),
    t.StructField("NEW_CustomerScore_PROB", t.DoubleType(), True)
])


udf_NewCustomerScore = f.udf(NewCustomerScore, returnType=schema_NewCustomerScore)
