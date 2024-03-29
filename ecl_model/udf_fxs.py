import pyspark.sql.functions as f
import pyspark.sql.types as t
import math


# Dependent on `WOF_NOV21`
def stage_fix(ACCOUNT_GBX, BD_USE, ACCOUNT_DLQ, STAGE):
    if ACCOUNT_GBX == "X":
        return 4
    elif BD_USE == 1:
        return 5
    elif ACCOUNT_DLQ == "4":
        return 3
    else:
        return int(STAGE)


udf_stage_fix = f.udf(stage_fix,
                      returnType=t.IntegerType())


def pd_use(STAGE_FIX, PD_STAGE_1, PD_STAGE_2, PD_STAGE_3):
    val_int = int(STAGE_FIX)
    if val_int in [3, 4, 5]:
        return PD_STAGE_3
    elif val_int == 2:
        return PD_STAGE_2
    else:
        return PD_STAGE_1


udf_pd_use = f.udf(pd_use,
                   returnType=t.DoubleType())


def avg_rem_time_fix(STAGE_FIX, TTD, AVG_REM_TIME_RED):
    if STAGE_FIX in [3, 4, 5]:
        return AVG_REM_TIME_RED
    else:
        return math.ceil(max([0, AVG_REM_TIME_RED - TTD + 2]))


udf_avg_rem_time_fix = f.udf(avg_rem_time_fix,
                             returnType=t.IntegerType())


def ecl_n(BAL_TOTAL, PIT_PD_ADJ, PD_STAGE_1, PD_STAGE_2, PD_STAGE_3, EAD,
          LGD_NEW_TO_NPL, LGD_CURVE, PIT_LGD_ADJ, TTD, TTWO,
          INTEREST_RATE, INTEREST_PWOR, n):
    if TTWO is None:
        TTWO = 0
    if BAL_TOTAL is None:
        BAL_TOTAL = 0
    if PIT_PD_ADJ is None:
        PIT_PD_ADJ = 0
    if EAD is None:
        EAD = 0
    if LGD_NEW_TO_NPL is None:
        LGD_NEW_TO_NPL = 0
    if PIT_LGD_ADJ is None:
        PIT_LGD_ADJ = 0
    if INTEREST_PWOR is None:
        INTEREST_PWOR = 0
    if PD_STAGE_1 is None:
        PD_STAGE_1 = 0
    if PD_STAGE_2 is None:
        PD_STAGE_2 = 0
    if PD_STAGE_3 is None:
        PD_STAGE_3 = 0
    if TTD is None:
        TTD = 0
    if INTEREST_RATE is None:
        INTEREST_RATE = 0
    if n == 1:
        A = BAL_TOTAL * PIT_PD_ADJ * PD_STAGE_1 * EAD * LGD_NEW_TO_NPL * PIT_LGD_ADJ * (1 - INTEREST_PWOR)
    elif n == 2:
        A = BAL_TOTAL * PIT_PD_ADJ * PD_STAGE_2 * EAD * LGD_NEW_TO_NPL * PIT_LGD_ADJ * (1 - INTEREST_PWOR)
    elif n == 3:
        A = BAL_TOTAL * PIT_PD_ADJ * PD_STAGE_3 * EAD * LGD_CURVE * PIT_LGD_ADJ * (1 - INTEREST_PWOR)
    else:
        return None

    B = (1 + INTEREST_RATE)**((-TTD - TTWO) / 12.0)
    return A * B


udf_ecl_n = f.udf(ecl_n, returnType=t.DoubleType())


# Dependent on `WOF_NOV21`
def ECL(PD_USE, EAD, LGD_USE, PIT_LGD_ADJ, PIT_PD_ADJ,
        BAL_TOTAL, TTD, TTWO, STAGE_FIX, BDT_ALL,
        PWOR, INTEREST_RATE):
    if BAL_TOTAL is None:
        BAL_TOTAL = 0
    if TTWO is None:
        TTWO = 0
    A = PD_USE * EAD * LGD_USE * PIT_LGD_ADJ * (1 - PWOR)
    B = (1 + INTEREST_RATE)**(-1 * (TTD + TTWO) / 12)
    if STAGE_FIX == 5:
        return BDT_ALL
    else:
        if STAGE_FIX == 1:
            k = BAL_TOTAL * PIT_PD_ADJ
        else:
            k = BAL_TOTAL * 1.0
        return k * A * B


udf_ECL = f.udf(ECL,
                returnType=t.DoubleType())


def updated_stage(STAGE_FIX, ECL_2, ECL_3):
    if (STAGE_FIX == 3) and (ECL_2 > ECL_3):
        return 2
    else:
        return STAGE_FIX


udf_updated_stage = f.udf(updated_stage,
                          returnType=t.IntegerType())


def PD_Base(UPDATED_STAGE, PD_STAGE_1, PD_STAGE_2, PD_STAGE_3, PIT_PD_ADJ, BASE_PD):
    if UPDATED_STAGE in [3, 4, 5]:
        B = PD_STAGE_3
    elif UPDATED_STAGE == 2:
        B = PD_STAGE_2 * BASE_PD
    else:
        B = PD_STAGE_1 * PIT_PD_ADJ * BASE_PD
    return min([1.0, B])


udf_PD_Base = f.udf(PD_Base,
                    returnType=t.DoubleType())


def PD_Upside(UPDATED_STAGE, PD_STAGE_1, PD_STAGE_2, PD_STAGE_3, PIT_PD_ADJ, UPSIDE_PD):
    if UPDATED_STAGE in [3, 4, 5]:
        B = PD_STAGE_3
    elif UPDATED_STAGE == 2:
        B = PD_STAGE_2 * PIT_PD_ADJ * UPSIDE_PD
    else:
        B = PD_STAGE_1 * PIT_PD_ADJ * UPSIDE_PD
    return min([1.0, B])


udf_PD_Upside = f.udf(PD_Upside,
                      returnType=t.DoubleType())


def PD_Downside(UPDATED_STAGE, PD_STAGE_1, PD_STAGE_2, PD_STAGE_3, PIT_PD_ADJ, DOWNSIDE_PD):
    if UPDATED_STAGE in [3, 4, 5]:
        B = PD_STAGE_3
    elif UPDATED_STAGE == 2:
        B = PD_STAGE_2 * PIT_PD_ADJ * DOWNSIDE_PD
    else:
        B = PD_STAGE_1 * PIT_PD_ADJ * DOWNSIDE_PD
    return min([1.0, B])


udf_PD_Downside = f.udf(PD_Downside,
                        returnType=t.DoubleType())


def LGD_Base(UPDATED_STAGE, LGD_NEW_TO_NPL, LGD_CURVE, PIT_LGD_ADJ, BASE_LGD):
    if UPDATED_STAGE in [1, 2]:
        A = LGD_NEW_TO_NPL
    else:
        A = LGD_CURVE
    B = PIT_LGD_ADJ * BASE_LGD
    return A * B


udf_LGD_Base = f.udf(LGD_Base,
                     returnType=t.DoubleType())


def LGD_Upside(UPDATED_STAGE, LGD_NEW_TO_NPL, LGD_CURVE, PIT_LGD_ADJ, UPSIDE_LGD):
    if UPDATED_STAGE in [1, 2]:
        A = LGD_NEW_TO_NPL
    else:
        A = LGD_CURVE
    B = PIT_LGD_ADJ * UPSIDE_LGD
    return A * B


udf_LGD_Upside = f.udf(LGD_Upside,
                       returnType=t.DoubleType())


def LGD_Downside(UPDATED_STAGE, LGD_NEW_TO_NPL, LGD_CURVE, PIT_LGD_ADJ, DOWNSIDE_LGD):
    if UPDATED_STAGE in [1, 2]:
        A = LGD_NEW_TO_NPL
    else:
        A = LGD_CURVE
    B = PIT_LGD_ADJ * DOWNSIDE_LGD
    return A * B


udf_LGD_Downside = f.udf(LGD_Downside,
                         returnType=t.DoubleType())


# Dependent on `WOF_NOV21`
def ECL_Base(STAGE_FIX, BDT_ALL, BAL_TOTAL, PD_BASE, EAD, LGD_BASE, TTD, TTWO,
             INTEREST_PWOR, INTEREST_RATE):
    if TTWO is None:
        TTWO = 0
    if BAL_TOTAL is None:
        BAL_TOTAL = 0
    if STAGE_FIX == 5:
        return max([0, BDT_ALL])
    else:
        A = BAL_TOTAL * PD_BASE * EAD * (LGD_BASE * (1.0 - INTEREST_PWOR))
        B = (1.0 + INTEREST_RATE)**((-TTD - TTWO) / 12.0)
        C = A * B
        if C is None:
            return 0
        else:
            return max([0, C])


udf_ECL_Base = f.udf(ECL_Base, returnType=t.DoubleType())


# Dependent on `WOF_NOV21`
def ECL_Upside(STAGE_FIX, BDT_ALL, BAL_TOTAL, PD_UPSIDE, EAD, LGD_UPSIDE, TTD, TTWO,
               INTEREST_PWOR, INTEREST_RATE):
    if TTWO is None:
        TTWO = 0
    if BAL_TOTAL is None:
        BAL_TOTAL = 0
    if STAGE_FIX == 5:
        return max([0, BDT_ALL])
    else:
        A = BAL_TOTAL * PD_UPSIDE * EAD * (LGD_UPSIDE * (1.0 - INTEREST_PWOR))
        B = (1.0 + INTEREST_RATE)**((-TTD - TTWO) / 12.0)
        C = A * B
        if C is None:
            return 0
        else:
            return max([0, C])


udf_ECL_Upside = f.udf(ECL_Upside, returnType=t.DoubleType())


# Dependent on `WOF_NOV21`
def ECL_Downside(STAGE_FIX, BDT_ALL, BAL_TOTAL, PD_DOWNSIDE, EAD, LGD_DOWNSIDE, TTD, TTWO,
               INTEREST_PWOR, INTEREST_RATE):
    if TTWO is None:
        TTWO = 0
    if BAL_TOTAL is None:
        BAL_TOTAL = 0
    if STAGE_FIX == 5:
        return max([0, BDT_ALL])
    else:
        A = BAL_TOTAL * PD_DOWNSIDE * EAD * (LGD_DOWNSIDE * (1.0 - INTEREST_PWOR))
        B = (1.0 + INTEREST_RATE)**((-TTD - TTWO) / 12.0)
        C = A * B
        if C is None:
            return 0
        else:
            return max([0, C])


udf_ECL_Downside = f.udf(ECL_Upside, returnType=t.DoubleType())


def final_ecl(ECL_BASE, BASE_WEIGHTING, ECL_UPSIDE, UPSIDE_WEIGHTING, ECL_DOWNSIDE, DOWNSIDE_WEIGHTING):
    if ECL_BASE is None:
        A = 0
    else:
        A = ECL_BASE * BASE_WEIGHTING
    if ECL_UPSIDE is None:
        B = 0
    else:
        B = ECL_UPSIDE * UPSIDE_WEIGHTING
    if ECL_DOWNSIDE is None:
        C = 0
    else:
        C = ECL_DOWNSIDE * DOWNSIDE_WEIGHTING
    return A + B + C


udf_final_ecl = f.udf(final_ecl, returnType=t.DoubleType())
