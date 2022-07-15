import pyspark.sql.functions as f
import pyspark.sql.types as t
import math


# Column Z:  Stage Fix
def stage_fix_func(ACCOUNT_GBX, BD_USE, ACCOUNT_DLQ, STAGE_2):
    if ACCOUNT_GBX == "X":
        result = 4
    elif BD_USE == 1:
        result = 5
    elif ACCOUNT_DLQ == "4":
        result = 3
    else:
        result = STAGE_2
    return result

udf_stage_fix = f.udf(stage_fix,
                      returnType=t.IntegerType())

# --------------------------------------------------------------------------------

# Column AA: Cure

def cure_func(stage_fix, ACCOUNT_DLQ, NON_DEF_2_C, NON_DEF_2_TIME, vlu_cure):
    try:
        if (stage_fix == 4):
            result = "DECEASED"
        elif (stage_fix == 5):
            result = "WO"
        elif (ACCOUNT_DLQ == "4" or ACCOUNT_DLQ == "A"):
            result = "DEFAULT"
        elif (NON_DEF_2_C == 1 and stage_fix <= 2):
            result = "PERF"
        else:
            if(NON_DEF_2_TIME == 0):
                result = "DEFAULT"
            elif(NON_DEF_2_C > 5):
                result = ">5 DEFAULTS"
            elif(NON_DEF_2_TIME < vlu_cure.loc[vlu_cure.Lookup == NON_DEF_2_C,"First Cure"].iloc[0]):
                result = "DEFAULT"
            elif(NON_DEF_2_TIME < vlu_cure.loc[vlu_cure.Lookup == NON_DEF_2_C,"Full Cure"].iloc[0]):
                result = "PARTIAL CURE"
            elif(NON_DEF_2_TIME >= vlu_cure.loc[vlu_cure.Lookup == NON_DEF_2_C,"First Cure"].iloc[0]):
                result = "PURE CURE"
    except:
        result = "DEFAULT"
    
    return result

udf_cure = f.udf(cure_func,
                 returnType=t.StringType())

# -----------------------------------------------------------------------------------------------------

# Column AB: Cure Adjustment

def cure_adjustment_func(cure_bool, cure, NON_DEF_2_C, vlu_cure):
    if(cure_bool == "No"):
        result = 1
    elif (cure == "PARTIAL CURE"):
        result = vlu_cure.loc[vlu_cure.Lookup == NON_DEF_2_C,"Adjustment"]
    else:
        result = 1
    
    return result

udf_cure_adjustment = f.udf(cure_adjustment_func,
                             returnType=t.IntegerType())

# -----------------------------------------------------------------------------------------------------

# Column AC: Stage Cure

def stage_cure_func(stage_fix, cure, ACCOUNT_DLQ):
    if(stage_fix==3 and (cure="PURE CURE" or cure="PARTIAL CURE")):
        if(ACCOUNT_DLQ == "C"):
            result = 1
        elif(ACCOUNT_DLQ == "P"):
            result = 1
        elif(ACCOUNT_DLQ == "0"):
            result = 1
        elif(ACCOUNT_DLQ == "1"):
            result = 2
        else:
            result = 3
    else:
        result = stage_fix
    return result

udf_stage_cure = f.udf(stage_cure_func,
                       returnType=t.IntegerType())

# -----------------------------------------------------------------------------------------------------

# Column AD: PD - Stage 1

# Column AE: PD - Stage 2

# Column AF: PD - Stage 3

# -----------------------------------------------------------------------------------------------------

# Column AG: PD - Use 

def PD_use_func(cure_bool, stage_fix, stage_cure, PD_stage1, PD_stage2, PD_stage3):
    stage = stage_fix if cure_bool=="No" else stage = stage_cure    
    if(stage == 2 or stage_fix in [4,5]):
        result = PD_stage3
    elif(stage == 2):
        result = PD_stage2
    else:
        result = PD_stage1
    return result

udf_pd_use = f.udf(pd_use_func,
                   returnType=t.DoubleType())

# -----------------------------------------------------------------------------------------------------

# Column AH: EAD

# Column AI: TTD 

# -----------------------------------------------------------------------------------------------------

# Column AJ: avg_rem_time_fix
    
def avg_rem_time_fix(cure_bool, stage_fix, stage_cure, TTD, avg_rem_time_red):
    stage = stage_fix if cure_bool=="No" else stage = stage_cure
    if (stage == 3 or stage_fix in [4, 5]):
        return avg_rem_time_red
    else:
        return math.ceil(max([0, avg_rem_time_red - TTD + 2]))

udf_avg_rem_time_fix = f.udf(avg_rem_time_fix,
                             returnType=t.IntegerType())

# -----------------------------------------------------------------------------------------------------

# Column AK: LGD - New to NPL

# Column AL: LGD - Curve

# Column AM: LGD - Use

# Column AN: TTWO

# Column AO: PWOR - just a constant

# Column AP: PIT PD Adj

# Column AQ: PIT LGD Adj - just a constant

# -----------------------------------------------------------------------------------------------------

# Column AR: ECL - 1 

# Column AS: ECL - 2 

# Column AT: ECL - 3

# for these 3 ECL columns only two variables differ:
def ECL_1_2_3(interest_rate, PWOR, TTD, TTWO, PIT_PD_Adj, PIT_LGD_Adj, EAD, bal_total, PD_stage, LGD_stage):
    if interest_rate is None:
        interest_rate = 0
    if PWOR is None:
        PWOR = 0
    if TTD is None:
        TTD = 0
    if TTWO is None:
        TTWO = 0
    if bal_total is None:
        bal_total = 0    
    if PIT_PD_Adj is None:
        PIT_PD_Adj = 0
    if PIT_LGD_Adj is None:
        PIT_LGD_Adj = 0
    if EAD is None:
        EAD = 0
    if PD_stage is None:
        PD_stage = 0
    if LGD_stage is None:
        LGD_stage = 0
    
    A = ((bal_total * PIT_PD_Adj) * PD_stage * EAD * (LGD_stage * PIT_LGD_Adj * (1-PWOR))) 
    B = ((1+interest_rate) ** (-(TTD+TTWO)/12))
    result = A * B
    return result
    
udf_ECL_stage = f.udf(ECL_1_2_3,
                      returnType=t.DoubleType())

# -----------------------------------------------------------------------------------------------------

# Column AU: ECL

def ECL_func(interest_rate, PWOR, TTD, TTWO, PIT_PD_Adj, PIT_LGD_Adj, EAD, bal_total, updated_stage, WOF_ALL, PD_use, LGD_use):
    if(updated_stage == 5):
        result = WOF_ALL
    else:
        PIT_PD = 1 if updated_stage=1 else PIT_PD = PIT_PD_Adj
        A = ((bal_total * PIT_PD) * PD_use * EAD * (LGD_use * PIT_LGD_Adj * (1-PWOR)))
        B = ((1+interest_rate) ** (-(TTD+TTWO)/12))
        result = A * B
    return result

udf_ECL = f.udf(ECL_func,
                returnType=t.DoubleType())

# -----------------------------------------------------------------------------------------------------

# Column AV: Updated Stage

def updated_stage_func(cure_bool, bal_total, stage_cure, stage_fix, ECL_2, ECL_3):
    if(bal_total < 0 and stage_cure <= 3):
        result = -1
    else:
        if(cure_bool == "No"):
            if(stage_fix == 3 and ECL_3 < ECL_2):
                result = 2
            else:
                result = stage_fix
        else:
            if(stage_cure == 3 and ECL_3 < ECL_2):
                result = 2
            else:
                result = stage_cure
    return result

udf_updated_stage = f.udf(updated_stage_func,
                          returnType=t.IntegerType())

# -----------------------------------------------------------------------------------------------------

# Column AW: PD - Base

# Column AX: PD - Upside

# Column AY: PD - Downside

# for these 3 PD scenario columns only one variable differs: scenario_PD_input
def PD_scenario_func(scenario_PD_input, PIT_PD_Adj, updated_stage, PD_stage1, PD_stage2, PD_stage3, cure_adjustment):
    if (updated_stage in [3,4,5]):
        interim = PD_stage3 
    elif (updated_stage == 2):
        interim = PD_stage2 * scenario_PD_input
    else:
        interim = PD_stage1 * PIT_PD_Adj * cure_adjustment * scenario_PD_input
    result = min(1, interim)
    return result

udf_PD_scenario = f.udf(PD_scenario_func,
                        returnType=t.DoubleType())

# -----------------------------------------------------------------------------------------------------

# Column AZ: LGD - Base

# Column BA: LGD - Upside

# Column BB: LGD - Downside

#for these 3 LGD scenario columns only one variable differs: scenario_LGD
def LGD_scenario_func(scenario_LGD_input, PIT_LGD_Adj, updated_stage, LGD_New_to_NPL, LGD_Curve):
    if (updated_stage in [1, 2]):
        interim = LGD_New_to_NPL
    else:
        interim = LGD_Curve
    result = interim * PIT_LGD_Adj * scenario_LGD_input
    return result

udf_LGD_scenario = f.udf(LGD_scenario_func,
                         returnType=t.DoubleType())

# -----------------------------------------------------------------------------------------------------

# Column BC: ECL - Base

# Column BD: ECL - Upside

# Column BE: ECL - Downside

# for these 3 ECL scenario columns only two variables differ: PD_scenario, LGD_scenario
def ecl_scenario_func(interest_rate, PWOR, TTD, TTWO, EAD, bal_total, stage_fix, WOF_ALL, PD_scenario, LGD_scenario):
    if interest_rate is None:
        interest_rate = 0
    if PWOR is None:
        PWOR = 0
    if TTD is None:
        TTD = 0
    if TTWO is None:
        TTWO = 0
    if EAD is None:
        EAD = 0
    if bal_total is None:
        bal_total = 0    
    if stage_fix is None:
        stage_fix = 0
    if WOF_ALL is None:
        WOF_ALL = 0    
    if PD_scenario is None:
        PD_scenario = 0
    if PD_scenario is None:
        PD_scenario = 0
    
    if (stage_fix = 5):
        interim = WOF_ALL
    else:
        A = bal_total * PD_scenario * EAD * LGD_scenario * (1-PWOR)
        B = (1+interest_rate) ** (-(TTD+TTWO)/12)
        interim = A * B
    result = max(interim, 0)
    return result

udf_ecl_scenario = f.udf(ecl_scenario_func,
                         returnType=t.DoubleType())

# -----------------------------------------------------------------------------------------------------

# Column BF: Final ECL

def ecl_final_func(ECL_base, weight_base, ECL_upside, weight_upside, ECL_downside, weight_downside):
    result = (ECL_base * weight_base) + (ECL_upside * weight_upside) + (ECL_downside * weight_downside)
    return result

udf_ecl_final = f.udf(ecl_final_func,
                      returnType=t.DoubleType())

