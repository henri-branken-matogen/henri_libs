import pyspark.sql.functions as f
import pyspark.sql.types as t


chunk_MOB = 3
chunk_FIN = 6


def Account_Behaviour(OBS1, OBS6, OBS12, OBS18, OBS24):
    """
    As per BD:  Generate the Account Behaviour Profile.
    However, this is to be eventually replaced by the Account_State.py script.
    The parameters below can be found in the BEH_ACCT_<ccyymm> dataset;
    they have slightly different names as indicated below.
    :param OBS1:   01-month behaviour feature.  `B01`.
    :param OBS6:   06-month behaviour feature.  `B06`.
    :param OBS12:  12-month behaviour feature.  `B12`.
    :param OBS18:  18-month behaviour feature.  `B18`.
    :param OBS24:  24-month behaviour feature.  `B24`.
    :return:  Returns the `Account_Behaviour_val` and `Customer_Behaviour_val` of the record.
              Both are of dtype string.
    """

    """
    Initialise the `Account_Behaviour_val` and the `Customer_Behaviour_val`.
    """
    Account_Behaviour_val = ""  # Account Behaviour (SME)
    Customer_Behaviour_val = "to be announced"  # Customer Behaviour (SME)

    if batch_evaluation("X-Exclusion", "or", OBS1, OBS6, OBS12, OBS18, OBS24):
        Account_Behaviour_val = "X. Exclusions"

    if batch_evaluation("I-Missing", "or", OBS1, OBS6, OBS12, OBS18, OBS24):
        Account_Behaviour_val = "D. Missing Record"

    if batch_evaluation("B-Bad", "or", OBS1, OBS6, OBS12, OBS18, OBS24):
        Account_Behaviour_val = "F. Adverse Account"

    if batch_evaluation("B-Partial", "or", OBS6, OBS12, OBS18, OBS24):
        Account_Behaviour_val = "E. Poor Account"

    if batch_evaluation("G-Good", "or", OBS6, OBS12, OBS18, OBS24):
        Account_Behaviour_val = "A. Excellent Account"

    if batch_evaluation("G-Partial", "or", OBS6, OBS12, OBS18, OBS24):
        Account_Behaviour_val = "B. Good Account"

    if batch_evaluation("P-Paid-up", "or", OBS1, OBS6, OBS12, OBS18, OBS24):
        Account_Behaviour_val = "C. Paid-up Account"

    return Account_Behaviour_val, Customer_Behaviour_val


schema_Account_Behaviour = t.StructType([
    t.StructField("acct_beh", t.StringType(), True),
    t.StructField("cust_beh", t.StringType(), True)
])

udf_Account_Behaviour = f.udf(Account_Behaviour, returnType=schema_Account_Behaviour)


def Account_State(Account_CAT, Account_DLQ,
                  OBS1_DC1,
                  OBS3_CRD, OBS3_DC0, OBS3_PUP,
                  OBS12_DC0, OBS12_DC2, OBS12_PUP,
                  OBS24, OBS24_DC1, OBS24_DC2, OBS24_DC3, OBS24_DC4, OBS24_ADV, OBS24_PUP):
    """
    As per BD:  Generate the Account Transition States (which should eventually replace the `Account Behaviour`).
    :param Account_CAT:  DType String.  Account Category.
    :param Account_DLQ:  Account Delinquency.
    :param OBS1_DC1:  Observation window, looking 1 month back (i.e. current month).
                      DC1 is a counter of how many times were DC1 (30 days) during observation window.
    :param OBS3_CRD:  Observation window looking 3 months back.
                      CRD a counter of how many times the account had a credit balance during the 3-month observation
                      window.
    :param OBS3_DC0:  Observation window looking 3 months back.  DC0 counter of how many times the account was UTD
                      during the measured period.
    :param OBS3_PUP:  Observation window looking 3 months back.  PUP = counter of how many times the account was paidup.
    :param OBS12_DC0:  Observation windows looking 12 months back.  DC2 = counter of how many times the account was
                       UTD.
    :param OBS12_DC2:  Observation window looking 12 months back.  DC2 = counter of how many times the account was DC2
                       (60 Days) during the observation window.
    :param OBS12_PUP:  Observation window looking 12 months back.  PUP = counter of how many times the account was
                       paidup.
    :param OBS24:  Determined by Behaviour_GBIPX script, and used for our current Account_Behaviour features (Stallion).
                   OBS = Observation variable.
                   OBS<x> creates the counters that the above and below variables absorb.
                   OBS24 is intended to be replaced by Account_Behaviour / Customer_Behaviour features in the future.
    :param OBS24_DC1:  Observation window looking 24 months back.
                       DC1 = counter of how many times the account was (30 Days) during the observation window.
    :param OBS24_DC2:  Observation window looking 24 months back.
                       DC2 = counter of how many times the account was (60 Days) during the observation window.
    :param OBS24_DC3:  Observation window looking 24 months back.
                       DC3 = counter of how many times the account was (90 Days) during the observation window.
    :param OBS24_DC4:  Observation window looking 24 months back.
                       DC4 = counter of how many times the account was (120 Days) during the observation window.
    :param OBS24_ADV:  Observation window looking 24 months back.
                       ADV = counter of how many times the account was (Adverse) during the observation window.
    :param OBS24_PUP:  Observation window looking 24 months back.
                       PUP = counter of how many times the account was (Paid-Up) during the observation window.
    :return:  Returns `Account_State_val` and `Customer_State_val`.  Both are of dtype String.
    """

    """
    Initialisation of `return` variables:
    In SAS, the variable names are simply named as:  `Account_State`, `Customer_State`.
            In Python, this would conflict the function name and/or script name.
    """
    Account_State_val = ""  # The Account State (SME)
    Customer_State_val = "to be announced"  # The Customer State (SME)

    """
    (i)
    Deceased, etc...
    """
    if (Account_DLQ == "D") or (OBS24 == "X-Exclusion") or (OBS24 == "I-Missing"):
        Account_State_val = "X. EXC Exclusions"

    """
    (ii)
    Still to be created from the Contracts Files.
    """
    if Account_CAT.upper() == "APP":
        Account_State_val = "A. NTU Not Taken-Up Applications"

    """
    (iii)
    New Accounts opened in the past 6 months.
    """
    if Account_CAT.upper() == "NEW":
        Account_State_val = "1. IMM Immature Accounts"

    if OBS3_CRD == 3:
        Account_State_val = "B. CRD Credit balance 3+ months (Voluntary Churn)"

    if (Account_DLQ == "C") and (OBS3_CRD is not None):
        Account_State_val = "2. C01 Credit Balance 1-2 months"

    if (OBS12_PUP == 12) and batch_evaluation(None, "and", OBS24_ADV, OBS24_DC4):
        Account_State_val = "C. PUP Paid-up 12+ months (Voluntary Churn)"

    if (OBS3_PUP == 3) and batch_evaluation(None, "and", OBS24_ADV, OBS24_DC4):
        Account_State_val = "3. P01 Paid-up 3-11 months"

    if Account_DLQ == "P" and (OBS3_PUP is not None) and batch_evaluation(None, "and", OBS24_ADV, OBS24_DC4):
        Account_State_val = "3. P01 Paid-up 1-2 months"

    """
    (iv)
    UTD with 12-month clear payment streak, no prior arrears / paid-up.
    """
    if (OBS12_DC0 == 12) and batch_evaluation(None, "and", OBS24_DC1, OBS24_DC2, OBS24_ADV, OBS24_DC3, OBS24_DC4,
                                              OBS24_PUP):
        Account_State_val = "4. CLR Clear"

    """
    (v)
    [1] In the past 3 months, the account must have been consecutively UTD (DC0).
    [2] In the past 12 months, the account must NOT have reached 60 Days (DC2).
    [3] In the past 24 months, the account must not have reached 90 Days (DC3), 120 Days (DC4), and not had any adverse.
    We effectively have 3 observation windows.
    The net effect is that the account must show a running streak of payments (first check), but could have had a minor
    delinquency in the 12 months (second check), but must not have had a serious delinquency during the 24 months
    contract period (third check).  This all assumes a 24 month contract which is the most common contract.
    """
    if (OBS3_DC0 == 3) and (OBS12_DC2 is None) and batch_evaluation(None, "and", OBS24_ADV, OBS24_DC3, OBS24_DC4):
        Account_State_val = "5. RES Responsible"

    """
    (vi)
    UTD, but struggles to pay consistently.
    """
    if Account_DLQ == "0":
        Account_State_val = "6. ERR Erratic"

    """
    (vii)
    30 Days in Arrears.
    """
    if OBS1_DC1 == 1:
        Account_State_val = "7. EXT Extended"

    """
    (viii)
    60 or 90 Days in Arrears.
    """
    if Account_DLQ in ["2", "3"]:
        Account_State_val = "8. Distressed"

    """
    (ix)
    120 days in Arrears / Adverse Event / Paid-up.
    """
    if Account_DLQ in ["A", "4", "P"]:
        Account_State_val = "D. DBT Doubtful Debt (Involuntary Churn)"

    return Account_State_val, Customer_State_val


schema_Account_State = t.StructType([
    t.StructField("acct_state", t.StringType(), True),
    t.StructField("cust_state", t.StringType(), True)
])

udf_Account_State = f.udf(Account_State,
                          returnType=schema_Account_State)


def Account_Transition(NDX, PIT, Account_AGE, Account_DLQ, Aging_profile, DLQ_profile, Account_State_val):
    """
    Determine the aging roll forward.
    Effectively produces the roll forward views.
    Should give us good enough representation of how accounts move through the aging buckets / delinquency cycles.
    :param NDX:  The starting point in the DELINQUENCY/GBIPX strings.
                      Remember that: NDX_py  = NDX_sas - 1.   > how to get the Python equivalent.
                      Alternatively: NDX_sas = NDX_py + 1.   > how to get the SAS equivalent.
                      NDX without any subscript is assumed to represent NDX_sas, NOT NDX_py.
                      The Parameter Passed should be NDX and NOT INDEX.
    :param PIT:  Point in Time.  Dtype integer.
    :param Account_AGE:  Aging attribute.  Dtype string.
    :param Account_DLQ:  Delinquency attribute.  Dtype string.
    :param Aging_profile:  60-character-long string containing ageing attributes of past 60 months.
    :param DLQ_profile:  60-character-long string containing delinquency attributes of the past 60 months.
    :param Account_State_val:  The state that the account is in.
    :return:    `Transition_NDX_AGE`  > Dtype String.
                `Transition_NDX_DLQ`  > Dtype String.
                `Transition_NDX_MAT`  > Dtype Integer.
    """

    """
    (i)
    Initialisation of the `Transition_NDX_<x>` variables:
    According to SAS:
            Transition_&NDX._AGE
            Transition_&NDX._DLQ
            Transition_&NDX._MAT
    """
    Transition_NDX_AGE = ""  # The Account Transition Aging &NDX.  > Type 'String'.
    Transition_NDX_DLQ = ""  # The Account Transition Delinquency &NDX.  > Type 'String'.
    Transition_NDX_MAT = None  # The Account Transition Aging and Delinquency Match &NDX.  > Type 'Numeric'.

    delta = PIT - NDX
    delta_py = delta - 1
    if delta > 0:
        # Fetch the next period account aging / delinquency / states, and append to the current month to create combos.
        Transition_NDX_AGE = str(Account_AGE + Aging_profile[delta_py]).replace(" ", "")
        Transition_NDX_DLQ = str(Account_DLQ + DLQ_profile[delta_py]).replace(" ", "")

        # If there is a missing aging field, then exclude by converting the field into "--".
        if Account_AGE in ("-", None) or Aging_profile[delta_py] in ("-", None):
            Transition_NDX_AGE = "--"
        if Account_AGE in (".", None) or Aging_profile[delta_py] in (".", None):
            Transition_NDX_AGE = ".."

        # If there is a missing Delinquency Field, then Exclude by converting the field into "..".
        if Account_DLQ in (".", None) or DLQ_profile[delta_py] in (".", None):
            Transition_NDX_DLQ = ".."

        # If `Account_DLQ`="X" (Exlusion), in either month, then we exclude by converting the field into "XX":
        if Account_DLQ == "X" or DLQ_profile[delta_py] == "X":
            Transition_NDX_AGE = "XX"

        # If deceased, in either month, then exclude by converting the field into "DD":
        if Account_DLQ == "D" or DLQ_profile[delta_py] == "D":
            Transition_NDX_AGE = "DD"

        """
        (ii)
        Scan the Account State, and override any prior combinations:
        """
        char_eval = Account_State_val[0].upper()
        if char_eval == "B":
            # Already a Credit Balance Account.
            Transition_NDX_AGE = "CC"
            Transition_NDX_DLQ = "CC"
        if char_eval == "C":
            # Already a Paid-Up Account.
            Transition_NDX_AGE = "PP"
            Transition_NDX_DLQ = "PP"
        if char_eval == "D":
            # Already a Doubtful Debt Account.
            Transition_NDX_AGE = "44"
            Transition_NDX_DLQ = "44"
        if char_eval == "X":
            # Already an Exclusion Account.
            Transition_NDX_AGE = "XX"
            Transition_NDX_DLQ = "XX"

        """
        (iii)
        Expand the 2 character aging and delinquency combinations, created above, into meaningful strings.
        """
        Transition_NDX_AGE = Transition_Convert(Transition_NDX_AGE)
        Transition_NDX_DLQ = Transition_Convert(Transition_NDX_DLQ)

        """
        (iv)
        Flag when the 'aging' and 'delinquency' match each other, so that it can be analysed down the line.
        """
        if Transition_NDX_AGE == Transition_NDX_DLQ:
            Transition_NDX_MAT = 1

        """
        (v)
        Remove the adverse wording, as the aging profile does not know about adverse events.
        The original SAS code is as follows:
            Transition_&NDX._AGE=CompBL(Tranwrd(Transition_&NDX._AGE,'or adverse event',''))
            Transition_&NDX._AGE=CompBL(Tranwrd(Transition_&NDX._AGE,'or 120 Days and adverse next period',''))
        """
        Transition_NDX_AGE = ' '.join((Transition_NDX_AGE.replace("or adverse event", "")).split())
        Transition_NDX_AGE = ' '.join((Transition_NDX_AGE.replace("or 120 Days and adverse next period", "")).split())

    return Transition_NDX_AGE, Transition_NDX_DLQ, Transition_NDX_MAT


schema_Account_Transition = t.StructType([
    t.StructField("tr_ndx_age", t.StringType(), True),
    t.StructField("tr_ndx_dlq", t.StringType(), True),
    t.StructField("tr_ndx_mat", t.IntegerType(), True)
])

udf_Account_Transition = f.udf(Account_Transition,
                               returnType=schema_Account_Transition)


def batch_evaluation(eval_against, conjunction, *observations):
    """
    A functions that evaluates a bunch of variables (stored in `observations`) against a single value
    (i.e. `eval_against`).
    The `conjunction` variable specifies whether the conditions should be chained together with an "and" operator,
    or an "or" operator.
    In the case of "and", True is returned if all `observations` are equal to `eval_against`.  Otherwise False.
    In the case of "or", True is returned if any one of the `observations` are equal to `eval_against`.
        If none of the `observations` are equal to `eval_against`, then False is returned.
    Therefore, in the end, a boolean value is finally returned.

    For Example:
    batch_evaluation(None, "and", a, b, c)
        True if a == b == c == None.  Otherwise False.

    batch_evaluation("Y", "or", e, f)
        True if e == "Y" or f == "Y" (also True if e == f == "Y").  Otherwise False.

    :param eval_against: The value against which all the elements in `*observations` are evaluated against.
    :param conjunction:  Of Dtype string.  Can only be either one of "and" or "or".
                         This is the logical operator sandwiched between the individual conditions.
    :param observations: A variable, or list of variables, which are evaluated against `eval_against`.
    :return:  A Boolean value, True or False.
    """
    ls_obs = [elem for elem in observations]
    if eval_against is None:
        ls_res = [elem is None for elem in ls_obs]

    else:
        ls_res = [elem == eval_against for elem in ls_obs]

    # "and" is coupled with the all(...) function.
    if conjunction.lower() == "and":
        eval_res = all(ls_res)

    # "or" is coupled with the any(...) function.
    elif conjunction.lower() == "or":
        eval_res = any(ls_res)

    # If the `else` clause is invoked, then the `conjunction` parameter was not specified correctly.
    else:
        raise Exception("The `conjunction` parameter can only be either one of 'and' or 'or'.\n"
                        "Please correct and retry...")

    return eval_res


def Behaviour_GBIPX(NDX, PER, DLQ_profile, GBIPX_profile):
    """
    This function generates:
        CNT_0, CNT_1, CNT_2, CNT_3, CNT_4, CNT_A, CNT_C  [based of the `DLQ_profile`]
        CNT_G, CNT_B, CNT_I, CNT_P, CNT_X                [based of the `GBIPX_profile`]
        variable                                         [based of the `CNT_<x>` variables]
    :param NDX:  The starting point in the DELINQUENCY/GBIPX strings.
                      Remember that: NDX_py = NDX_sas - 1.   > how to get the Python equivalent.
                      Alternatively: NDX_sas = NDX_py + 1.   > how to get the SAS equivalent.

                      NDX without any subscript is assumed to represent NDX_sas, NOT NDX_py.

                      NDX is 1-index based.

                      The Parameter Passed should be NDX and NOT INDEX.
    :param PER:  The number of points to inspect.
                 In other words, the number of months spanning a specific period.
    :param DLQ_profile:  N-Month Delinquency String Profile.  Of DType String.
    :param GBIPX_profile:  N-Month GBIPX String Profile.  Of Dtype String.
    :return:  variable, [CNT_G, CNT_B, CNT_I, CNT_P, CNT_X, CNT_0, CNT_1, CNT_2, CNT_3, CNT_4, CNT_A, CNT_C]
    """
    NDX_py = NDX - 1
    """
    A:  The set up of temporary Counters and their Initialisation:
    """
    CNT_G = 0
    CNT_B = 0
    CNT_I = 0
    CNT_P = 0
    CNT_X = 0

    CNT_0 = 0
    CNT_1 = 0
    CNT_2 = 0
    CNT_3 = 0
    CNT_4 = 0
    CNT_A = 0
    CNT_C = 0

    """
    B:  Traverse the DLQ_profile...
    """
    for i in range(NDX_py, NDX_py + PER):
        char = DLQ_profile[i].upper()  # The character under investigation in the current for-loop iteration.
        if char == "0":  # Up To Date.
            CNT_0 = CNT_0 + 1
        if char == "1":  # 30 Days.
            CNT_1 = CNT_1 + 1
        if char == "2":  # 60 Days.
            CNT_2 = CNT_2 + 1
        if char == "3":  # 90 Days.
            CNT_3 = CNT_3 + 1
        if char == "4":  # 120 Days.
            CNT_4 = CNT_4 + 1
        if char == "A":  # Adverse Behaviour.
            CNT_A = CNT_A + 1
        if char == "C":  # In Credit.
            CNT_C = CNT_C + 1

        """
        Count the GBIPX attributes over the period.
        """
        char_b = GBIPX_profile[i].upper()  # .upper() is a Belts-and-Braces approach.
        if char_b == "G":  # Good behaviour.
            CNT_G = CNT_G + 1
        if char_b == "B":  # Bad behaviour.
            CNT_B = CNT_B + 1
        if char_b == ".":  # Indeterminate.
            CNT_I = CNT_I + 1
        if char_b == "P":  # Paid-Up.
            CNT_P = CNT_P + 1
        if char_b == "X":  # Exclusion.
            CNT_X = CNT_X + 1

    """
    C.  Summarise the GBIPX behaviour over the period of `PER`.
        Determine the `GBIPX` streaks.
    """
    # `variable` is derived from the CNT_<GBIPX> variables.
    # `variable` summarises the Account GBIPX behaviour
    variable = ""  # In SAS, this is simply called &VAR.

    if CNT_X > 0:
        variable = "X-Exclusion"
    if CNT_P == PER:
        variable = "P-Paid-up"
    if CNT_I == PER:
        variable = "I-Missing"
    if CNT_G == PER:
        variable = "G-Good"
    if CNT_B == PER:
        variable = "B-Bad"
    if CNT_B > 0:
        variable = "B-Partial"
    if CNT_G > 0:
        variable = "G-Partial"
    if CNT_P > 0:
        variable = "P-Partial"
    """
    If `variable` at this point is still empty, then it must be assigned the following "I-Indeterminate" Catch-All.
    """
    if variable == "":
        variable = "I-Indeterminate"

    # If the counter at this stage is still 0, then revert it back to NULL to align with the SAS Code.
    if CNT_G == 0:
        CNT_G = None    # 1
    if CNT_B == 0:
        CNT_B = None    # 2
    if CNT_I == 0:
        CNT_I = None    # 3
    if CNT_P == 0:
        CNT_P = None    # 4
    if CNT_X == 0:
        CNT_X = None    # 5
    if CNT_0 == 0:
        CNT_0 = None    # 6
    if CNT_1 == 0:
        CNT_1 = None    # 7
    if CNT_2 == 0:
        CNT_2 = None    # 8
    if CNT_3 == 0:
        CNT_3 = None    # 9
    if CNT_4 == 0:
        CNT_4 = None    # 10
    if CNT_A == 0:
        CNT_A = None    # 11
    if CNT_C == 0:
        CNT_C = None    # 12


    # indices:      0      1      2      3      4      5      6      7      8      9      10     11
    ls = variable, [CNT_G, CNT_B, CNT_I, CNT_P, CNT_X, CNT_0, CNT_1, CNT_2, CNT_3, CNT_4, CNT_A, CNT_C]
    return ls


def drop_null_columns(pysdf, *ls_cols):
    """
    This function drops columns that only contains null values.
    :param pysdf:  A PySpark DataFrame.
    :ls_cols: The columns under investigation.
    """
    # print(f"pysdf.count() = {pysdf.count()}.")
    null_counts = pysdf \
        .select([f.count(f.when(f.col(c).isNull(), c)).alias(c) for c in ls_cols]) \
        .collect()[0] \
        .asDict()
    # print(f"Dictionary `null_counts` = {null_counts}.")
    pysdf_count = pysdf.count()
    to_drop = [k for k, v in null_counts.items() if v == pysdf_count]
    # print(f"List `to_drop` = {to_drop}.")
    pysdf = pysdf.drop(*to_drop)

    return pysdf


def Doubtful_Debt(Account_State_val, PIT):
    DoubtfulDebt1M = None
    DoubtfulDebt3M = None
    DoubtfulDebt6M = None
    DoubtfulDebt9M = None
    DoubtfulDebt12M = None
    if Account_State_val == "D. Doubtful Debt (Involuntary Churn)":
        if PIT > 12:
            DoubtfulDebt1M = 1
            DoubtfulDebt3M = 1
            DoubtfulDebt6M = 1
            DoubtfulDebt9M = 1
            DoubtfulDebt12M = 1
        if PIT > 9:
            DoubtfulDebt1M = 1
            DoubtfulDebt3M = 1
            DoubtfulDebt6M = 1
            DoubtfulDebt9M = 1
        if PIT > 6:
            DoubtfulDebt1M = 1
            DoubtfulDebt3M = 1
            DoubtfulDebt6M = 1
        if PIT > 3:
            DoubtfulDebt1M = 1
            DoubtfulDebt3M = 1
        if PIT > 1:
            DoubtfulDebt1M = 1
    return DoubtfulDebt1M, DoubtfulDebt3M, DoubtfulDebt6M, DoubtfulDebt9M, DoubtfulDebt12M


schema_Doubtful_Debt = t.StructType([
    t.StructField("DoubtfulDebt1M", t.IntegerType(), True),
    t.StructField("DoubtfulDebt3M", t.IntegerType(), True),
    t.StructField("DoubtfulDebt6M", t.IntegerType(), True),
    t.StructField("DoubtfulDebt9M", t.IntegerType(), True),
    t.StructField("DoubtfulDebt12M", t.IntegerType(), True)
])

udf_Doubtful_Debt = f.udf(Doubtful_Debt,
                          returnType=schema_Doubtful_Debt)


def Observation_GBIPX(PIT, NBR, DLQ_profile, GBIPX_profile):
    """
    A function that invokes `Behaviour_GBIPX`, which returns the
        1   GBIPX streaks (Captured in `OBS_NBR` below)
        2   CNT_<x> variables  (Captured in `ls_int` below)
    :param NBR: Numeric variable of dtype Integer.
                The NBR of observation periods to be inspected.
                This parameter indicates the 'period of each observation variable' created.
    :return:  Return two lists.
              ls_str = [OBS_NBR, GRP]
              ls_int = [DC0, DC1, DC2, DC3, DC4, ADV, CRD, GDS, BAD, PUP, CNT]
    """

    """
    A.  Invoke `Behaviour_GBIPX.py` that calculates the GBIPX Behaviour.
    It appears that `CNT_I` [2] and `CNT_X` [4] are not needed downstream, and are therefore excluded.
    """
    # OBS_&NBR
    OBS_NBR, ls_counters = Behaviour_GBIPX(NDX=PIT, PER=NBR, DLQ_profile=DLQ_profile, GBIPX_profile=GBIPX_profile)
    ls_str = [OBS_NBR]

    """
    Store the GBIPX attribute counts over the observation period, based on `NBR`.
    """
    GDS = ls_counters[0]  # OBS&NBR._GDS  # int
    BAD = ls_counters[1]  # OBS&NBR._BAD  # int
    PUP = ls_counters[3]  # OBS&NBR._PUP  # int

    """
    Store the DELINQUENCY attribute counts over the Observation Period, based on `NBR`.
    """
    DC0 = ls_counters[5]   # OBS&NBR._DC0  # int
    DC1 = ls_counters[6]   # OBS&NBR._DC1  # int
    DC2 = ls_counters[7]   # OBS&NBR._DC2  # int
    DC3 = ls_counters[8]   # OBS&NBR._DC3  # int
    DC4 = ls_counters[9]   # OBS&NBR._DC4  # int
    ADV = ls_counters[10]  # OBS&NBR._ADV  # int
    CRD = ls_counters[11]  # OBS&NBR._CRD  # int

    """
    Initialise a list that will store variables pertaining to `OBS_NBR`.
    This is a workaround for not being able to create `._attr` attributes as done in SAS.
    """
    if GDS is None:
        GDS = 0
    if BAD is None:
        BAD = 0
    if PUP is None:
        PUP = 0
    CNT = sum([GDS, BAD, PUP])  # OBS&NBR._CNT  # int
    if CNT == 0:
        CNT = None
    if GDS == 0:
        GDS = None
    if BAD == 0:
        BAD = None
    if PUP == 0:
        PUP = None

    # indices 0    1    2    3    4    5    6    7    8    9    10
    ls_int = [DC0, DC1, DC2, DC3, DC4, ADV, CRD, GDS, BAD, PUP, CNT]
    #         int  int  int  int  int  int  int  int  int  int  int

    """
    D.  Keep track as to how many GBP observation points are missing.
        We may want to exclude observations with missing records when performing analysis.
    """
    # Extract the value we are evaluating in the following conditional statements.
    val = ls_int[-1]  # `CNT` variable

    """
    Initialise the variable into which the output will be stored (RE: number of points missing).
    `GRP` is the GBP Count (Group).
    """
    GRP = ""  # OBS&NBR._GRP
    if (val == 0) or (val is None):
        GRP = "E. All OBS points missing"
    elif val < (NBR - 2):
        GRP = "D. 3+ missing OBS points"
    elif val == (NBR - 2):
        GRP = "C. 2 missing OBS points"
    elif val == (NBR - 1):
        GRP = "B. 1 missing OBS point"
    elif val == NBR:
        GRP = "A. No missing OBS point"
    else:
        pass

    """
    Expand the `ls_OBS_NBR` with the `GRP` variable created in the 'D.' Section.
    """
    ls_str.append(GRP)

    # indices   0        1
    # ls_str = [OBS_NBR, GRP]
    #           str      str

    # indices   0    1    2    3    4    5    6    7    8    9    10
    # ls_int = [DC0, DC1, DC2, DC3, DC4, ADV, CRD, GDS, BAD, PUP, CNT]
    #           int  int  int  int  int  int  int  int  int  int  int

    return ls_str, ls_int


schema_Observation_GBIPX = t.StructType([
    t.StructField("ls_str", t.ArrayType(t.StringType()), True),
    t.StructField("ls_int", t.ArrayType(t.IntegerType()), True)
])

udf_Observation_GBIPX = f.udf(Observation_GBIPX,
                              returnType=schema_Observation_GBIPX)


def Payment_Defaulters(PIT, DLQ_profile):
    """
    This function evaluates `PIT` and `DLQ_profile` to determine if a customer is a FID/SID/TID.

    When it comes to substring indexing, the following rule applies:
    PIT_py = PIT_sas - 1, equivalently
    PIT_sas = PIT_py + 1

    :return:
            `NextCycle30Days`   > Next Billing Cycle 30 Days (FID).    Binary indicator.  Type Integer.
            `SecondCycle60Days` > Second Billing Cycle 60 Days (SID).  Binary indicator.  Type Integer.
            `ThirdCycle90Days`  > Third Billing Cycle 90 Days (TID).   Binary indicator.  Type Integer.
    """
    # First, we need to Initialise all 3 Variables to None.
    NextCycle30Days = None    # int  # Next billing cycle 30 days   (FID).
    SecondCycle60Days = None  # int  # Second billing cycle 60 days (SID).
    ThirdCycle90Days = None   # int  # Third billing cycle 90 days  (TID).
    PIT_py = PIT - 1  # See the documentation above.

    if (PIT > 1) and (DLQ_profile[PIT_py - 1] == "1"):
        NextCycle30Days = 1
    if (PIT > 2) and (DLQ_profile[PIT_py - 2: PIT_py] == "21"):
        SecondCycle60Days = 1
    if (PIT > 3) and (DLQ_profile[PIT_py - 3: PIT_py] == "321"):
        ThirdCycle90Days = 1

    ls_pd = [NextCycle30Days, SecondCycle60Days, ThirdCycle90Days]

    return ls_pd


udf_Payment_Defaulters = f.udf(Payment_Defaulters,
                               returnType=t.ArrayType(t.IntegerType()))


def Performance_GBIPX(PIT, NBR, DLQ_profile, GBIPX_profile):
    """
    A function that ensures there are sufficient PERFORMANCE points to inspect.
    If not, then the routine will be aborted.
    We have 3 metrics over 1, 3, 6, 9, 12 months to measure churn.
        1   Bad Rates measures whether an account reaches 2+ over the above 5 periods.
            The 12-month period definition is for scorecard development and setting our credit risk policies.
        2   Paid-Up Rate measures whether an account voluntary churns.  You may notice in the code that bad rates and
            paid-up rates are mutually exclusive, and that bad rates trump paid-up rates.
        3   Doubtful debt is whetehr the account has reached 120+ days so that we can determine the financial impact of
            our credit policies, i.e. involuntary churn.  We have not had a consistent write-off indicator to date, and
            this will be used when we update credit policies going forward.
    :param NBR:  The NBR of performance periods to be inspected.
                 This parameter indicates the 'period of each performance variable' created.
    :return:  Returns two lists:
              ls_str = [PERF_NBR, GRP]
              ls_int = ls_int = [BadRateNBRM, DoubtfulDebtNBRM, PaidUpNBRM, GDS, BAD, PUP, CRD, CNT,
                                 goods_NBR, bads_NBR, paidup_NBR, GBP]
    """
    if PIT <= NBR:
        print("There are not sufficient PERFORMANCE points to inspect.\n"
              "In other words:  (PIT <= NBR), instead of (PIT > NBR).\n"
              "This routine is being aborted.")
        return None
    else:  # i.e. PIT > NBR
        """
        A.  Call the Python Script that calculates the GBIPX Behaviour
        """
        delta = PIT - NBR  # A positive integer.  delta >= 1.
        delta_py = delta - 1  # delta_py >= 0.
        PERF_NBR, ls_counters = Behaviour_GBIPX(NDX=delta_py, PER=NBR,
                                                DLQ_profile=DLQ_profile, GBIPX_profile=GBIPX_profile)
        ls_str = [PERF_NBR]

        """
        B. Create the variables that will hold the doubtful debt, bad rate, and paid-up rate.
        """
        BadRateNBRM = None  # 'Bad Rate' over &NBR of Months.  # Integer I suppose.

        CNT_4 = ls_counters[9]
        DoubtfulDebtNBRM = None  # 'Doubtful Debt' over &NBR of Months.  # Integer.
        if CNT_4 is not None:
            if CNT_4 >= 1:
                DoubtfulDebtNBRM = 1

        PaidUpNBRM = None  # The 'PaidUp Rate' over &NBR of Months.  # Supposedly Integer.


        """
        C.  Store the count of the GBIPX attributes over the performance period,
            as well as the Missed Payment Streaks.
        """
        GDS = ls_counters[0]   # PER&NBR._GDS  # int
        BAD = ls_counters[1]   # PER&NBR._BAD  # int
        PUP = ls_counters[3]   # PER&NBR._PUP  # int
        CRD = ls_counters[11]  # PER&NBR._CRD  # int
        if GDS is None:
            GDS = 0
        if BAD is None:
            BAD = 0
        if PUP is None:
            PUP = 0
        CNT = GDS + BAD + PUP  # PER&NBR._CNT  # int
        if CNT == 0:
            CNT = None
        if GDS == 0:
            GDS = None
        if BAD == 0:
            BAD = None
        if PUP == 0:
            PUP = None
        # In a section below, we compute the `GRP` variable.

        """
        D.  Initialise a list pertaining to `PERF_NBR`.
        """
        # ind     0            1                 2           3    4    5    6    7
        ls_int = [BadRateNBRM, DoubtfulDebtNBRM, PaidUpNBRM, GDS, BAD, PUP, CRD, CNT]

        """
        E.  Keep track of how many performance points are missing,
            as we may want to exclude observations with missing records when performing analysis.
        """
        # Extract the `value` that we are evaluating in the subsequent conditional statements:
        val = ls_int[7]  # List route.
        # Initialise the `GRP` variable into which the output will be stored:
        GRP = ""  # PER&NBR._GRP  # String
        if (val == 0) or (val is None):
            GRP = "E. All PER points missing"
        elif val < (NBR - 2):
            GRP = "D. 3+ missing PER points"
        elif val == (NBR - 2):
            GRP = "C. 2 missing PER points"
        elif val == (NBR - 1):
            GRP = "B. 1 missing PER point"
        elif val == NBR:
            GRP = "A. No missing PER point"
        else:
            pass

        """
        Expand the `ls_PERF_NBR` with the `PERF_NBR_GRP` variable created in the previous Section.
        """
        ls_str.append(GRP)

        # indices   0         1
        # ls_str = [PERF_NBR, GRP]

        """
        F.  Set the Goods, Bads & Paid-up counters.
            These are used when calculating the "Account Performance" for the "Risk Mandates".
        """
        goods_NBR = None  # Goods&NBR    # int
        bads_NBR = None  # Bads&NBR      # int
        paidup_NBR = None  # PaidUp&NBR  # int

        val_char = PERF_NBR[0]
        if val_char == "G":
            goods_NBR = 1
        elif val_char == "B":
            bads_NBR = 1
        elif val_char == "P":
            paidup_NBR = 1
        else:
            pass

        """
        Get the summation of the three counters above:
        """
        if goods_NBR is None:
            goods_NBR = 0
        if bads_NBR is None:
            bads_NBR = 0
        if paidup_NBR is None:
            paidup_NBR = 0
        GBP = sum([goods_NBR, bads_NBR, paidup_NBR])  # PER&NBR._GBP  # int
        if GBP == 0:
            GBP = None
        if goods_NBR == 0:
            goods_NBR = None
        if bads_NBR == 0:
            bads_NBR = None
        if paidup_NBR == 0:
            paidup_NBR = None

        """
        Expand `ls_PERF_NBR` with GBP
        """
        ls_int = ls_int + [goods_NBR, bads_NBR, paidup_NBR, GBP]

        # indices   0            1                 2           3    4    5    6    7    8          9         10
        # ls_int = [BadRateNBRM, DoubtfulDebtNBRM, PaidUpNBRM, GDS, BAD, PUP, CRD, CNT, goods_NBR, bads_NBR, paidup_NBR,
        #           GBP]
        #           11

        return ls_str, ls_int


schema_Performance_GBIPX = t.StructType([
    t.StructField("ls_str", t.ArrayType(t.StringType()), True),
    t.StructField("ls_int", t.ArrayType(t.IntegerType()), True)
])

udf_Performance_GBIPX = f.udf(Performance_GBIPX,
                              returnType=schema_Performance_GBIPX)


def set_spark_session(spark_session):
    global spark
    spark = spark_session


def Transition_Convert(VAR):
    """
    Evaluates the input `VAR` against many conditions.
    `VAR` is then updated based on the match it has made.
    IN ESSENCE, Transition_Convert.py expands the 2-character aging and dlq combinations into meaningful strings.
    :param VAR:  Based on the logic present in the `Account_Transition.py` script, it appears that `VAR` may be
            (i) `Transition_NDX_AGE`, or
            (ii) `Transition_NDX_DLQ`.
    This `VAR` is of dtype string.  In this scenario, it should be a 2-character combination.
    :return:  Return an updated `VAR` stored in the variable `VAR_return`.
    """

    """
    (i)
    Initialisation of the return variable (which is of dtype string):
    """
    VAR_return = None

    """
    (ii)
    UTD Accounts:
    """
    if VAR == "00":
        VAR_return = "0(=) UTD both periods"
    if VAR in ("01", "02", "03", "04", "0A"):
        VAR_return = "0(-) UTD: Rolled forward or adverse event next period"
    if VAR in ("0C", "0P"):
        VAR_return = "0(+) UTD: Credit balance or paid-up next period"

    """
    30-Day Accounts:
    (iii)
    """
    if VAR in ("10", "1C", "1P"):
        VAR_return = "1(+) 30 Days: Cured or credit balance or paid-up next period"
    if VAR == "11":
        VAR_return = "1(=) 30 Days both periods"
    if VAR in ("12", "13", "14", "1A"):
        VAR_return = "1(-) 30 Days: Rolled forward or adverse event next period"

    """
    60-Day Accounts:
    (iv)
    """
    if VAR in ("20", "2C"):
        VAR_return = "2(+) 60 Days: Cured or credit balance next period"
    if VAR in ("21", "2P"):
        VAR_return = "2(+) 60 Days: Rolled backward or paid-up next period"
    if VAR == "22":
        VAR_return = "2(=) 60 Days both periods"
    if VAR in ("23", "24", "2A"):
        VAR_return = "2(-) 60 Days: Rolled forward or adverse event next period"

    """
    (v)
    90-Day Accounts:
    """
    if VAR in ("30", "3C"):
        VAR_return = "3(+) 90 Days: Cured or credit balance next period"
    if VAR in ("31", "32", "3P"):
        VAR_return = "3(+) 90 Days: Rolled backward or paid-up next period"
    if VAR == "33":
        VAR_return = "3(=) 90 Days both periods"
    if VAR in ("34", "3A"):
        VAR_return = "3(-) 90 Days: Rolled forward or adverse event next period"

    """
    (vi)
    120-Day Accounts
    """
    if VAR in ("40", "41", "42", "43", "4C", "4P"):
        VAR_return = "4(+) 120 Days: Rolled backward or credit balance or paid-up next period"
    if VAR in ("44", "4A"):
        VAR_return = "4(=) 120 Days both periods or 120 Days and adverse next period"

    """
    (vii)
    Adverse-event Accounts:
    """
    if VAR == "AA":
        VAR_return = "5(=) Adverse both periods"
    if VAR in ("A0", "AC"):
        VAR_return = "5(+) Adverse: UTD or credit balance next period"
    if VAR in ("A1", "AP"):
        VAR_return = "5(+) Adverse: 30 days or paid-up next period"
    if VAR in ("A2", "A3", "A4"):
        VAR_return = "5(-) Adverse: 60+ days next period"

    """
    (viii)
    Credit-Balance Accounts
    """
    if VAR == "CC":
        VAR_return = "6(=) Credit Balance both periods"
    if VAR == "C0":
        VAR_return = "6(+) Credit Balance: UTD next period"
    if VAR in ("C1", "C2", "C3", "C4", "CA", "CP"):
        VAR_return = "6(-) Credit Balance: 30+ days or paid-up or adverse event next period"

    """
    (ix)
    Paid-up Accounts:
    """
    if VAR == "PP":
        VAR_return = "7(=) Paid-up both periods"
    if VAR in ("P0", "PC"):
        VAR_return = "7(+) Paid-up: UTD or credit balance next period"
    if VAR == "P1":
        VAR_return = "7(-) Paid-up: 30 days next period"
    if VAR in ("P2", "P3", "P4", "PA"):
        VAR_return = "7(-) Paid-up: 60+ days or adverse event next period"

    """
    (x)
    Insufficient performance information in either of the periods.
    """
    if VAR in ("--", ".."):
        VAR_return = "8(.) Insufficient performance information"

    """
    (xi)
    Exclusion accounts including deceased, doubtful debt, etc...
    """
    if VAR in ("DD", "XX"):
        VAR_return = "9(.) Exclusion account in either period"

    return VAR_return






