from pyspark.sql.types import StringType, IntegerType, StructType, StructField, DateType
from pyspark.sql.functions import udf
import math
from datetime import datetime
import numpy as np
from dateutil.relativedelta import relativedelta


PROF_TYPE_PROF_LENGTH_MAP = {
    "AGE": (1, 1, 60),      # no pipes between elements of 1 character each.  1 x 60 = 60.
    "DLQ": (1, 1, 60),      # no pipes between elements of 1 character each.  1 x 60 = 60.
    "GBIPX": (1, 1, 60),    # no pipes between elements of 1 character each.  1 x 60 = 60.
    "MOB": (2, 3, 180),     # pipes between elements of 2 characters each.    3 x 60 = 180.
    "BAL": (5, 6, 360),     # pipes between elements of 5 characters each.    6 x 60 = 360.
    "INS": (5, 6, 360),     # pipes between elements of 5 characters each.    6 x 60 = 360.
    "PMT": (5, 6, 360)      # # pipes between elements of 5 characters each.  6 x 60 = 360.
}


def behaviour_account(b_01, b_06, b_12, b_18, b_24):
    """
    Take the behaviour features {b_01, b_06, b_12, b_18, b_24} as input and return account-level behaviour attributes.
    :param b_01:  A 1-month behaviour feature.
    :param b_06:  A 6-month behaviour feature.
    :param b_12:  A 12-month behaviour feature.
    :param b_18:  An 18-month behaviour feature.
    :param b_24:  A 24-month behaviour feature.
    :return:  The following flag/binary columns are returned:
    [1] 'beh_exclusion': 1 if one of the behaviour features is equal to 'X-Exclusion'.  Else Null.
    [2] 'beh_missing': 1 if one of the behaviour features is equal to 'I-Missing'.  Else Null.
    [3] 'beh_adverse': 1 if one of the behaviour features is equal to 'B-Bad'.  Else Null.
    [4] 'beh_poor': 1 if one of [b_06, b_12, b_18, b_24] is 'B-Partial'.  Else Null.
    [5] 'beh_excellent': 1 if one of [b_06, b_12, b_18, b_24] is 'G-Good'.  Else Null
    [6] 'beh_good': 1 if one of [b_06, b_12, b_18, b_24] if 'G-Partial'.  Else Null.
    [7] 'beh_paidup': 1 if one of the behaviour features is equal to 'P-Paid-up'.  Else Null.
    [extra] Also, the string column `beh_account` is returned.
    `beh_account` can be one of the following:
    (1) 'X. Exclusions': if 'beh_exclusion' = 1.
    (2) 'D. Missing Record': if 'beh_missing' = 1.
    (3) 'F. Adverse Account': if 'beh_adverse' = 1.  (Involuntary Churn).
    (4) 'E. Poor Account': if 'beh_poor' = 1.
    (5) 'A. Excellent Account': if 'beh_excellent' = 1.
    (6) 'B. Good Account': if 'beh_good' = 1.
    (7) 'C. Paid-up Account': if 'beh_paidup' = 1.  (Voluntary Churn).
    """
    beh_exclusion = None
    beh_missing = None
    beh_adverse = None
    beh_poor = None
    beh_excellent = None
    beh_good = None
    beh_paidup = None

    def eval_b_xx_vars(behaviour_feature, *b_args):
        behaviour_feature = str(behaviour_feature).lower().strip()
        b_args = [str(b_arg).lower().strip() for b_arg in list(b_args)]
        for b_arg in b_args:
            if b_arg == behaviour_feature:
                return True
        else:
            return False

    # DO NOT CHANGE THE FOLLOWING SELECTION SEQUENCE.
    if eval_b_xx_vars("X-Exclusion", b_01, b_06, b_12, b_18, b_24):
        beh_account = "X. Exclusions"
        beh_exclusion = 1
    elif eval_b_xx_vars("I-Missing", b_01, b_06, b_12, b_18, b_24):
        beh_account = "D. Missing Record"
        beh_missing = 1
    elif eval_b_xx_vars("B-Bad", b_01, b_06, b_12, b_18, b_24):
        beh_account = "F. Adverse Account"
        beh_adverse = 1
    elif eval_b_xx_vars("B-Partial", b_06, b_12, b_18, b_24):
        beh_account = "E. Poor Account"
        beh_poor = 1
    elif eval_b_xx_vars("G-Good", b_06, b_12, b_18, b_24):
        beh_account = "A. Excellent Account"
        beh_excellent = 1
    elif eval_b_xx_vars("G-Partial", b_06, b_12, b_18, b_24):
        beh_account = "B. Good Account"
        beh_good = 1
    elif eval_b_xx_vars("P-Paid-up", b_01, b_06, b_12, b_18, b_24):
        beh_account = "C. Paid-up Account"
        beh_paidup = 1
    else:
        beh_account = None

    return beh_exclusion, beh_missing, beh_adverse, beh_poor, beh_excellent, beh_good, beh_paidup, beh_account


beh_acct_schema = StructType([
    StructField("BEH_Exclusion", IntegerType(), True),
    StructField("BEH_Missing", IntegerType(), True),
    StructField("BEH_Adverse", IntegerType(), True),
    StructField("BEH_Poor", IntegerType(), True),
    StructField("BEH_Excellent", IntegerType(), True),
    StructField("BEH_Good", IntegerType(), True),
    StructField("BEH_Paidup", IntegerType(), True),
    StructField("BEH_Account", StringType(), True)
])

udf_behaviour_account = udf(behaviour_account, returnType=beh_acct_schema)


def behaviour_gbipx(profile, n_months, point_in_time=1):
    """
    Traverse over the `profile` string `n_months` times; store the results in interim counters; return a description of
    the account behaviour.
    profile > F.col().  n_months > F.lit().  point_in_time > F.lit().
    :param profile: A string profile that is 60 characters long in length.
    :param point_in_time: An integer.
    :param n_months:  An integer that can be either {1, 6, 12, 18, 24}.
    Specifies the number of times we must traverse/iterate through the profile string inside the `for` loop.
    :return: The string variable `acc_beh_attr`, giving us a description of the behaviour of the account over the
    last `n_months`.
    `acc_beh_attr` can be either one of the following string values:
    ['X-Exclusion', 'P-Paid-Up', 'I-Missing', 'G-Good', 'B-Bad', 'B-Partial', 'G-Partial', 'P-Partial',
    'I-Indeterminate'].
    In the actual script, the results of this function are store into the variables 'b_01', 'b_06', 'b_12', 'b_18',
    'b_24'.
    """
    cnt_g = 0
    cnt_b = 0
    cnt_i = 0
    cnt_p = 0
    cnt_x = 0

    for i in range(point_in_time - 1, point_in_time + n_months - 1):
        char = str(profile[i: i + 1]).lower()  # extract a SINGLE character
        if char == "g":
            cnt_g += 1
        elif char == "b":
            cnt_b += 1
        elif char == ".":
            cnt_i += 1
        elif char == "p":
            cnt_p += 1
        elif char == "x":
            cnt_x += 1
        else:
            pass

    if cnt_x != 0:
        acc_beh_attr = "X-Exclusion"
    elif cnt_p == n_months:
        acc_beh_attr = "P-Paid-up"
    elif cnt_i == n_months:
        acc_beh_attr = "I-Missing"
    elif cnt_g == n_months:
        acc_beh_attr = "G-Good"
    elif cnt_b == n_months:
        acc_beh_attr = "B-Bad"
    elif cnt_b != 0:
        acc_beh_attr = "B-Partial"
    elif cnt_g != 0:
        acc_beh_attr = "G-Partial"
    elif cnt_p != 0:
        acc_beh_attr = "P-Partial"
    else:
        acc_beh_attr = "I-Indeterminate"

    return acc_beh_attr


udf_behaviour_gbipx = udf(behaviour_gbipx, returnType=StringType())


def cnt_pipes(txt):
    """
    Function that counts the number of pipes present in a text string.  By pipe I mean the '|' character.
    :param txt:  The input text that we need to count the number of pipes for.
    :return: The number, in integer format, of pipes that are present in the text string.
    """
    n = txt.count("|")
    return int(n)


udf_cnt_pipes = udf(cnt_pipes, returnType=IntegerType())


def date_comparison(dte_m0m, dte_m1m):
    """
    Function that evaluates two date values (which come from 2 columns) on whether they are Null or not, and
    returns information     based on that evaluation.  Note that it does not compare the 2 date values with each other.
    :param dte_m0m:  The name of the one column whose date values we need to examine on whether they are NULL or not.
    :param dte_m1m:  The name of the second columns whose date values we need to examine on whether they are NULL
    or not.
    :return: Return a short descriptive text on the status of the two date columns.
    """
    if (dte_m0m is None) and (dte_m1m is not None):
        return "1: Only M0M is Null"
    elif (dte_m0m is not None) and (dte_m1m is None):
        return "2: Only M1M is Null"
    elif (dte_m0m is None) and (dte_m1m is None):
        return "3: Both M0M and M1M are Null"
    elif dte_m0m == dte_m1m:
        return "4: M0M = M1M"
    elif dte_m0m < dte_m1m:
        return "5: M0M is historic wrt M1M"
    elif dte_m0m > dte_m1m:
        return "6: M0M more recent wrt M1M"
    else:
        return "7: Unaccounted situation"


udf_date_comparison = udf(date_comparison, returnType=StringType())


def delinquency_attributes(cust_status, cred_ctrl_group, sub_ctrl_group, balance, cd0, cd1, cd2, cd3, cd4):
    """
    Function that returns single-character strings that represent the Delinquency Status of the Client.
    :param cust_status:  The status of the customer as reflected in the original Service Line Age file.
    :param cred_ctrl_group:  A text description as reflected in the original Service Line Age file.
    :param sub_ctrl_group:  A text description as reflected in the original Service Line Age file.
    :param balance:  The overall outstanding balance of the customer.
    :param cd0:  Ageing Bucket 0.  I.e. month 0 (60 days).
    :param cd1:  Ageing Bucket 1.  I.e. month 1 (30 days).
    :param cd2:  Ageing Bucket 2.  I.e. month 2 (60 days).
    :param cd3:  Ageing Bucket 3.  I.e. month 3 (90 days).
    :param cd4:  Ageing Bucket 4.  I.e. month 4 (120 days).
    :return:
    [1] 'delinquency_attribute'
    A single-character string that is either one of ['A', 'C', 'P', 'D', '0', '1', '2', '3', '4'].
    'A' >  Adverse Delinquency because of, for example, Fraud, Debt Review, Administration, and so forth...
    'C' >  The customer's account is actually in Credit.  Therefore customer is not delinquent.
    'P' >  The customer's account is Paid Up.  I.e. Balance = 0.  This represents voluntary churn.
    'D' >  Deceased Customer.
    '0' >  Current_Value > 0 and Days_030 <= 0.  The Account falls into the 'CD0 (Current)' bucket.
    '1' >  Days_030 > 0 and Days_060 <= 0.  The account falls into the 'CD1 (30 days)' bucket.  Delinquent.
    '2' >  Days_060 > 0 and Days_090 <= 0.  The account falls into the 'CD2 (60 days)' bucket.  Moderately Delinquent.
    '3' >  Days_090 > 0 and Days_120 <= 0.  The account falls into the 'CD3 (90 days)' bucket.  Very Delinquent.
    '4' >  Days_120 > 0.  The account falls into the 'CD4 (120)' bucket.  Extremely Delinquent.
    [2] 'ageing_attribute', a single-character string that is either one of ['0', '1', '2', '3', '4', 'C', 'P'].
    '0' > (cd0 > 0) and (cd1 + cd2 + cd3 + cd4) <= 0.
    '1' > (cd1 > 0) and (cd2 + cd3 + cd4) <= 0.
    '2' > (cd2 > 0) and (cd3 + cd4) <= 0.
    '3' > (cd3 > 0) and (cd4 <= 0).
    '4' > (cd4 > 0).
    'C' > (balance < 0).  A negative balance means the account is in Credit.
    'P' > (balance == 0).  A zero balance means the account PaidUp.
    [3] 'gbipx_attribute', a single-character string that is either one of ['G', 'B', 'I', 'P', 'X'].
    'G' > If the `ageing_attribute` is either one of ['0', '1', 'C'], then 'G' which stands for 'Good'.
    'B' > Bad attribute.
    'I' > Indeterminate attribute.
    'P' > If the `ageing_attribute` is 'P', then 'P' which stands for 'Paid-Up'.
    'X' > The customer is deceased.
    [4] 'delinquency_trigger', a short text description explaining the status of the customer.
    """
    # Set the default attributes and trigger
    ageing_attribute = "-"
    delinquency_attribute = "-"
    gbipx_attribute = "?"
    delinquency_trigger = "LAST CASE: no description."

    # Concatenate the `cred_ctrl_group` and `sub_ctrl_group` to create `control_groups`:
    cred_ctrl_group = str(cred_ctrl_group).strip().upper()
    control_groups = str(str(cred_ctrl_group) + "|" + str(sub_ctrl_group)).strip().upper()

    # Generate an Account Age by working backwards, as the account could be highly in arrears,
    # with a zero (cleared) current balance.
    d_120 = cd4
    d_090 = d_120 + cd3
    d_060 = d_090 + cd2
    d_030 = d_060 + cd1

    # Determine the `ageing_attribute`:
    if cd4 > 0:
        ageing_attribute = "4"
    elif (cd3 > 0) and (d_120 <= 0):
        ageing_attribute = "3"
    elif (cd2 > 0) and (d_090 <= 0):
        ageing_attribute = "2"
    elif (cd1 > 0) and (d_060 <= 0):
        ageing_attribute = "1"
    elif (cd0 > 0) and (d_030 <= 0):
        ageing_attribute = "0"
    elif balance < 0:
        ageing_attribute = "C"
    elif balance == 0:
        ageing_attribute = "P"

    # Determine `gbipx_attribute`, `delinquency_attribute`, `delinquency_trigger`.
    # The Delinquency Attribute Sequence is critical.
    # Do not change the order.
    # >>> Exclusion Attributes (X) <<<
    if "DECEASED" in control_groups:
        gbipx_attribute = "X"
        delinquency_attribute = "D"
        delinquency_trigger = "A. Deceased"
    # >>> Bad Attributes (B) <<<
    elif "FRAUD" in control_groups:
        gbipx_attribute = "B"
        delinquency_attribute = "A"
        delinquency_trigger = "B. Fraud"
    elif ageing_attribute == "4":
        gbipx_attribute = "B"
        delinquency_attribute = "4"
        delinquency_trigger = "C. CD4 (120+ days)"
    elif ageing_attribute == "3":
        gbipx_attribute = "B"
        delinquency_attribute = "3"
        delinquency_trigger = "D. CD3 (90 days)"
    elif ageing_attribute == "2":
        gbipx_attribute = "B"
        delinquency_attribute = "2"
        delinquency_trigger = "E. CD2 (60 days)"
    elif "PRESCRIP" in control_groups:
        gbipx_attribute = "B"
        delinquency_attribute = "A"
        delinquency_trigger = "F. Debt Prescription"
    elif "REVIEW" in control_groups:
        gbipx_attribute = "B"
        delinquency_attribute = "A"
        delinquency_trigger = "G. Debt Review"
    elif "COUNCIL" in control_groups:
        gbipx_attribute = "B"
        delinquency_attribute = "A"
        delinquency_trigger = "G. Debt Review"
    elif "ADMIN" in control_groups:
        gbipx_attribute = "B"
        delinquency_attribute = "A"
        delinquency_trigger = "H. Administration, Insolvency & Liquidation"
    elif "INSOLVE" in control_groups:
        gbipx_attribute = "B"
        delinquency_attribute = "A"
        delinquency_trigger = "H. Administration, Insolvency & Liquidation"
    elif "LIQUID" in control_groups:
        gbipx_attribute = "B"
        delinquency_attribute = "A"
        delinquency_trigger = "H. Administration, Insolvency & Liquidation"
    elif "SALE" in control_groups:
        gbipx_attribute = "B"
        delinquency_attribute = "A"
        delinquency_trigger = "I. Debt Sale"
    elif "SOLD" in control_groups:
        gbipx_attribute = "B"
        delinquency_attribute = "A"
        delinquency_trigger = "I. Debt Sale"
    elif "AOD" in control_groups:
        gbipx_attribute = "B"
        delinquency_attribute = "A"
        delinquency_trigger = "J. Debt Recovery"
    elif "RECOVER" in control_groups:
        gbipx_attribute = "B"
        delinquency_attribute = "A"
        delinquency_trigger = "J. Debt Recovery"
    elif "ARRANGE" in control_groups:
        gbipx_attribute = "B"
        delinquency_attribute = "A"
        delinquency_trigger = "K. Payment Arrangement"
    elif "REHAB" in control_groups:
        gbipx_attribute = "B"
        delinquency_attribute = "A"
        delinquency_trigger = "K. Payment Arrangement"
    elif "LEGAL" in control_groups:
        gbipx_attribute = "B"
        delinquency_attribute = "A"
        delinquency_trigger = "L. Legal Action"
    elif "EDC" in control_groups:
        gbipx_attribute = "B"
        delinquency_attribute = "A"
        delinquency_trigger = "M. External Debt Collector"
    elif "ADRS" in control_groups:
        gbipx_attribute = "B"
        delinquency_attribute = "A"
        delinquency_trigger = "M. External Debt Collector"
    elif "BROOKS" in control_groups:
        gbipx_attribute = "B"
        delinquency_attribute = "A"
        delinquency_trigger = "M. External Debt Collector"
    elif "BLAKE" in control_groups:
        gbipx_attribute = "B"
        delinquency_attribute = "A"
        delinquency_trigger = "M. External Debt Collector"
    elif "CBS" in control_groups:
        gbipx_attribute = "B"
        delinquency_attribute = "A"
        delinquency_trigger = "M. External Debt Collector"
    elif "DALY" in control_groups:
        gbipx_attribute = "B"
        delinquency_attribute = "A"
        delinquency_trigger = "M. External Debt Collector"
    elif "HAMMOND" in control_groups:
        gbipx_attribute = "B"
        delinquency_attribute = "A"
        delinquency_trigger = "M. External Debt Collector"
    elif "MBD" in control_groups:
        gbipx_attribute = "B"
        delinquency_attribute = "A"
        delinquency_trigger = "M. External Debt Collector"
    elif "MMM" in control_groups:
        gbipx_attribute = "B"
        delinquency_attribute = "A"
        delinquency_trigger = "M. External Debt Collector"
    elif "MYBURG" in control_groups:
        gbipx_attribute = "B"
        delinquency_attribute = "A"
        delinquency_trigger = "M. External Debt Collector"
    elif "NDS" in control_groups:
        gbipx_attribute = "B"
        delinquency_attribute = "A"
        delinquency_trigger = "M. External Debt Collector"
    elif "NIMBLE" in control_groups:
        gbipx_attribute = "B"
        delinquency_attribute = "A"
        delinquency_trigger = "M. External Debt Collector"
    elif "NUDEBT" in control_groups:
        gbipx_attribute = "B"
        delinquency_attribute = "A"
        delinquency_trigger = "M. External Debt Collector"
    elif "PHOLOSA" in control_groups:
        gbipx_attribute = "B"
        delinquency_attribute = "A"
        delinquency_trigger = "M. External Debt Collector"
    elif "PLACEMENT" in control_groups:
        gbipx_attribute = "B"
        delinquency_attribute = "A"
        delinquency_trigger = "M. External Debt Collector"
    elif "REALPEOPLE" in control_groups:
        gbipx_attribute = "B"
        delinquency_attribute = "A"
        delinquency_trigger = "M. External Debt Collector"
    elif "R&W" in control_groups:
        gbipx_attribute = "B"
        delinquency_attribute = "A"
        delinquency_trigger = "M. External Debt Collector"
    elif "TMT" in control_groups:
        gbipx_attribute = "B"
        delinquency_attribute = "A"
        delinquency_trigger = "M. External Debt Collector"
    elif "VVM" in control_groups:
        gbipx_attribute = "B"
        delinquency_attribute = "A"
        delinquency_trigger = "M. External Debt Collector"
    elif "SUSPEND" in cust_status:
        gbipx_attribute = "B"
        delinquency_attribute = "A"
        delinquency_trigger = "N. Phone Suspended"
    elif "BARRING" in cust_status:
        gbipx_attribute = "B"
        delinquency_attribute = "A"
        delinquency_trigger = "O. Phone Barred"
    # >>> Good Attributes <<<
    elif ageing_attribute == "1":
        gbipx_attribute = "G"
        delinquency_attribute = "1"
        delinquency_trigger = "P. CD1 (30 days)"
    elif ageing_attribute == "0":
        gbipx_attribute = "G"
        delinquency_attribute = "0"
        delinquency_trigger = "Q. CD0 (Current)"
    # >>> Credit Attribute (C) <<<
    elif ageing_attribute == "C":
        gbipx_attribute = "G"
        delinquency_attribute = "C"
        delinquency_trigger = "R. Credit Balance"
    # >>> Paid-Up Attribute (P) <<<
    elif ageing_attribute == "P":  # The adverse code above ensures that these are true paid-ups.
        gbipx_attribute = "P"
        delinquency_attribute = "P"
        delinquency_trigger = "S. Paid-up Account"
    else:
        pass
    return ageing_attribute, gbipx_attribute, delinquency_attribute, delinquency_trigger


dlq_attrs_schema = StructType([
    StructField("Ageing_Attribute", StringType(), True),
    StructField("GBIPX_Attribute", StringType(), True),
    StructField("Delinquency_Attribute", StringType(), True),
    StructField("Delinquency_Trigger", StringType(), True)
])
udf_delinquency_attributes = udf(delinquency_attributes, returnType=dlq_attrs_schema)


def derive_first_mob(arr):
    """
    Given an input Months-On-Book String Profile, that is mostly empty, this function determines the right-most,
    non-null value.  If such a value exists, then derive the Month-01 Months-On-Book value, and return that value.
    If such a value does not exist, then the Month-01 Months-On-Book value cannot be determined,
    and a Null is returned as a result.
    """
    arr_1 = arr[:-1]
    ls_0 = arr_1.split("|")
    ls_1 = [int(chunk) if "." not in chunk else None for chunk in ls_0]
    ls_2 = ls_1[::-1]
    idx = next((i for i, val in enumerate(ls_2) if val), -999)
    if idx >= 0:
        mob_mth_1 = ls_2[idx] + 60 - idx - 1
    else:
        mob_mth_1 = None
    return mob_mth_1


udf_derive_first_mob = udf(derive_first_mob, returnType=StringType())


def derive_quarter(stamp_val):
    """
    A function that takes a yyyymm integer stamp, and converts the mm part to Quarter format.
    Say, for example, that the input is the integer 202006, then the output would be the string "2020Q2".
    :param stamp_val: A year-month stamp that is in integer format.  `stamp_val` must therefore contain
    6 digits.
    :return: A string containing (1) the year of `stamp_val` in string format, and (2) the Quarter in which
    the mm part of `stamp_val` gets classified into.
    """
    yyyy = str(stamp_val)[0:4]
    mm = str(stamp_val)[4:]
    if mm in ("01", "02", "03"):
        suffix = "Q1"
    elif mm in ("04", "05", "06"):
        suffix = "Q2"
    elif mm in ("07", "08", "09"):
        suffix = "Q3"
    else:
        suffix = "Q4"
    return yyyy + suffix


udf_derive_quarter = udf(derive_quarter, returnType=StringType())


def element_extraction(profile, n_chars_p_chunk, pipe_present=True, point_in_time=1):
    """
    Return the MONTH_01 element present in the profile.  This should be the left-most element.
    profile > F.col().  n_chars_p_chunk > F.lit().  point_in_time > F.lit().
    :param profile: A 60-month string profile.
    :param n_chars_p_chunk:  The number of characters (including pipes) there are per chunk.
    This depends on the profile being considered.
    :param pipe_present:  Whether | characteres are present inside the string profile, and act as delimiters.
    :param point_in_time: An integer equal to 1 by default.
    :return: Return the MONTH_01 element as a string.
    """
    point_in_time -= point_in_time  # MONTH_01 corresponds to Index 0.

    if pipe_present:
        element = profile[
                  point_in_time: point_in_time + n_chars_p_chunk - 1]  # We are not interested in the "|" delimiter.
    else:
        element = profile[point_in_time: point_in_time + n_chars_p_chunk]
    if "." in element:  # The MONTH_01 element is a missing value.
        return None
    else:
        return element


udf_element_extraction = udf(element_extraction, returnType=StringType())


def expand_id_info(idno):
    """
    Given an ID number, this function determines whether it is a valid South African ID Number.
    :param idno: Input ID Number in string format.
    :return: [1] IDFIX: A string of 13 characters. The corrected (and fully padded) version of the ID Number.
    [2] IDTYPE: A single-character string. The type of ID Number as per Bayan Dekker's definition in his SAS Code.
    [3] IDREPORT: A very important output.  It is a string that can be either one of ['N', 'X', 'U', 'O', '-'].
    In order to only retain customers with valid South-African IDNumbers, you need to filter your records on:
    IDREPORT in ['N', 'O'].  'N' represents valid SA IDNumbers in the 'New' format.
    'O' represents valid SA IDNumbers in the 'Old' format.
    [4] IDFAILURE: A short text description explaining why a specific IDNumber is not valid.
    An example is: 'Impossible Date of Birth'.
    """
    id_failure = ""
    idno = str(idno)

    if idno.endswith(".0"):
        idno = idno[:-2]  # Remove ".0" in case float conversion occurred in between.

    idno = idno.lstrip()  # Strip the IDNumber from whitespace on the left-hand side.

    def eliminate(orig_string, to_be_eliminated):
        return orig_string.translate({ord(c): None for c in to_be_eliminated})

    def compress(orig_string, width):
        val = orig_string.replace(" ", "").strip()
        val_1 = val.ljust(width)
        return val_1

    idfix = compress(idno, 15)

    # Sundry Corrections:  Clean up previously noted errors.
    if idfix[0:2].lower() == "ck":
        idfix = idfix[2:]  # Remove the "CK" from the beginning.
    if idfix[0] in [".", "/"]:
        idfix = idfix[1:]  # Remove the "." or "/" from the beginning.

    # ID first pass:
    if len(idfix.replace(" ", "").strip()) >= 6:
        if (len(idfix.strip()) == 13) and (eliminate(idfix, "0123456789 ") == "") \
                and (idfix[6:13].strip() == "0000000"):
            id_type = "G"
        elif (len(idfix.strip()) == 6) and (eliminate(idfix, "0123456789 ") == ""):
            id_type = "G"
        elif (len(idfix.strip()) == 13) and (eliminate(idfix, "0123456789 ") == ""):
            id_type = "I"
        elif (idfix[4] == idfix[11] == "/") and (len(idfix.strip()) == 14) \
                and (eliminate(idfix, "/0123456789 ") == "") and idfix[0:4] > "1800":
            id_type = "B"
        elif (idfix[4] == idfix[10] == "/") and (len(idfix.strip()) == 13) \
                and (eliminate(idfix, "/0123456789 ") == "") and (idfix[0:4] > "1800"):
            interim = idfix[0:5] + "0" + idfix[5:]
            idfix = compress(interim, 15)
            id_type = "b"
        elif (idfix[4] == idfix[9] == "/") and (len(idfix.strip()) == 12) \
                and (eliminate(idfix, "/0123456789 ") == "") and (idfix[0:4] > "1800"):
            interim = idfix[0:5] + "00" + idfix[5:]
            idfix = compress(interim, 15)
            id_type = "b"
        elif (len(idfix.strip()) == 12) and (eliminate(idfix, "0123456789 ") == "") and (idfix[0:4] > "1800"):
            interim = idfix[0:4] + "/" + idfix[4:10] + "/" + idfix[10:]
            idfix = compress(interim, 15)
            id_type = "b"
        elif (idfix[2] == idfix[9] == "/") and (len(idfix.strip()) == 12) and (eliminate(idfix, "/0123456789 ") == ""):
            interim = "00" + idfix
            idfix = compress(interim, 15)
            id_type = "b"
        elif (idfix[2] == idfix[8] == "/") and (len(idfix.strip()) == 11) and (eliminate(idfix, "/0123456789 ") == ""):
            interim = ("00" + idfix).rstrip()
            interim = interim[0:5] + "0" + interim[5:]
            idfix = compress(interim, 15)
            id_type = "b"
        elif (idfix[2] == idfix[7] == "/") and (len(idfix.rstrip()) == 10) and (eliminate(idfix, "/0123456789 ") == ""):
            interim = "00" + idfix
            interim = interim[0:5] + "00" + interim[5:]
            idfix = compress(interim, 15)
            id_type = "b"
        elif (idfix[0:2].upper() == "IT") and (idfix[2] in "0123456789"):
            id_type = "T"
        elif any(c.isalpha() for c in idfix):
            id_type = "P"
        else:
            id_type = "X"
            id_failure = "00 Unsure ID Type"
    elif (len(idfix.replace(" ", "").strip()) <= 1) or (idfix.strip() in [None, np.NaN, np.NAN, np.nan,
                                                                          False, "", ".", "None"]):
        id_failure = "01 Blank ID"
        id_type = "X"
    else:
        id_failure = "00 Short ID <= 5"
        id_type = "X"

    # CHECK 1: Validate DOB part of the ID number:
    if id_type == "I":
        try:
            datetime.strptime(idfix[0:6], "%y%m%d")
        except ValueError:
            id_failure = "02 Impossible Date of Birth"
    if id_type == "G":
        if len(idfix.strip()) == 13:
            try:
                datetime.strptime(idfix[0:6], "%y%m%d")
                id_failure = "03 Last 7 digits replaced by zeros"
            except ValueError:
                id_failure = "02 Impossible Date of Birth"
        else:
            try:
                datetime.strptime(idfix[0:6], "%y%m%d")
                id_failure = "04 Truncation of last 7 digits"
            except ValueError:
                id_failure = "02 Impossible Date of Birth"

    # CHECK 2: The MODULUS11 Check:
    if id_type == "I":
        oddno = int(idfix[0]) + int(idfix[2]) + int(idfix[4]) + int(idfix[6]) + int(idfix[8]) + int(idfix[10])

        even_nums = int(str(idfix[1]) + str(idfix[3]) + str(idfix[5]) +
                        str(idfix[7]) + str(idfix[9]) + str(idfix[11])) * 2
        sum_even_nums = 0
        for elem in str(even_nums):
            sum_even_nums += int(elem)
        total = sum_even_nums + oddno
        control_char = int(str(total)[len(str(total)) - 1])

        if control_char != 0:
            control_char = 10 - control_char

        if control_char == int(idfix[12]):
            pass
        else:
            id_failure = "05 Failed Modulus 11 Test"

    # CHECK 3:  Nationality and the recency of the IDNumber.
    if id_type == "I":
        selection = idfix[10:12]
        if selection in ["08", "09"]:
            id_type = "N"
        elif selection in ["00", "01", "02", "03", "04", "05", "06", "07"]:
            id_type = "n"
        elif selection in ["18", "19"]:
            id_type = "I"
        else:
            id_type = "i"

    # CHECK 4: Override 0 ID Numbers
    if all(c == "0" for c in idfix.strip()):
        id_failure = "06 Only zero digits"

    # Create `IDReport`.
    if id_failure == "" and id_type in ("I", "N"):
        id_report = "N"  # Valid consumer ID: New Format
    elif id_failure == "" and id_type in ("i", "n"):
        id_report = "O"  # Valid consumer ID: Old Format
    elif (id_failure == "01 Blank ID") or (id_failure == "06 Only zero digits") \
            or (id_failure == "03 Last 7 digits replaced by zeros") or (id_failure == "04 Truncation of last 7 digits"):
        id_report = "-"  # Blank Consumer ID Number
    elif id_failure == "00 Unsure ID Type":
        id_report = "U"
    elif (id_failure == "05 Failed Modulus 11 Test") or (id_failure == "02 Impossible Date of Birth"):
        id_report = "X"
    elif id_type in ["b", "B"]:
        id_report = "X"
    elif id_type == "T":
        id_report = "X"
    else:
        id_report = "X"

    idfix = idfix.replace(" ", "").strip()
    if idfix.lower() == "none":
        idfix = None
    return idfix, id_type, id_failure, id_report


idschema = StructType([StructField("IDFIX", StringType(), True),
                       StructField("IDTYPE", StringType(), True),
                       StructField("IDFAILURE", StringType(), True),
                       StructField("IDREPORT", StringType(), True)])


udf_expand_id = udf(expand_id_info, returnType=idschema)


def extract_idkey(idnum):
    """
    A function that extracts the first ten digits of a valid SA ID number.
    These first ten digits are coined as the `ID Key` of the ID Number.
    :param idnum: The South African ID Number, in string format.
    :return:  Returns the first 10 digits of the input ID Number.
    """
    if idnum in [None, "", np.nan, np.NaN, np.NAN]:
        return idnum
    else:
        try:
            return str(idnum[0: 10])
        except IndexError:
            return idnum


udf_extract_idkey = udf(extract_idkey, returnType=StringType())


def finalise_aad(dte_m0m, dte_m1m):
    """
    A function that based on the date values from 2 columns decides which one to use.
    In essence, it finalises the value of the Account Activation Date of a particular account.
    :param dte_m0m: The name of the one column whose date values we need to examine.
    :param dte_m1m: The name of the second columns whose date values we need to examine.
    :return: Returns one of the two dates to use as the Account Activation Date.  In the worst case in which both
    dte_m0m and dte_m1m are NULL, the function will return a NULL value.
    """
    if (dte_m0m is None) and (dte_m1m is not None):
        return dte_m1m
    elif (dte_m0m is not None) and (dte_m1m is None):
        return dte_m0m
    elif (dte_m0m is None) and (dte_m1m is None):
        return None
    elif dte_m0m == dte_m1m:
        return dte_m0m
    elif dte_m0m < dte_m1m:
        return dte_m0m
    elif dte_m0m > dte_m1m:
        return dte_m1m
    else:
        return None


udf_finalise_aad = udf(finalise_aad, returnType=DateType())


def generate_mob_prof(mob_start):
    """
    Given the Months-On-Book value, in integer format, for Month-01, the function generates a full string profile for
    the entire observation period of 60 Months.
    :param mob_start: The Month-01 Months-On-Book value.
    :return: A 60-month string profile representing the Months-On-Book profile over a period of 60 months.
    """
    ls_raw_mob = [mob_start]
    x = mob_start
    for i in range(59):
        x -= 1
        ls_raw_mob.append(x)
    ls_fix_mob = [None if x < 0 else x if x <= 99 else 99 for x in ls_raw_mob]
    ls_str_mob = [".." if x is None else str(x).zfill(2) for x in ls_fix_mob]
    str_prof_1 = "|".join(ls_str_mob) + "|"
    return str_prof_1


udf_generate_mob_prof = udf(generate_mob_prof, returnType=StringType())


def get_beh_customer(beh_exclusion, mob_immature, beh_excellent, beh_good,
                     beh_paidup, beh_poor, beh_adverse, beh_missing, mob_missing):
    """
    Function that takes many counter variables as input, performs conditional logic evaluation on them,
    and returns a string to classify the payment behaviour of a customer.  The returned string can be one of 8 values:
    (1) "EA [Exclusion Account]"
    (2) "IB [Immature Payment Behaviour]"
    (3) "EB [Excellent Payment Behaviour]"
    (4) "GB [Good Payment Behaviour]"
    (5) "PB [Poor Payment Behaviour]"
    (6) "AB [Adverse Payment Behaviour]"
    (7) "MR [Missing Billing Records]"
    (8) "ND [No Account Opening Date]"
    :param beh_exclusion:  0 / positive integer.
      Number or accounts associated with person/IDNumber classified as "Deceased".
    :param mob_immature:  0 / positive integer.
      Number of immature accounts associated with a person/IDNumber.
    :param beh_excellent:  0 / positive integer.
      Number of excellent accounts associated with a person/IDNumber.
    :param beh_good:  0 / positive integer.
      Number of good accounts associated with a person/IDNumber.
    :param beh_paidup:  0 / positive integer.
      Number of PaidUp accounts associated with a person/IDNumber.
    :param beh_poor:  0 / positive integer.
      Number of Poor Aaccounts associated with a person/IDNumber.
    :param beh_adverse:  0 / positive integer.
      Number of Adverse Accounts associated with a person/IDNumber.
    :param beh_missing:  0 / positive integer.
      Number of Missing Accounts (due to missing records) associated with a person/IDNumber.
    :param mob_missing:  0 / positive integer.
      Number of accounts with unknown Account Opening Date associated with a person/IDNumber.
    :return BEH_Customer:  A string that categorises the payment behaviour of a customer into one of 8 buckets.
    """
    if beh_exclusion >= 1:
        return "EA [Exclusion Account]"
    elif mob_immature >= 1:
        return "IB [Immature Payment Behaviour]"
    elif beh_excellent >= 1:
        return "EB [Excellent Payment Behaviour]"
    elif (beh_good >= 1) or (beh_paidup >= 1):
        return "GB [Good Payment Behaviour]"
    elif beh_poor >= 1:
        return "PB [Poor Payment Behaviour]"
    elif beh_adverse >= 1:
        return "AB [Adverse Payment Behaviour]"
    elif beh_missing >= 1:
        return "MR [Missing Billing Records]"
    elif mob_missing >= 1:
        return "ND [No Account Opening Date]"
    else:
        return None


udf_get_beh_customer = udf(get_beh_customer, returnType=StringType())


def intel_truncate(val):
    """
    This function divides the input value by 100, and returns the result in Integer format.
    This is done to convert Cents to Rands.
    :param val:  The input value that needs to be divided by 100.  Can be either an integer or a float.
    :return:  The value after division by 100 and conversion to integer format.
    """
    if val in [None, "", ".", np.nan, np.NaN, np.NAN]:
        return None
    if val == 0:
        return int(0)
    elif val > 0:
        return int(math.floor(val / 100))
    elif val <= -100:
        return int(math.ceil(val / 100))
    elif -100 < val < 0:
        return int(0)
    else:
        return None


udf_intel_truncate = udf(intel_truncate, returnType=IntegerType())


def make_proxy_date(monthid_stamp):
    """
    Make a Proxy Date from an Integer that is in the format of ccyymm.  E.g.: 202108.
    :param monthid_stamp: A year-month stamp in integer format.
    :return: Return a proxy date that is in date format.  The day of the date is defaulted to "1".
    """
    mid_str = str(monthid_stamp)
    yyyy = int(mid_str[:4])
    mm = int(mid_str[4:])
    dte_proxy = datetime.date(year=yyyy, month=mm, day=1)
    return dte_proxy


udf_make_proxy_date = udf(make_proxy_date, returnType=DateType())


def mob_category(profile, point_in_time=1):
    """
    From the MONTH_01 (i.e. index 0) element of MOB_profile, determine whether the Account is
    {"missing", "immature", "established"}.
    profile > F.col().  point_in_time > F.lit().
    :param profile: The `MOB_profile` that is in the format of "..|..|..|".  I.e., 3 characters per chunk.
    :param point_in_time: An Integer equal to 1 by default.
    :return:  Return the flag values `mob_missing`, `mob_immature`, `mob_established`.
    Only one of these can be equal to 1 for any given record.  The rest should be equal to 0/Null.
    """
    mob_missing = None
    mob_immature = None
    mob_established = None

    point_in_time -= 1
    mob_element = profile[
                  point_in_time: point_in_time + 3 - 1]  # we are not interested in the trailing "|" character.
    if mob_element == "..":
        mob_missing = 1
    elif mob_element in ("00", "01", "02", "03", "04", "05"):
        mob_immature = 1
    else:
        mob_established = 1

    return mob_element, mob_missing, mob_immature, mob_established


mob_category_schema = StructType([
    StructField("Account_MOB", StringType(), True),
    StructField("MOB_Missing", IntegerType(), True),
    StructField("MOB_Immature", IntegerType(), True),
    StructField("MOB_Established", IntegerType(), True)
])

udf_mob_category = udf(mob_category, returnType=mob_category_schema)


def mback_from_dte(dte_val, ref_dte):
    """
    Calculate how many number of months "back into past" `dte_val` is from `ref_dte`.
    :param dte_val: The starting point value.  A column that is of Date Type.
    :param ref_dte: The reference date.  Date Type.
    :return: The difference between dte_val and ref_dte in terms of months.
    """
    year_ref_dte = ref_dte.year
    month_ref_dte = ref_dte.month
    del_1 = (year_ref_dte - dte_val.year) * 12
    del_2 = month_ref_dte - dte_val.month
    return del_1 + del_2


udf_mback_from_dte = udf(mback_from_dte, returnType=IntegerType())


def norm_contact_number(number_1):
    """
    Takes an input contact number, and then sanitizes it to the correct format.
    We want all contact numbers in a column to conform to the same notation.
    :param number_1: The raw, input contact number.
    Can be of type integer, float, or string.
    :return: A sanitized contact number.
    """
    if number_1 in [None, np.nan, "", "."]:
        return None

    number_2 = str(number_1)
    if number_2.endswith(".0"):
        number_2 = number_2[:-2]

    if len(number_2) == 9:
        return "0" + number_2
    elif (len(number_2) == 10) and (number_2[0] == "0"):
        return number_2
    elif (number_2[0:3] == "+27") and (len(number_2) == 12):
        return "0" + number_2[3:]
    elif (number_2[0:2] == "27") and (len(number_2) == 11):
        return "0" + number_2[2:]
    else:
        return None


udf_norm_contact_number = udf(norm_contact_number, returnType=StringType())


def normalise_profile(profile, type_name, mapping_dict=PROF_TYPE_PROF_LENGTH_MAP):
    """
    A function that ensures that any 60-MONTH string profile satisfies certain requirements.
    It can be used to: [1] truncate redundant chunks from the string (therefore making it of the right length),
    [2] Initialise empty profiles  for new accounts, [3] ensure the correct number of pipes are
    present in the string profile.
    :param profile: An input string profile that is supposed to represent attributes of an account
    over 60 months.
    :param type_name: A string that specifies what type of string profile we are dealing with.
    Can be either one of ['AGE', 'DLQ', 'GBIPX', 'MOB', 'BAL', 'INS', 'PMT'].
    'AGE' > represents Ageing Attribute Profile.
    'DLQ' > represents Delinquency Attribute Profile.
    'GBIPX' > represents 'Good' / 'Bad' / 'Indeterminate' / 'Paidup' / 'Deceased' Attribute Profile.
    'MOB' > represents the Months-on-Book Profile.
    'BAL' > represents the BALANCE Profile.
    'INS' > represents the Instalment Profile.
    'PMT' > Represents the Payment Profile.
    This key is used to look up supplementary data in a dictionary about the total profile length,
    the length per chunk, and length per element.
    :param mapping_dict: The dictionary that is used to consult additional information about the string profile.
    :return:  A string profile that is [1] of the correct length, and [2] contains the correct amount of pipes
    as derived from the `mapping_dict`.
    """
    length_max = mapping_dict.get(type_name)[2]
    length_p_chunk = mapping_dict.get(type_name)[1]
    length_p_element = mapping_dict.get(type_name)[0]
    if profile in (None, "", np.nan, np.NaN, np.NAN):
        if length_p_element <= 1:
            profile_normed = "." * length_max
        else:
            profile_chunk = "." * length_p_element + "|"
            profile_normed = profile_chunk * 60
        return profile_normed
    # Get string length of the profile.
    length_current = len(profile)
    if length_current == 0:  # CASE 1:  Entire profile is blank / missing / null.
        if length_p_element <= 1:
            profile_normed = "." * length_max
        else:
            profile_chunk = "." * length_p_element + "|"
            profile_normed = profile_chunk * 60
        return profile_normed
    elif 0 < length_current < length_max:  # CASE 2:  Profile is not entirely full.
        deficit_chars = length_max - length_current
        deficit_chunks = deficit_chars / length_p_chunk
        if length_p_element <= 1:
            add_on = "." * int(deficit_chunks)
            profile_normed = profile + add_on
        else:
            add_on_chunk = "." * length_p_element + "|"
            add_on = add_on_chunk * int(deficit_chunks)
            profile_normed = profile + add_on
        return profile_normed
    elif length_current == length_max:  # CASE 3:  Profile is of exactly the right length.
        return profile
    else:  # i.e. length_current > length_max  # CASE 4:  Profile is too long.
        profile_truncated = profile[0: length_max]
        return profile_truncated


udf_normalise_profile = udf(normalise_profile, returnType=StringType())


def number_to_padded_text(num_value, n_chars):
    """
    Takes in some numeric value, and returns a string that is 5 characters in total width.
    If necessary, pads on the left-hand side with zeroes.
    :param num_value:  The input value that needs to be converted to a string.
    :param n_chars:  The number of characters in the final text string.
    :return:  A string representation of `num_value` that is exactly of character width five in length.
    """
    if (num_value in (None, np.nan, np.NAN, np.NaN)) and (n_chars == 5):  # BOTH conditions need to be satisfied.
        return "....."
    if (num_value in (None, np.nan, np.NAN, np.NaN)) and (n_chars == 2):  # BOTH conditions need to be satisfied.
        return ".."
    if (num_value in (None, np.nan, np.NAN, np.NaN)) and (n_chars == 1):  # BOTH conditions need to be satisfied.
        return "."
    else:
        if n_chars == 5:
            num_value = int(num_value)
            if num_value < -9999:
                return "-9999"
            elif num_value == 0:
                return "00000"
            elif num_value > 99999:
                return "99999"
            else:
                return "{0:05}".format(num_value)
        elif n_chars == 2:
            num_value = int(num_value)
            if num_value < -9:
                return "-9"
            elif num_value is None:  # A Flag value indicating MOB could not be deduced.
                return ".."
            elif num_value == 0:
                return "00"
            elif num_value > 99:
                return "99"
            else:
                return "{0:02}".format(num_value)
        elif n_chars == 1:
            return str(num_value)
        else:
            raise ValueError("`n_chars` is not one of allowed values.")


udf_number_to_padded_text = udf(number_to_padded_text, returnType=StringType())


def starting_mob(acctact_dte, statement_dte):
    """
    Determines the Month-01 value for Months-On-Book.  It does so by determining the number of elapsed months from the
    account activation date till the statement date.
    :param acctact_dte: The Account Activation Date, in date format.
    :param statement_dte: The Statement-Month End Date, in date format.
    :return: The Month-01 Months-On-Book value, in integer format.
    """
    if acctact_dte is None:
        return -1
    delta = relativedelta(statement_dte, acctact_dte)
    return int(delta.years * 12 + delta.months)


udf_starting_mob = udf(starting_mob, returnType=IntegerType())


def str_length(txt):
    """
    A function that specifies the number of characters (also known as the length) in a string.
    :param txt:  Any text that is, of course, in String format.
    :return:  The length of the string.
    """
    assert type(txt) == str, "The input to function `str_length` must be of type String."
    return len(txt)


udf_str_length = udf(str_length, returnType=IntegerType())
