import re
import pyspark.sql.functions as f
import pyspark.sql.types as t


def Filter_Administration(SEQ, Delinquency_Trigger, Filter_Waterfall):
    # Set some default values to be returned:
    Filter_Administration = None
    Administration_PER = None
    Administration_DLQ_Trigger = None
    Administration_Credit_Bureau = None

    if Delinquency_Trigger == "H. Administration, Insolvency & Liquidation":
        if Filter_Waterfall == "":
            cat = str(SEQ) + " Consumer under Administration, Insolvency, Liquidation"
            Filter_Waterfall = re.sub(" +", " ", cat)
        Filter_Administration = 1  # Consumer under Administration, Insolvency, Liquidation
        Administration_PER = 1  # % number of accounts flagged as AIL
        Administration_DLQ_Trigger = 1  # Delinquency trigger customer under AIL
        Administration_Credit_Bureau = None  # Credit bureau identified customer as under AIL
    return (Filter_Waterfall, Filter_Administration, Administration_PER,
            Administration_DLQ_Trigger, Administration_Credit_Bureau)


schema_Filter_Administration = t.StructType([
    t.StructField("FILTER_WATERFALL", t.StringType(), True),
    t.StructField("FILTER_ADMINISTRATION", t.IntegerType(), True),
    t.StructField("ADMINISTRATION_PER", t.IntegerType(), True),
    t.StructField("ADMINISTRATION_DLQ_TRIGGER", t.IntegerType(), True),
    t.StructField("ADMINISTRATION_CREDIT_BUREAU", t.IntegerType(), True)
])

udf_Filter_Administration = f.udf(Filter_Administration, returnType=schema_Filter_Administration)


def Filter_Bad_Debt(SEQ, BAD_Month, Analysis_A, Analysis_B, Filter_Waterfall):
    """
    Filter accounts when the account is in bad debt.
    """

    # Set some default values of variables to be returned:
    Bad_Debt_Extract_File = None
    Bad_Debt_Analysis_A = None
    Bad_Debt_Analysis_B = None
    Filter_Bad_Debt = None
    Bad_Debt_PER = None
    if (BAD_Month is not None) or (Analysis_A == "PREWRITEOF") or (Analysis_B == "PRE_LEGAL"):
        if Filter_Waterfall == "":
            cat = str(SEQ) + " Account involuntary churned (120-239 Days)"
            Filter_Waterfall = re.sub(" +", " ", cat)
        Filter_Bad_Debt = 1  # Account involuntary churned (120-239 Days)
        Bad_Debt_PER = 1  # % number of accounts flagged as bad debt (120-239 days)
        if BAD_Month is not None:
            Bad_Debt_Extract_File = None  # Bad debt extract file
        if Analysis_A == "PREWRITEOF":
            Bad_Debt_Analysis_A = 1  # ANALYSIS_A = PREWRITEOF
        if Analysis_B == "PRE_LEGAL":
            Bad_Debt_Analysis_B = 1  # ANALYSIS_B = PRE_LEGAL

        """
        The following is marked as `TMP`:
        """
        if Analysis_A == "PREWRITEOF":
            Analysis_A = "---"
        if (Analysis_A == "COLLECTION") and (Analysis_B == "PRE_LEGAL"):
            Analysis_A = "---"
        if Analysis_B == "PRE_LEGAL":
            Analysis_B = "---"

    return (Filter_Waterfall, Filter_Bad_Debt, Bad_Debt_PER, Bad_Debt_Extract_File,
            Bad_Debt_Analysis_A, Bad_Debt_Analysis_B, Analysis_A, Analysis_B)


schema_Filter_Bad_Debt = t.StructType([
    t.StructField("FILTER_WATERFALL", t.StringType(), True),
    t.StructField("FILTER_BAD_DEBT", t.IntegerType(), True),
    t.StructField("BAD_DEBT_PER", t.IntegerType(), True),
    t.StructField("BAD_DEBT_EXTRACT_FILE", t.IntegerType(), True),
    t.StructField("BAD_DEBT_ANALYSIS_A", t.IntegerType(), True),
    t.StructField("BAD_DEBT_ANALYSIS_B", t.IntegerType(), True),
    t.StructField("ANALYSIS_A", t.StringType(), True),
    t.StructField("ANALYSIS_B", t.StringType(), True)
])

udf_Filter_Bad_Debt = f.udf(Filter_Bad_Debt, returnType=schema_Filter_Bad_Debt)


def Filter_Business_Subscriptions(SEQ, PER, CHN_Business, Contracts, Filter_Waterfall):
    """
    Filter accounts when subscriptions from a business.
    """

    # Make some default assignments:
    Business_Subscriptions = None
    Filter_Business = None
    Business_PER = None

    if CHN_Business >= 1:
        Business_Subscriptions = CHN_Business  # Number business subscriptions
        if CHN_Business >= int(PER * Contracts / 100.0):
            cat = str(SEQ) + " " + str(PER) + "% business subscriptions"
            Filter_Waterfall = re.sub(" +", " ", cat)
            Filter_Business = 1  # Flagged as a business account
            Business_PER = CHN_Business  # % business originated subscriptions
    return (Business_Subscriptions, Filter_Waterfall, Filter_Business, Business_PER)


schema_Filter_Business_Subscriptions = t.StructType([
    t.StructField("BUSINESS_SUBSCRIPTIONS", t.IntegerType(), True),
    t.StructField("FILTER_WATERFALL", t.StringType(), True),
    t.StructField("FILTER_BUSINESS", t.IntegerType(), True),
    t.StructField("BUSINESS_PER", t.IntegerType(), True),
])

udf_Filter_Business_Subscriptions = f.udf(Filter_Business_Subscriptions,
                                          returnType=schema_Filter_Business_Subscriptions)


def Filter_CD0_Collection(CD0_NBR, Analysis_A, Filter_Waterfall):
    """
    Filter UTD accounts when Infinity identifies them as in Collections.
    """

    # Set some default values:
    Filter_CD0_Collection = None
    CD0_Collection_PER = None

    if (CD0_NBR == 1) and (Analysis_A == "COLLECTION"):
        if Filter_Waterfall == "":
            Filter_Waterfall = "0X UTD account flagged as being in COLLECTION by Infinity"
        Filter_CD0_Collection = 1  # UTD account flagged as being in COLLECTION by Infinity
        CD0_Collection_PER = 1  # % UTD account flagged as being in COLLECTION by Infinity
        Analysis_A = "---"
    return (Filter_Waterfall, Filter_CD0_Collection, CD0_Collection_PER, Analysis_A)


schema_Filter_CD0_Collection = t.StructType([
    t.StructField("FILTER_WATERFALL", t.StringType(), True),
    t.StructField("FILTER_CD0_COLLECTION", t.IntegerType(), True),
    t.StructField("CD0_Collection_PER", t.IntegerType(), True),
    t.StructField("ANALYSIS_A", t.StringType(), True)
])

udf_Filter_CD0_Collection = f.udf(Filter_CD0_Collection, returnType=schema_Filter_CD0_Collection)


def Filter_CD0_Current(CD0_NBR, Analysis_A, Filter_Waterfall):
    """
    Filter UTD accounts and not in Infinity collections.
    """

    # Set some default values:
    Filter_CD0_Current = None
    CD0_Current_PER = None

    if (CD0_NBR == 1) and (Analysis_A == "CURRENT"):
        if Filter_Waterfall == "":
            Filter_Waterfall = "00 UTD account and CURRENT per Infinity"
        Filter_CD0_Current = 1  # UTD account and CURRENT per Infinity.
        CD0_Current_PER = 1  # % UTD account and CURRENT per Infinity.
        # The following is marked as `TMP`:
        Analysis_A = "---"

    return (Filter_Waterfall, Filter_CD0_Current, CD0_Current_PER, Analysis_A)


schema_Filter_CD0_Current = t.StructType([
    t.StructField("FILTER_WATERFALL", t.StringType(), True),
    t.StructField("FILTER_CD0_CURRENT", t.IntegerType(), True),
    t.StructField("CD0_CURRENT_PER", t.IntegerType(), True),
    t.StructField("ANALYSIS_A", t.StringType(), True)
])

udf_Filter_CD0_Current = f.udf(Filter_CD0_Current, returnType=schema_Filter_CD0_Current)


def Filter_CDX_Collection(CD1_NBR, CD2_NBR, CD3_NBR, CD4_NBR, Analysis_A, Filter_Waterfall, Aging_Attribute):
    """
    Filter delinquent accounts when Infinity also identifies them as being in collections.
    """
    # Set some default values:
    Filter_CDX_Collection = None
    CDX_Collection_PER = None

    if (CD1_NBR is None): CD1_NBR = 0
    if (CD2_NBR is None): CD2_NBR = 0
    if (CD3_NBR is None): CD3_NBR = 0
    if (CD4_NBR is None): CD4_NBR = 0
    sum_CDX = CD1_NBR + CD2_NBR + CD3_NBR + CD4_NBR

    if (sum_CDX == 1) and (Analysis_A == "COLLECTION"):
        if Filter_Waterfall == "":
            cat = str(Aging_Attribute) + "X Delinquent account flagged as being in COLLECTION by Infinity"
            Filter_Waterfall = re.sub(" +", " ", cat)
        Filter_CDX_Collection = 1  # Delinquent account flagged as being in COLLECTION by Infinity
        CDX_Collection_PER = 1  # % Delinqeunt account flagged as being in COLLECTION by Infinity
        # The following is being marked as `TMP`:
        Analysis_A = "---"

    return Filter_Waterfall, Filter_CDX_Collection, CDX_Collection_PER, Analysis_A


schema_Filter_CDX_Collection = t.StructType([
    t.StructField("FILTER_WATERFALL", t.StringType(), True),
    t.StructField("FILTER_CDX_COLLECTION", t.IntegerType(), True),
    t.StructField("CDX_COLLECTION_PER", t.IntegerType(), True),
    t.StructField("ANALYSIS_A", t.StringType(), True)
])

udf_Filter_CDX_Collection = f.udf(Filter_CDX_Collection, returnType=schema_Filter_CDX_Collection)


def Filter_CDX_Current(CD1_NBR, CD2_NBR, CD3_NBR, CD4_NBR, Analysis_A, Filter_Waterfall, Aging_Attribute):
    """
    Filter accounts that are in arrears when Infinity identifies them as being CURRENT
    """

    # Set some Default Values:
    Filter_CDX_Current = None
    CDX_Current_PER = None

    if (CD1_NBR is None): CD1_NBR = 0
    if (CD2_NBR is None): CD2_NBR = 0
    if (CD3_NBR is None): CD3_NBR = 0
    if (CD4_NBR is None): CD4_NBR = 0

    sum_CDX = CD1_NBR + CD2_NBR + CD3_NBR + CD4_NBR

    if (sum_CDX == 1) and (Analysis_A == "CURRENT"):
        if Filter_Waterfall == "":
            cat = str(Aging_Attribute) + "0 Delinquent account and CURRENT per Infinity"
            Filter_Waterfall = re.sub(" +", " ", cat)
        Filter_CDX_Current = 1  # Delinquent account and CURRENT per Infinity.
        CDX_Current_PER = 1  # % Delinquent account and CURRENT per Infinity.
        # The following are being marked as `TMP`:
        Analysis_A = "---"

    return (Filter_Waterfall, Filter_CDX_Current, CDX_Current_PER, Analysis_A)


schema_Filter_CDX_Current = t.StructType([
    t.StructField("FILTER_WATERFALL", t.StringType(), True),
    t.StructField("FILTER_CDX_CURRENT", t.IntegerType(), True),
    t.StructField("CDX_CURRENT_PER", t.IntegerType(), True),
    t.StructField("ANALYSIS_A", t.StringType(), True)
])

udf_Filter_CDX_Current = f.udf(Filter_CDX_Current, returnType=schema_Filter_CDX_Current)


def Filter_Contract_Abnormal_Date(SEQ, CON_Abnormal_Date, Filter_Waterfall):
    """
    Filter Accounts based on contract churned status.
    """

    # Set default values:
    Filter_Contract_Abnormal_Date = None
    Contract_Abnormal_Date_PER = None

    if CON_Abnormal_Date is not None:
        if Filter_Waterfall == "":
            cat = str(SEQ) + " Subscription has an abnormal start date"
            Filter_Waterfall = re.sub(" +", "", cat)
        Filter_Contract_Abnormal_Date = 1  # Subscription has an abnormal start date.
        Contract_Abnormal_Date_PER = 1  # % subscriptions with abnormal start dates.
    return (Filter_Waterfall, Filter_Contract_Abnormal_Date, Contract_Abnormal_Date_PER)


schema_Filter_Contract_Abnormal_Date = t.StructType([
    t.StructField("FILTER_WATERFALL", t.StringType(), True),
    t.StructField("FILTER_CONTRACT_ABNORMAL_DATE", t.IntegerType(), True),
    t.StructField("CONTRACT_ABNORMAL_DATE_PER", t.IntegerType(), True)
])


def Filter_Contract_Barred(SEQ, CON_Status_Bar_Outgoing, Filter_Waterfall):
    """
    Filter Accounts based on the Contract Churned Status.
    """

    # Set some default values:
    Filter_Contract_Barred = None
    Contract_Barred_PER = None

    if CON_Status_Bar_Outgoing is not None:
        if Filter_Waterfall == "":
            cat = str(SEQ) + " Subscriptions barred from outgoing calls"
            Filter_Waterfall = re.sub(" +", "", cat)
        Filter_Contract_Barred = 1  # Subscriptions barred from outgoing calls
        Contract_Barred_PER = 1  # % subscriptions barred from outgoing calls

    return (Filter_Waterfall, Filter_Contract_Barred, Contract_Barred_PER)


schema_Filter_Contract_Barred = t.StructType([
    t.StructField("FILTER_WATERFALL", t.StringType(), True),
    t.StructField("FILTER_CONTRACT_BARRED", t.IntegerType(), True),
    t.StructField("CONTRACT_BARRED_PER", t.IntegerType(), True)
])

udf_Filter_Contract_Barred = f.udf(Filter_Contract_Barred, returnType=schema_Filter_Contract_Barred)


def Filter_Contract_Churned(SEQ, CON_Status_Churned, Filter_Waterfall):
    """
    Filter Accounts based on Contract Churned Status.
    """

    # Set some default values:
    Filter_Contract_Churned = None
    Contract_Churned_PER = None

    if CON_Status_Churned is not None:
        if Filter_Waterfall == "":
            cat = str(SEQ) + " Subscriptions involuntary or voluntary churned"
            Filter_Waterfall = re.sub(" +", " ", cat)
        Filter_Contract_Churned = 1  # Subscriptions involuntary or voluntary churned
        Contract_Churned_PER = 1  # % subscriptions involuntary or voluntary churned

    return (Filter_Waterfall, Filter_Contract_Churned, Contract_Churned_PER)


schema_Filter_Contract_Churned = t.StructType([
    t.StructField("FILTER_WATERFALL", t.StringType(), True),
    t.StructField("FILTER_CONTRACT_CHURNED", t.IntegerType(), True),
    t.StructField("CONTRACT_CHURNED_PER", t.IntegerType(), True)
])

udf_Filter_Contract_Churned = f.udf(Filter_Contract_Churned, returnType=schema_Filter_Contract_Churned)


def Filter_Debt_Prescription(SEQ, Delinquency_Trigger, Filter_Waterfall):
    """
    Filter Accounts when Debt Prescription is invoked.
    """

    # Set some default values:
    Filter_Debt_Prescription = None
    Debt_Prescription_PER = None

    if Delinquency_Trigger == "F. Debt Prescription":
        if Filter_Waterfall == "":
            cat = str(SEQ) + " Debt prescription invoked"
            Filter_Waterfall = re.sub(" +", " ", cat)
        Filter_Debt_Prescription = 1  # Debt prescription invoked.
        Debt_Prescription_PER = 1  # % number of accounts flagged as under debt prescription.

    return (Filter_Waterfall, Filter_Debt_Prescription, Debt_Prescription_PER)


schema_Filter_Debt_Prescription = t.StructType([
    t.StructField("FILTER_WATERFALL", t.StringType(), True),
    t.StructField("FILTER_DEBT_PRESCRIPTION", t.IntegerType(), True),
    t.StructField("DEBT_PRESCRIPTION_PER", t.IntegerType(), True)
])

udf_Filter_Debt_Prescription = f.udf(Filter_Debt_Prescription, returnType=schema_Filter_Debt_Prescription)


def Filter_Debt_Review(SEQ, Delinquency_Trigger, Filter_Waterfall):
    """
    Filter when the customer is under debt review.
    """

    # Set some default values:
    Filter_Debt_Review = None
    Debt_Review_PER = None
    Debt_Review_DLQ_Trigger = None
    Debt_Review_Credit_Bureau = None

    if Delinquency_Trigger == "G. Debt Review":
        if Filter_Waterfall == "":
            cat = str(SEQ) + " Customer under debt review"
            Filter_Waterfall = re.sub(" +", " ", cat)
        Filter_Debt_Review = 1  # Customer under debt review.
        Debt_Review_PER = 1  # % number of accounts flagged as debt review.
        Debt_Review_DLQ_Trigger = 1  # Delinquency trigger customer under debt review.
        # To be derived from the EXPERIAN Future File.
        Debt_Review_Credit_Bureau = None  # Credit bureau identified customer as under debt review

    return (Filter_Waterfall, Filter_Debt_Review, Debt_Review_PER,
            Debt_Review_DLQ_Trigger, Debt_Review_Credit_Bureau)


schema_Filter_Debt_Review = t.StructType([
    t.StructField("FILTER_WATERFALL", t.StringType(), True),
    t.StructField("FILTER_DEBT_REVIEW", t.IntegerType(), True),
    t.StructField("DEBT_REVIEW_PER", t.IntegerType(), True),
    t.StructField("DEBT_REVIEW_DLQ_TRIGGER", t.IntegerType(), True),
    t.StructField("DEBT_REVIEW_CREDIT_BUREAU", t.IntegerType(), True)
])

udf_Filter_Debt_Review = f.udf(Filter_Bad_Debt, returnType=schema_Filter_Debt_Review)


def Filter_Deceased(SEQ, Delinquency_Trigger, Filter_Waterfall):
    """
    Filter accounts when the customer is deceased.
    """

    if Delinquency_Trigger == "A. Deceased":
        if Filter_Waterfall == "":
            cat = str(SEQ) + " Customer is deceased"
            Filter_Waterfall = re.sub(" +", " ", cat)
        Filter_Deceased = 1  # Customer is deceased
        Deceased_PER = 1  # % number of accounts flagged as deceased
        Deceased_DLQ_Trigger = 1  # Delinquency trigger that customer is deceased
        # Following to be derived from the EXPERIAN Future File:
        Deceased_Credit_Bureau = None  # Credit bureau identified customer as deceased.

        return (Filter_Waterfall, Filter_Deceased, Deceased_PER,
                Deceased_DLQ_Trigger, Deceased_Credit_Bureau)


schema_Filter_Deceased = t.StructType([
    t.StructField("FILTER_WATERFALL", t.StringType(), True),
    t.StructField("FILTER_DECEASED", t.IntegerType(), True),
    t.StructField("DECEASED_PER", t.IntegerType(), True),
    t.StructField("DECEASED_DLQ_TRIGGER", t.IntegerType(), True),
    t.StructField("DECEASED_CREDIT_BUREAU", t.IntegerType(), True)
])

udf_Filter_Deceased = f.udf(Filter_Deceased, returnType=schema_Filter_Deceased)


def Filter_Enterprise(SEQ, Customer_TYP, Filter_Waterfall, Trigger_Analysis_B, Trigger_Account_Type,
                      Trigger_IDValidate):
    """
    Filter Accounts when an enterprise.
    """

    # Set some default values:
    Filter_Enterprise = None
    Enterprise_PER = None
    Enterprise_Analysis_B = None
    Enterprise_Account_Type = None
    Enterprise_IDValidate = None

    if Customer_TYP == "Enterprise":
        if Filter_Waterfall == "":
            cat = str(SEQ) + " Enterprise account"
            Filter_Waterfall = re.sub(" +", " ", cat)
        Filter_Enterprise = 1  # Enterprise account
        Enterprise_PER = 1  # % accounts enterprises
        Enterprise_Analysis_B = Trigger_Analysis_B  # ANALYSIS_B = BUSINESS
        Enterprise_Account_Type = Trigger_Account_Type  # Account_Type = Enterprise
        Enterprise_IDValidate = Trigger_IDValidate  # ID Validation = Enterprise
        # The following is marked as `TMP`:
        if Trigger_Analysis_B == 1:
            Analysis_B = "---"
    # The following is marked as `TMP`:
    Analysis_B = "---"

    return (Filter_Waterfall, Filter_Enterprise, Enterprise_PER, Enterprise_Analysis_B,
            Enterprise_Account_Type, Enterprise_IDValidate, Analysis_B)


schema_Filter_Enterprise = t.StructType([
    t.StructField("FILTER_WATERFALL", t.IntegerType(), True),
    t.StructField("FILTER_ENTERPRISE", t.IntegerType(), True),
    t.StructField("ENTERPRISE_PER", t.IntegerType(), True),
    t.StructField("ENTERPRISE_ANALYSIS_B", t.StringType(), True),
    t.StructField("ENTERPRISE_ACCOUNT_TYPE", t.StringType(), True),
    t.StructField("ENTERPRISE_IDVALIDATE", t.StringType(), True),
    t.StructField("ANALYSIS_B", t.StringType(), True)
])

udf_Filter_Enterprise = f.udf(Filter_Enterprise, returnType=schema_Filter_Enterprise)


def Filter_Fraud(SEQ, Delinquency_Trigger, Filter_Waterfall):
    """
    Filter accounts when identified as fraud.
    """

    # Set some default values:
    Filter_Fraud = None
    Fraud_PER = None

    if Delinquency_Trigger == "B. Fraud":
        if Filter_Waterfall == "":
            cat = str(SEQ) + " Account is fraudulent"
            Filter_Waterfall = re.sub(" +", "", cat)
        Filter_Fraud = 1  # Account is fraudulent.
        Fraud_PER = 1  # % number of accounts flagged as fraudulent.
    return Filter_Waterfall, Filter_Fraud, Fraud_PER


schema_Filter_Fraud = t.StructType([
    t.StructField("FILTER_WATERFALL", t.StringType(), True),
    t.StructField("FILTER_FRAUD", t.IntegerType(), True),
    t.StructField("FRAUD_PER", t.IntegerType(), True)
])

udf_Filter_Fraud = f.udf(Filter_Fraud, returnType=schema_Filter_Fraud)


def Filter_FTTH_Subscriptions(SEQ, PER, PRD_FTTH, Contracts, Filter_Waterfall):
    """
    Filter accounts when the subscription product is FTTH.
    """

    # Set some default values:
    FTTH_Subscriptions = None
    Filter_FTTH = None
    FTTH_PER = None

    if PRD_FTTH >= 1:
        FTTH_Subscriptions = PRD_FTTH
        if PRD_FTTH >= int(PER * Contracts / 100.0):
            if Filter_Waterfall == "":
                cat = str(SEQ) + " " + str(PER) + "% FTTH subscriptions"
                Filter_Waterfall = re.sub(" +", " ", cat)
            Filter_FTTH = 1  # Flagged as a FTTH subscription.
            FTTH_PER = PRD_FTTH  # % FTTH product subscriptions

    return (FTTH_Subscriptions, Filter_Waterfall, Filter_FTTH, FTTH_PER)


schema_Filter_FTTH_Subscriptions = t.StructType([
    t.StructField("FTTH_SUBSCRIPTIONS", t.IntegerType(), True),
    t.StructField("FILTER_WATERFALL", t.StringType(), True),
    t.StructField("FILTER_FTTH", t.IntegerType(), True),
    t.StructField("FTTH_PER", t.IntegerType(), True)
])

udf_Filter_FTTH_Subscriptions = f.udf(Filter_FTTH_Subscriptions, returnType=schema_Filter_FTTH_Subscriptions)


def Filter_High_Balances(SEQ, BAL, Balance_SME, Filter_Waterfall):
    """
    Filter accounts when abnormally high balance.
    """

    # Set some default values:
    Filter_High_Balance = None
    High_Balance_PER = None

    if (Balance_SME <= -BAL) or (Balance_SME >= BAL):
        if Filter_Waterfall == "":
            cat = str(SEQ) + " R" + str(BAL) + "+ balance account"
            Filter_Waterfall = re.sub(" +", " ", cat)
        Filter_High_Balance = 1  # Flagged as an abnormally high-balance account.
        High_Balance_PER = Balance_SME  # % High balance amount (negative or postive).

    return (Filter_Waterfall, Filter_High_Balance, High_Balance_PER)


schema_Filter_High_Balances = t.StructType([
    t.StructField("FILTER_WATERFALL", t.StringType(), True),
    t.StructField("FILTER_HIGH_BALANCE", t.IntegerType(), True),
    t.StructField("HIGH_BALANCE_PER", t.IntegerType(), True)
])

udf_Filter_High_Balances = f.udf(Filter_High_Balances, returnType=schema_Filter_High_Balances)


def Filter_High_Contracts(SEQ, SUB, Contracts, Filter_Waterfall):
    """
    Filter accounts when abnormally high number of contracts (or subscriptions).
    """

    # Define some default values:
    Filter_High_Contracts = None
    High_Contracts_PER = None

    if Contracts >= SUB:
        if Filter_Waterfall == "":
            cat = str(SEQ) + " " + str(SUB) + "+ contracts (subscriptions)"
            Filter_Waterfall = re.sub(" +", " ", cat)
        Filter_High_Contracts = 1  # Flagged as an abnormally high number contracts.
        High_Contracts_PER = Contracts  # % High number of contacts (subscriptions)

    return (Filter_Waterfall, Filter_High_Contracts, High_Contracts_PER)


schema_Filter_High_Contracts = t.StructType([
    t.StructField("FILTER_WATERFALL", t.StringType(), True),
    t.StructField("FILTER_HIGH_CONTRACTS", t.IntegerType(), True),
    t.StructField("HIGH_CONTRACTS_PER", t.IntegerType(), True)
])

udf_Filter_High_Contracts = f.udf(Filter_High_Contracts, returnType=schema_Filter_High_Contracts)


def Filter_Not_Infinity(SEQ, Source, Filter_Waterfall):
    """
    Filter out accounts not processed by Infinity
    """

    # Set some default values:
    Filter_Not_Infinity = None
    Not_Infinity_PER = None

    if Source != "CEC":
        if Filter_Waterfall == "":
            cat = str(SEQ) + " Account not processed by Infinity"
            Filter_Waterfall = re.sub(" +", " ", cat)
        Filter_Not_Infinity = 1  # Not processed by Infinity.
        Not_Infinity_PER = 1  # % not processed by Infinity.

    return (Filter_Waterfall, Filter_Not_Infinity, Not_Infinity_PER)


schema_Filter_Not_Infinity = t.StructType([
    t.StructField("FILTER_WATERFALL", t.StringType(), True),
    t.StructField("FILTER_NOT_INFINITY", t.IntegerType(), True),
    t.StructField("NOT_INFINITY_PER", t.IntegerType(), True)
])

udf_Filter_Not_Infinity = f.udf(Filter_Not_Infinity, returnType=schema_Filter_Not_Infinity)


def Filter_Onseller_Subscriptions(SEQ, PER, CHN_Onseller, Contracts, Filter_Waterfall):
    """
    Filter accounts when any subscriptions from an onseller.
    """

    # Set some default values:
    Onseller_Subscriptions = None
    Filter_Onseller = None
    Onseller_PER = None

    if CHN_Onseller >= 1:
        Onseller_Subscriptions = CHN_Onseller
        if CHN_Onseller >= int(PER * Contracts / 100.0):
            if Filter_Waterfall == "":
                cat = str(SEQ) + " " + str(PER) + "% onseller subscriptions"
                Filter_Waterfall = re.sub(" +", " ", cat)
            Filter_Onseller = 1  # Flagged as an onseller account.
            Onseller_PER = CHN_Onseller  # % onseller originated subscriptions.
    return (Onseller_Subscriptions, Filter_Waterfall, Filter_Onseller, Onseller_PER)


schema_Filter_Onseller_Subscriptions = t.StructType([
    t.StructField("ONSELLER_SUBSCRIPTIONS", t.IntegerType(), True),
    t.StructField("FILTER_WATERFALL", t.StringType(), True),
    t.StructField("FILTER_ONSELLER", t.IntegerType(), True),
    t.StructField("ONSELLER_PER", t.IntegerType(), True)
])

udf_Filter_Onseller_Subscriptions = f.udf(Filter_Onseller_Subscriptions,
                                          returnType=schema_Filter_Onseller_Subscriptions)


def Filter_Written_Off(SEQ, Delinquency_Trigger, Analysis_A, Analysis_B, Filter_Waterfall):
    """
    Filter accounts when the account is written off.
    """

    # Set some default values:
    Filter_Written_Off = None
    Written_Off_DLQ_Trigger = None
    Written_Off_Analysis_A = None
    Written_Off_Analysis_B = None
    Written_Off_Extract_File = None

    ls_triggers = ['I. Debt Sale', 'J. Debt Recovery', 'L. Legal Action']
    if (Delinquency_Trigger in ls_triggers) or (Analysis_A == "WRITEOFF") or (Analysis_B == "LEGAL"):
        if Filter_Waterfall == "":
            cat = str(SEQ) + " Account written-off"
            Filter_Waterfall = re.sub(" +", " ", cat)
        Filter_Written_Off = 1  # Account Written-Off
        if Delinquency_Trigger in ls_triggers:
            Written_Off_DLQ_Trigger = 1  # Delinquency trigger is that account written-off.

        if Analysis_A == "WRITEOFF":
            Written_Off_Analysis_A = 1  # ANALYSIS_A = WRITEOFF.

        if Analysis_B == "LEGAL":
            Written_Off_Analysis_B = 1  # ANALYSIS_B = LEGAL.

        # The following is to be derived from the **FUTURTE WRITE-OFF FILE**.
        Written_Off_Extract_File = None

        # The following fields are marked as TMP:
        if Analysis_A == "WRITEOFF":
            Analysis_A = "---"
        if (Analysis_A == "COLLECTION") and (Analysis_B == "LEGAL"):
            Analysis_A = "---"
        if Analysis_B == "LEGAL":
            Analysis_B = "---"

    return (Filter_Waterfall, Filter_Written_Off, Written_Off_DLQ_Trigger,
            Written_Off_Analysis_A, Written_Off_Analysis_B, Written_Off_Extract_File,
            Analysis_A, Analysis_B)


schema_Filter_Written_Off = t.StructType([
    t.StructField("FILTER_WATERFALL", t.StringType(), True),
    t.StructField("FILTER_WRITTEN_OFF", t.IntegerType(), True),
    t.StructField("WRITTEN_OFF_DLQ_TRIGGER", t.IntegerType(), True),
    t.StructField("WRITTEN_OFF_ANALYSIS_A", t.IntegerType(), True),
    t.StructField("WRITTEN_OFF_ANALYSIS_B", t.IntegerType(), True),
    t.StructField("WRITTEN_OFF_EXTRACT_FILE", t.IntegerType(), True),
    t.StructField("ANALYSIS_A", t.StringType(), True),
    t.StructField("ANALYSIS_B", t.StringType(), True)
])

udf_Filter_Written_Off = f.udf(Filter_Written_Off, returnType=schema_Filter_Written_Off)
