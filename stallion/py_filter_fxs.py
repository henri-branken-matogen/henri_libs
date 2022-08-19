import re
import pyspark.sql.functions as f
import pyspark.sql.types as t


def Filter_Activations_Initialise():
    """
    Create the empty application-activation matches, which are updated each time a new activation gets linked to
    a single application.
    """
    Filter_Declined_No_Activations = None  # Declined application with any subscription activations.
    Filter_Arrears_No_Activations = None  # Arrears account application without any subscription activations.
    Filter_Referred_No_Activations = None  # Referred application without any subscription activations.
    Filter_Approved_No_Activations = None  # Approved application without any subscription activations (NTU).
    Filter_Declined_With_Activations = None  # Declined application with matched subscription activations.
    Filter_Arrears_With_Activations = None  # Arrears account application with matched subscription activations
    Filter_Referred_With_Activations = None  # Referred application with matched subscription activations.
    Filter_Approved_With_Activations = None  # Approved application with mathced subscription activations.
    Filter_Immature_Activations = None  # Immature customer activation without an application.
    Filter_Clear_Activations = None  # UTD Clear Customer Activation without an application.
    Filter_Responsible_Activations = None  # UTD Responsible customer activation without an application.
    Filter_Erratic_Activations = None  # UTD Erratic customer activation without an application.
    Filter_Arrears_Activations = None  # Arrears Customer Activation without an application.
    Filter_Other_Activations = None  # Other customer states activations without an application.

    return (Filter_Declined_No_Activations, Filter_Arrears_No_Activations, Filter_Referred_No_Activations,
            Filter_Approved_No_Activations, Filter_Declined_With_Activations, Filter_Arrears_With_Activations,
            Filter_Referred_With_Activations, Filter_Approved_With_Activations, Filter_Immature_Activations,
            Filter_Clear_Activations, Filter_Responsible_Activations, Filter_Erratic_Activations,
            Filter_Arrears_Activations, Filter_Other_Activations)


schema_Filter_Activations_Initialise = t.StructType([
    t.StructField("FILTER_DECLINED_NO_ACTIVATIONS", t.StringType(), True),
    t.StructField("FILTER_ARREARS_NO_ACTIVATIONS", t.StringType(), True),
    t.StructField("FILTER_REFERRED_NO_ACTIVATIONS", t.StringType(), True),
    t.StructField("FILTER_APPROVED_NO_ACTIVATIONS", t.StringType(), True),
    t.StructField("FILTER_DECLINED_WITH_ACTIVATIONS", t.StringType(), True),
    t.StructField("FILTER_ARREARS_WITH_ACTIVATIONS", t.StringType(), True),
    t.StructField("FILTER_REFERRED_WITH_ACTIVATIONS", t.StringType(), True),
    t.StructField("FILTER_APPROVED_WITH_ACTIVATIONS", t.StringType(), True),
    t.StructField("FILTER_IMMATURE_ACTIVATIONS", t.StringType(), True),
    t.StructField("FILTER_CLEAR_ACTIVATIONS", t.StringType(), True),
    t.StructField("FILTER_RESPONSIBLE_ACTIVATIONS", t.StringType(), True),
    t.StructField("FILTER_ERRATIC_ACTIVATIONS", t.StringType(), True),
    t.StructField("FILTER_ARREARS_ACTIVATIONS", t.StringType(), True),
    t.StructField("FILTER_OTHER_ACTIVATIONS", t.StringType(), True)
])

udf_Filter_Activations_Initialise = f.udf(Filter_Activations_Initialise,
                                          returnType=schema_Filter_Activations_Initialise)


def Filter_Activation_Status(Matched_Distance, Filter_Decision_Outcome_Declined, Filter_Decision_Outcome_Arrears,
                             Filter_Decision_Outcome_Referred, Filter_Decision_Outcome_Approved,
                             Filter_Declined_No_Activations, Filter_Arrears_No_Activations,
                             Filter_Referred_No_Activations, Filter_Approved_No_Activations,
                             Filter_Declined_With_Activations, Filter_Arrears_With_Activations,
                             Filter_Referred_With_Activations, Filter_Approved_With_Activations):

    Filter_Activation_SEQ = None
    if Filter_Declined_No_Activations is None:
        Filter_Declined_No_Activations = 0
    if Filter_Arrears_No_Activations is None:
        Filter_Arrears_No_Activations = 0
    if Filter_Referred_No_Activations is None:
        Filter_Referred_No_Activations = 0
    if Filter_Approved_No_Activations is None:
        Filter_Approved_No_Activations = 0
    if Filter_Declined_With_Activations is None:
        Filter_Declined_With_Activations = 0
    if Filter_Arrears_With_Activations is None:
        Filter_Arrears_With_Activations = 0
    if Filter_Referred_With_Activations is None:
        Filter_Referred_With_Activations = 0
    if Filter_Approved_With_Activations is None:
        Filter_Approved_With_Activations = 0

    if (Matched_Distance is None) and (Filter_Decision_Outcome_Declined is not None):
        Filter_Activation_SEQ = 7
        Filter_Declined_No_Activations += 1
    elif (Matched_Distance is None) and (Filter_Decision_Outcome_Arrears is not None):
        Filter_Activation_SEQ = 6
        Filter_Arrears_No_Activations += 1
    elif (Matched_Distance is None) and (Filter_Decision_Outcome_Referred is not None):
        Filter_Activation_SEQ = 5
        Filter_Referred_No_Activations += 1
    elif (Matched_Distance is None) and (Filter_Decision_Outcome_Approved is not None):
        Filter_Activation_SEQ = 4
        Filter_Approved_No_Activations += 1
    elif (Matched_Distance is not None) and (Filter_Decision_Outcome_Declined is not None):
        Filter_Activation_SEQ = 3
        Filter_Declined_With_Activations += 1
    elif (Matched_Distance is not None) and (Filter_Decision_Outcome_Arrears is not None):
        Filter_Activation_SEQ = 2
        Filter_Arrears_With_Activations += 1
    elif (Matched_Distance is not None) and (Filter_Decision_Outcome_Referred is not None):
        Filter_Activation_SEQ = 1
        Filter_Referred_With_Activations += 1
    elif (Matched_Distance is not None) and (Filter_Decision_Outcome_Approved is not None):
        Filter_Activation_SEQ = 0
        Filter_Approved_With_Activations += 1
    else:
        pass

    if Filter_Declined_No_Activations == 0:
        Filter_Declined_No_Activations = None
    if Filter_Arrears_No_Activations == 0:
        Filter_Arrears_No_Activations = None
    if Filter_Referred_No_Activations == 0:
        Filter_Referred_No_Activations = None
    if Filter_Approved_No_Activations == 0:
        Filter_Approved_No_Activations = None
    if Filter_Declined_With_Activations == 0:
        Filter_Declined_With_Activations = None
    if Filter_Arrears_With_Activations == 0:
        Filter_Arrears_With_Activations = None
    if Filter_Referred_With_Activations == 0:
        Filter_Referred_With_Activations = None
    if Filter_Approved_With_Activations == 0:
        Filter_Approved_With_Activations = None

    return (Filter_Activation_SEQ, Filter_Declined_No_Activations, Filter_Arrears_No_Activations,
            Filter_Referred_No_Activations, Filter_Approved_No_Activations, Filter_Declined_With_Activations,
            Filter_Arrears_With_Activations, Filter_Referred_With_Activations, Filter_Approved_With_Activations)


schema_Filter_Activation_Status = t.StructType([
    t.StructField("FILTER_ACTIVATION_SEQ", t.IntegerType(), True),
    t.StructField("FILTER_DECLINED_NO_ACTIVATIONS", t.IntegerType(), True),
    t.StructField("FILTER_ARREARS_NO_ACTIVATIONS", t.IntegerType(), True),
    t.StructField("FILTER_REFERRED_NO_ACTIVATIONS", t.IntegerType(), True),
    t.StructField("FILTER_APPROVED_NO_ACTIVATIONS", t.IntegerType(), True),
    t.StructField("FILTER_DECLINED_WITH_ACTIVATIONS", t.IntegerType(), True),
    t.StructField("FILTER_ARREARS_WITH_ACTIVATIONS", t.IntegerType(), True),
    t.StructField("FILTER_REFERRED_WITH_ACTIVATIONS", t.IntegerType(), True),
    t.StructField("FILTER_APPROVED_WITH_ACTIVATIONS", t.IntegerType(), True)
])

udf_Filter_Activation_Status = f.udf(Filter_Activation_Status,
                                     returnType=schema_Filter_Activation_Status)


def Filter_Administration(SEQ, Delinquency_Trigger, Filter_Waterfall):
    """
    Whether an account is under administration, insolvent, or in liquidation.  This comes from the CellC/CEC data, and
    provision has been made for when Tshepi brings through the batch Experian data.
    """
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
    Filter accounts when the account is in bad debt.  Whether the account has reached a bad debt churned state (not
    write off).  This will help resolve DOUBTFUL_DEBT mismatch we have as the month post the bad debt the account is
    moved from the ACTIVE population to the CLOSED (churned) population.
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
    Filter accounts when subscriptions from a business.  The % of subscriptions that come from a business channel,
    should we want to exclude them from reporting.  I have found that not all enterprises are flagged correctly, and
    the opportunity is for CEC to investigate these and flag them correctly, i.e. that they are enterprises.
    """

    if CHN_Business is None:
        CHN_Business = 0
    if Contracts is None:
        Contracts = 0

    # Make some default assignments:
    Business_Subscriptions = None
    Filter_Business = None
    Business_PER = None

    if CHN_Business >= 1:
        Business_Subscriptions = CHN_Business  # Number business subscriptions
        if CHN_Business >= int(PER * Contracts / 100):
            cat = str(SEQ) + " " + str(PER) + "% business subscriptions"
            Filter_Waterfall = re.sub(" +", " ", cat)
            Filter_Business = 1  # Flagged as a business account
            Business_PER = CHN_Business  # % business originated subscriptions
    return Business_Subscriptions, Filter_Waterfall, Filter_Business, Business_PER


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
    Filter UTD accounts when Infinity identifies them as in Collections.  This is part of the reconciliation Paul
    requires between what we see in collections (from the AR files) and what infinity says they are working.  This
    identifies where we see the account as being UTD, and Infinity say that they form part of their collections KPIs.
    This will help close the gap between the two versions.  Bear in mind that the AR and Infinity snapshots are on the
    same day!
    """

    if CD0_NBR is None:
        CD0_NBR = 0

    # Set some default values:
    Filter_CD0_Collection = None
    CD0_Collection_PER = None

    if (CD0_NBR == 1) and (Analysis_A == "COLLECTION"):
        if Filter_Waterfall == "":
            Filter_Waterfall = "0X UTD account flagged as being in COLLECTION by Infinity"
        Filter_CD0_Collection = 1  # UTD account flagged as being in COLLECTION by Infinity
        CD0_Collection_PER = 1  # % UTD account flagged as being in COLLECTION by Infinity

        # The following is marked as /* TMP */.
        Analysis_A = "---"
    return Filter_Waterfall, Filter_CD0_Collection, CD0_Collection_PER, Analysis_A


schema_Filter_CD0_Collection = t.StructType([
    t.StructField("FILTER_WATERFALL", t.StringType(), True),
    t.StructField("FILTER_CD0_COLLECTION", t.IntegerType(), True),
    t.StructField("CD0_Collection_PER", t.IntegerType(), True),
    t.StructField("ANALYSIS_A", t.StringType(), True)
])

udf_Filter_CD0_Collection = f.udf(Filter_CD0_Collection, returnType=schema_Filter_CD0_Collection)


def Filter_CD0_Current(CD0_NBR, Analysis_A, Filter_Waterfall):
    """
    Filter UTD accounts and not in Infinity collections.  This is where the AR files aggre with Infinity that the
    account is UTD, and what we expect each month.
    """

    if CD0_NBR is None:
        CD0_NBR = 0

    # Set some default values:
    Filter_CD0_Current = None
    CD0_Current_PER = None

    if (CD0_NBR == 1) and (Analysis_A == "CURRENT"):
        if Filter_Waterfall == "":
            Filter_Waterfall = "00 UTD account and CURRENT per Infinity"
        Filter_CD0_Current = 1  # UTD account and CURRENT per Infinity.
        CD0_Current_PER = 1  # % UTD account and CURRENT per Infinity.

        # The following is marked as /* TMP */:
        Analysis_A = "---"

    return Filter_Waterfall, Filter_CD0_Current, CD0_Current_PER, Analysis_A


schema_Filter_CD0_Current = t.StructType([
    t.StructField("FILTER_WATERFALL", t.StringType(), True),
    t.StructField("FILTER_CD0_CURRENT", t.IntegerType(), True),
    t.StructField("CD0_CURRENT_PER", t.IntegerType(), True),
    t.StructField("ANALYSIS_A", t.StringType(), True)
])

udf_Filter_CD0_Current = f.udf(Filter_CD0_Current, returnType=schema_Filter_CD0_Current)


def Filter_CDX_Collection(CD1_NBR, CD2_NBR, CD3_NBR, CD4_NBR, Analysis_A, Filter_Waterfall, Aging_Attribute):
    """
    Filter delinquent accounts when Infinity also identifies them as being in collections.  This is where the AR files
    agree with Infinity that the account is in arrears, and being collected on, and what we expect to see each month.
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

        # The following is being marked as /* TMP */:
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
    Filter accounts that are in arrears when Infinity identifies them as being CURRENT.  This is where the AR file says
    that the account is in arrears, but Infinity say that they are not being collected on.
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

        # The following are being marked as /* TMP */:
        Analysis_A = "---"

    return Filter_Waterfall, Filter_CDX_Current, CDX_Current_PER, Analysis_A


schema_Filter_CDX_Current = t.StructType([
    t.StructField("FILTER_WATERFALL", t.StringType(), True),
    t.StructField("FILTER_CDX_CURRENT", t.IntegerType(), True),
    t.StructField("CDX_CURRENT_PER", t.IntegerType(), True),
    t.StructField("ANALYSIS_A", t.StringType(), True)
])

udf_Filter_CDX_Current = f.udf(Filter_CDX_Current, returnType=schema_Filter_CDX_Current)


def Filter_Channel_CEC(APP_Channel_CEC):
    """
    Filter Accounts based on the CEC-defined channel.  This is the CEC defined application channel.  The importance of
    this is that we bind applications to activations (subscriptions) to accounts, so when we view accounts bad rate,
    etc, we can correctly identify channel.  For instance, a growing channel is ONLINE, for which we currently don't
    have any DMP decision service.  There are currently just under 30k online applications so this decision service will
    now need to be provided for.  Using this filter approach, it will be easy for Tshepi to monitor how the channel is
    growing and the resultant take-ups (activations), accounts generated and more importantly bad rates, bad debt.
    He will not need you or me to extract this for him.
    """
    if (APP_Channel_CEC is None): APP_Channel_CEC = ""

    # Set some default values:
    Filter_Channel_Store = None
    Filter_Channel_Franchise = None
    Filter_Channel_Outbound = None
    Filter_Channel_Inbound = None
    Filter_Channel_Dealer = None
    Filter_Channel_Online = None
    Filter_Channel_Other = None

    if "STORE" in APP_Channel_CEC.upper():
        Filter_Channel_Store = 1  # Channel = Own Store.
    elif "FRANCHISE" in APP_Channel_CEC.upper():
        Filter_Channel_Franchise = 1  # Channel = Franchise Store.
    elif "OUTBOUND" in APP_Channel_CEC.upper():
        Filter_Channel_Outbound = 1  # Channel = Telesales Outbound.
    elif "INBOUND" in APP_Channel_CEC.upper():
        Filter_Channel_Inbound = 1  # Channel = Telesales Inbound.
    elif "DEALER" in APP_Channel_CEC.upper():
        Filter_Channel_Dealer = 1  # Channel = Dealer.
    elif "ONLINE" in APP_Channel_CEC.upper():
        Filter_Channel_Online = 1  # Channel = Online.
    else:
        Filter_Channel_Other = 1  # Channel = Other.

    return (Filter_Channel_Store, Filter_Channel_Franchise, Filter_Channel_Outbound, Filter_Channel_Inbound,
            Filter_Channel_Dealer, Filter_Channel_Online, Filter_Channel_Other)


schema_Filter_Channel_CEC = t.StructType([
    t.StructField("FILTER_CHANNEL_STORE", t.IntegerType(), True),
    t.StructField("FILTER_CHANNEL_FRANCHISE", t.IntegerType(), True),
    t.StructField("FILTER_CHANNEL_OUTBOUND", t.IntegerType(), True),
    t.StructField("FILTER_CHANNEL_INBOUND", t.IntegerType(), True),
    t.StructField("FILTER_CHANNEL_DEALER", t.IntegerType(), True),
    t.StructField("FILTER_CHANNEL_ONLINE", t.IntegerType(), True),
    t.StructField("FILTER_CHANNEL_OTHER", t.IntegerType(), True)
])

udf_Filter_Channel_CEC = f.udf(Filter_Channel_CEC,
                               returnType=schema_Filter_Channel_CEC)


def Filter_Contract_Abnormal_Date(SEQ, CON_Abnormal_Date, Filter_Waterfall):
    """
    Filter Accounts based on contract churned status.  Identifies subscritpion abnormal dates are are excluded during
    application to activation matching routine.  These should not be CEC activations, and should be excluded from
    subscriptions analysis.
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
    return Filter_Waterfall, Filter_Contract_Abnormal_Date, Contract_Abnormal_Date_PER


schema_Filter_Contract_Abnormal_Date = t.StructType([
    t.StructField("FILTER_WATERFALL", t.StringType(), True),
    t.StructField("FILTER_CONTRACT_ABNORMAL_DATE", t.IntegerType(), True),
    t.StructField("CONTRACT_ABNORMAL_DATE_PER", t.IntegerType(), True)
])

udf_Filter_Contract_Abnormal_Date = f.udf(Filter_Contract_Abnormal_Date,
                                          returnType=schema_Filter_Contract_Abnormal_Date)


def Filter_Contract_Barred(SEQ, CON_Status_Bar_Outgoing, Filter_Waterfall):
    """
    Filter Accounts based on the Contract Churned Status.  Identifies contracts that are barred.  Just a reminder how
    this works is that you can now view barred contracts via the accounts insights file using this filter approach.
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

    return Filter_Waterfall, Filter_Contract_Barred, Contract_Barred_PER


schema_Filter_Contract_Barred = t.StructType([
    t.StructField("FILTER_WATERFALL", t.StringType(), True),
    t.StructField("FILTER_CONTRACT_BARRED", t.IntegerType(), True),
    t.StructField("CONTRACT_BARRED_PER", t.IntegerType(), True)
])

udf_Filter_Contract_Barred = f.udf(Filter_Contract_Barred, returnType=schema_Filter_Contract_Barred)


def Filter_Contract_Churned(SEQ, CON_Status_Churned, Filter_Waterfall):
    """
    Filter Accounts based on Contract Churned Status.  Identifies churned contracts, i.e., you can then filter the
    account insights for voluntary and involuntary churned accounts.
    """

    # Set some default values:
    Filter_Contract_Churned = None
    Contract_Churned_PER = None

    if CON_Status_Churned is not None:
        if Filter_Waterfall == "":
            cat = str(SEQ) + " Subscriptions involuntary or voluntary churned"
            Filter_Waterfall = re.sub(" +", " ", cat)
        Filter_Contract_Churned = 1  # Subscriptions involuntary or voluntary churned.
        Contract_Churned_PER = 1  # % subscriptions involuntary or voluntary churned.

    return Filter_Waterfall, Filter_Contract_Churned, Contract_Churned_PER


schema_Filter_Contract_Churned = t.StructType([
    t.StructField("FILTER_WATERFALL", t.StringType(), True),
    t.StructField("FILTER_CONTRACT_CHURNED", t.IntegerType(), True),
    t.StructField("CONTRACT_CHURNED_PER", t.IntegerType(), True)
])

udf_Filter_Contract_Churned = f.udf(Filter_Contract_Churned, returnType=schema_Filter_Contract_Churned)


def Filter_Debt_Prescription(SEQ, Delinquency_Trigger, Filter_Waterfall):
    """
    Filter Accounts when Debt Prescription is invoked.  Identifies account that have prescribed debt.  This is a
    historic regulation issue and good to know whether the debt presribed i.e. no longer legally can be collected on.
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

    return Filter_Waterfall, Filter_Debt_Prescription, Debt_Prescription_PER


schema_Filter_Debt_Prescription = t.StructType([
    t.StructField("FILTER_WATERFALL", t.StringType(), True),
    t.StructField("FILTER_DEBT_PRESCRIPTION", t.IntegerType(), True),
    t.StructField("DEBT_PRESCRIPTION_PER", t.IntegerType(), True)
])

udf_Filter_Debt_Prescription = f.udf(Filter_Debt_Prescription, returnType=schema_Filter_Debt_Prescription)


def Filter_Debt_Review(SEQ, Delinquency_Trigger, Filter_Waterfall):
    """
    Filter when the customer is under debt review.  This is where Cell C / CEC identified the account as being under
    debt review.  I have also provided for when Tshepi implements this Experian batch process to update this filter
    automatically.
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

udf_Filter_Debt_Review = f.udf(Filter_Debt_Review, returnType=schema_Filter_Debt_Review)


def Filter_Deceased(SEQ, Delinquency_Trigger, Filter_Waterfall):
    """
    Filter accounts when the customer is deceased.  Same principle but just where the account is deceased.
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


def Filter_Decision_Outcome(APP_Decision_Outcome):
    """
    Filter Accounts based on the Decision Outcome.  Where the application matched to an account (via the contract
    activation) was initially approved, referred, or declined.  will help to identify where referrals result in
    accounts/contracts, or a decline outcome was overridden by the Cell C operational team.
    """
    if APP_Decision_Outcome is None:
        APP_Decision_Outcome = ""
    else:
        APP_Decision_Outcome = APP_Decision_Outcome.upper()
    Filter_Decision_Outcome_Approved = None
    Filter_Decision_Outcome_Referred = None
    Filter_Decision_Outcome_Arrears = None
    Filter_Decision_Outcome_Declined = None
    Filter_Decision_Outcome_Unknown = None

    if APP_Decision_Outcome.upper() == "APPROVE":
        Filter_Decision_Outcome_SEQ = 1
        Filter_Decision_Outcome_Approved = 1  # Approved
    elif APP_Decision_Outcome.upper() == "REFER":
        Filter_Decision_Outcome_SEQ = 2
        Filter_Decision_Outcome_Referred = 1  # Referred
    elif APP_Decision_Outcome.upper() == "ARREARS":
        Filter_Decision_Outcome_SEQ = 3
        Filter_Decision_Outcome_Arrears = 1  # Arrears
    elif APP_Decision_Outcome.upper() == "DECLINE":
        Filter_Decision_Outcome_SEQ = 4
        Filter_Decision_Outcome_Declined = 1  # Declined
    else:
        Filter_Decision_Outcome_SEQ = 9
        Filter_Decision_Outcome_Unknown = 1  # Unknown Decision Outcome.

    return (Filter_Decision_Outcome_SEQ, Filter_Decision_Outcome_Approved, Filter_Decision_Outcome_Referred,
            Filter_Decision_Outcome_Arrears, Filter_Decision_Outcome_Declined, Filter_Decision_Outcome_Unknown)


schema_Filter_Decision_Outcome = t.StructType([
    t.StructField("FILTER_DECISION_OUTCOME_SEQ", t.IntegerType(), True),
    t.StructField("FILTER_DECISION_OUTCOME_APPROVED", t.IntegerType(), True),
    t.StructField("FILTER_DECISION_OUTCOME_REFERRED", t.IntegerType(), True),
    t.StructField("FILTER_DECISION_OUTCOME_ARREARS", t.IntegerType(), True),
    t.StructField("FILTER_DECISION_OUTCOME_DECLINED", t.IntegerType(), True),
    t.StructField("FILTER_DECISION_OUTCOME_UNKNOWN", t.IntegerType(), True)
])

udf_Filter_Decision_Outcome = f.udf(Filter_Decision_Outcome,
                                    returnType=schema_Filter_Decision_Outcome)


def Filter_Decision_Service(APP_DecisionService):
    """
    Filter accounts based on the decision services deployed.  The decision service applied to the application, which
    resulted in an activation/account, so that Tshepi can monitor bad rates each month without our assistance.
    I have future proffed for future decision services such as NTC (New To Credit, i.e. Thin File), FTA (First Time
    Account, i.e. MOB=0), IMM (Immature accounts, i.e. MOB=1-5), CLR (Clear Account State), etc.
    """
    if APP_DecisionService is None:
        APP_DecisionService = ""
    else:
        APP_DecisionService = APP_DecisionService.upper()
    Filter_New_To_Credit = None
    Filter_First_Account_Applicant = None
    Filter_Web_Service = None
    Filter_Telesales_Inbound = None
    Filter_Telesales_Outbound = None
    Filter_Immature_State = None
    Filter_Clear_State = None
    Filter_Responsible_State = None
    Filter_Erratic_State = None
    Filter_Arrears_State = None
    Filter_XXXXXX_State = None
    Filter_DMP_Campaign = None
    Filter_DMP_New = None
    Filter_DMP_Established = None
    Filter_DMP_Unknown = None

    # No Credit Profile
    if "NEW TO CREDIT" in APP_DecisionService:
        Filter_New_To_Credit = 1
    elif "FIRST ACCOUNT APPLICANT" in APP_DecisionService:
        Filter_First_Account_Applicant = 1
    elif "WEB SERVICE" in APP_DecisionService:
        Filter_Web_Service = 1
    elif "TELESALES INBOUND" in APP_DecisionService:
        Filter_Telesales_Inbound = 1
    elif "TELESALES OUTBOUND" in APP_DecisionService:
        Filter_Telesales_Outbound = 1
    elif "IMMATURE CUSTOMER STATE" in APP_DecisionService:
        Filter_Immature_State = 1
    elif "CLEAR CUSTOMER STATE" in APP_DecisionService:
        Filter_Clear_State = 1
    elif "RESPONSIBLE CUSTOMER STATE" in APP_DecisionService:
        Filter_Responsible_State = 1
    elif "ERRATIC CUSTOMER STATE" in APP_DecisionService:
        Filter_Erratic_State = 1
    elif "ARREARS CUSTOMER STATE" in APP_DecisionService:
        Filter_Arrears_State = 1
    elif "XXXXXXX CUSTOMER STATE" in APP_DecisionService:
        Filter_XXXXXX_State = 1
    elif "CAMPAIGN" in APP_DecisionService:
        Filter_DMP_Campaign = 1
    elif "NEW" in APP_DecisionService:
        Filter_DMP_New = 1
    elif "ESTABLISHED" in APP_DecisionService:
        Filter_DMP_Established = 1
    else:
        Filter_DMP_Unknown = 1

    return (Filter_New_To_Credit, Filter_First_Account_Applicant, Filter_Web_Service, Filter_Telesales_Inbound,
            Filter_Telesales_Outbound, Filter_Immature_State, Filter_Clear_State, Filter_Responsible_State,
            Filter_Erratic_State, Filter_Arrears_State, Filter_XXXXXX_State, Filter_DMP_Campaign,
            Filter_DMP_New, Filter_DMP_Established, Filter_DMP_Unknown)


schema_Filter_Decision_Service = t.StructType([
    t.StructField("FILTER_NEW_TO_CREDIT", t.IntegerType(), True),
    t.StructField("FILTER_FIRST_ACCOUNT_APPLICANT", t.IntegerType(), True),
    t.StructField("FILTER_WEB_SERVICE", t.IntegerType(), True),
    t.StructField("FILTER_TELESALES_INBOUND", t.IntegerType(), True),
    t.StructField("FILTER_TELESALES_OUTBOUND", t.IntegerType(), True),
    t.StructField("FILTER_IMMATURE_STATE", t.IntegerType(), True),
    t.StructField("FILTER_CLEAR_STATE", t.IntegerType(), True),
    t.StructField("FILTER_RESPONSIBLE_STATE", t.IntegerType(), True),
    t.StructField("FILTER_ERRATIC_STATE", t.IntegerType(), True),
    t.StructField("FILTER_ARREARS_STATE", t.IntegerType(), True),
    t.StructField("FILTER_XXXXXX_STATE", t.IntegerType(), True),
    t.StructField("FILTER_DMP_CAMPAIGN", t.IntegerType(), True),
    t.StructField("FILTER_DMP_NEW", t.IntegerType(), True),
    t.StructField("FILTER_DMP_ESTABLISHED", t.IntegerType(), True),
    t.StructField("FILTER_DMP_UNKNOWN", t.IntegerType(), True)
])

udf_Filter_Decision_Service = f.udf(Filter_Decision_Service, returnType=schema_Filter_Decision_Service)


def Filter_Enterprise(SEQ, Customer_TYP, Filter_Waterfall, Trigger_Analysis_B, Trigger_Account_Type,
                      Trigger_IDValidate, Analysis_B):
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
    if Analysis_B == "INDIVIDUAL":
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
    Filter accounts when identified as fraud.  Where Cell C / CEC flag the account as being fraudulent. This flag
    depends on the team continuing to identify and investigate true fraud.
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
    Filter accounts when the subscription product is FTTH.  Fibre subscriptions so that we can filter out accounts that
    have fibre-only subscriptions (as not a CEC account if I understand correctly - needs to be verified with them).
    """

    # Set some default values:
    FTTH_Subscriptions = None
    Filter_FTTH = None
    FTTH_PER = None

    if PRD_FTTH is None:
        PRD_FTTH = 0
    if Contracts is None:
        Contracts = 0

    if PRD_FTTH >= 1:
        FTTH_Subscriptions = PRD_FTTH
        if PRD_FTTH >= int(PER * Contracts / 100):
            if Filter_Waterfall == "":
                cat = str(SEQ) + " " + str(PER) + "% FTTH subscriptions"
                Filter_Waterfall = re.sub(" +", " ", cat)
            Filter_FTTH = 1  # Flagged as a FTTH subscription.
            FTTH_PER = PRD_FTTH  # % FTTH product subscriptions

    return FTTH_Subscriptions, Filter_Waterfall, Filter_FTTH, FTTH_PER


schema_Filter_FTTH_Subscriptions = t.StructType([
    t.StructField("FTTH_SUBSCRIPTIONS", t.IntegerType(), True),
    t.StructField("FILTER_WATERFALL", t.StringType(), True),
    t.StructField("FILTER_FTTH", t.IntegerType(), True),
    t.StructField("FTTH_PER", t.IntegerType(), True)
])

udf_Filter_FTTH_Subscriptions = f.udf(Filter_FTTH_Subscriptions, returnType=schema_Filter_FTTH_Subscriptions)


def Filter_High_Balances(SEQ, BAL, Balance_SME, Filter_Waterfall):
    """
    Filter accounts when abnormally high balance.  Based on the balance RAND value, this will allow us to trap unusual
    accounts to be excluded, e.g. reconciliation accounts.  Optional whether to use this or not.
    """

    if (Balance_SME is None): Balance_SME = 0

    # Set some default values:
    Filter_High_Balance = None
    High_Balance_PER = None

    if (Balance_SME <= -BAL) or (Balance_SME >= BAL):
        if Filter_Waterfall == "":
            cat = str(SEQ) + " R" + str(BAL) + "+ balance account"
            Filter_Waterfall = re.sub(" +", " ", cat)
        Filter_High_Balance = 1  # Flagged as an abnormally high-balance account.
        High_Balance_PER = Balance_SME  # % High balance amount (negative or postive).

    return Filter_Waterfall, Filter_High_Balance, High_Balance_PER


schema_Filter_High_Balances = t.StructType([
    t.StructField("FILTER_WATERFALL", t.StringType(), True),
    t.StructField("FILTER_HIGH_BALANCE", t.IntegerType(), True),
    t.StructField("HIGH_BALANCE_PER", t.IntegerType(), True)
])

udf_Filter_High_Balances = f.udf(Filter_High_Balances, returnType=schema_Filter_High_Balances)


def Filter_High_Contracts(SEQ, SUB, Contracts, Filter_Waterfall):
    """
    Filter accounts when abnormally high number of contracts (or subscriptions).  Similar but based on an unusually high
    number of subscriptions which tend to be businesses.
    """

    if (Contracts is None): Contracts = 0

    # Define some default values:
    Filter_High_Contracts = None
    High_Contracts_PER = None

    if Contracts >= SUB:
        if Filter_Waterfall == "":
            cat = str(SEQ) + " " + str(SUB) + "+ contracts (subscriptions)"
            Filter_Waterfall = re.sub(" +", " ", cat)
        Filter_High_Contracts = 1  # Flagged as an abnormally high number contracts.
        High_Contracts_PER = Contracts  # % High number of contacts (subscriptions)

    return Filter_Waterfall, Filter_High_Contracts, High_Contracts_PER


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

    return Filter_Waterfall, Filter_Not_Infinity, Not_Infinity_PER


schema_Filter_Not_Infinity = t.StructType([
    t.StructField("FILTER_WATERFALL", t.StringType(), True),
    t.StructField("FILTER_NOT_INFINITY", t.IntegerType(), True),
    t.StructField("NOT_INFINITY_PER", t.IntegerType(), True)
])

udf_Filter_Not_Infinity = f.udf(Filter_Not_Infinity, returnType=schema_Filter_Not_Infinity)


def Filter_Onseller_Subscriptions(SEQ, PER, CHN_Onseller, Contracts, Filter_Waterfall):
    """
    Filter accounts when any subscriptions from an onseller.  Onseller subscriptions which are not meant to be CEC
    managed accounts, and they need to be excluded.
    """

    if CHN_Onseller is None:
        CHN_Onseller = 0
    if Contracts is None:
        Contracts = 0

    # Set some default values:
    Onseller_Subscriptions = None
    Filter_Onseller = None
    Onseller_PER = None

    if CHN_Onseller >= 1:
        Onseller_Subscriptions = CHN_Onseller
        if CHN_Onseller >= int(PER * int(Contracts) / 100.0):
            if Filter_Waterfall == "":
                cat = str(SEQ) + " " + str(PER) + "% onseller subscriptions"
                Filter_Waterfall = re.sub(" +", " ", cat)
            Filter_Onseller = 1  # Flagged as an onseller account.
            Onseller_PER = CHN_Onseller  # % onseller originated subscriptions.
    return Onseller_Subscriptions, Filter_Waterfall, Filter_Onseller, Onseller_PER


schema_Filter_Onseller_Subscriptions = t.StructType([
    t.StructField("ONSELLER_SUBSCRIPTIONS", t.IntegerType(), True),
    t.StructField("FILTER_WATERFALL", t.StringType(), True),
    t.StructField("FILTER_ONSELLER", t.IntegerType(), True),
    t.StructField("ONSELLER_PER", t.IntegerType(), True)
])

udf_Filter_Onseller_Subscriptions = f.udf(Filter_Onseller_Subscriptions,
                                          returnType=schema_Filter_Onseller_Subscriptions)


def Filter_Risk_Grade(APP_RiskGrade):
    """
    Filter Accounts based on the decision outcome.  Identifes the risk grade triggered at application time that
    resulted in an account/activation and can be used to measure subsequent bad rates, etc.
    """
    Filter_Risk_Grade_1 = None
    Filter_Risk_Grade_2 = None
    Filter_Risk_Grade_3 = None
    Filter_Risk_Grade_4 = None
    Filter_Risk_Grade_5 = None
    Filter_Risk_Grade_6 = None
    Filter_Risk_Grade_7 = None
    Filter_Risk_Grade_8 = None
    Filter_Risk_Grade_9 = None
    Filter_Risk_Grade_X = None

    if str(APP_RiskGrade).upper() == "1":
        Filter_Risk_Grade_1 = 1  # Risk Grade 1
    elif str(APP_RiskGrade).upper() == "2":
        Filter_Risk_Grade_2 = 1  # Risk Grade 2
    elif str(APP_RiskGrade).upper() == "3":
        Filter_Risk_Grade_3 = 1  # Risk Grade 3
    elif str(APP_RiskGrade).upper() == "4":
        Filter_Risk_Grade_4 = 1  # Risk Grade 4
    elif str(APP_RiskGrade).upper() == "5":
        Filter_Risk_Grade_5 = 1  # Risk Grade 5
    elif str(APP_RiskGrade).upper() == "6":
        Filter_Risk_Grade_6 = 1  # Risk Grade 6
    elif str(APP_RiskGrade).upper() == "7":
        Filter_Risk_Grade_7 = 1  # Risk Grade 7
    elif str(APP_RiskGrade).upper() == "8":
        Filter_Risk_Grade_8 = 1  # Risk Grade 8
    elif str(APP_RiskGrade).upper() == "9":
        Filter_Risk_Grade_9 = 1  # Risk Grade 9
    else:
        Filter_Risk_Grade_X = 1  # Unknown Risk Grade.

    return (Filter_Risk_Grade_1, Filter_Risk_Grade_2, Filter_Risk_Grade_3, Filter_Risk_Grade_4, Filter_Risk_Grade_5,
            Filter_Risk_Grade_6, Filter_Risk_Grade_7, Filter_Risk_Grade_8, Filter_Risk_Grade_9, Filter_Risk_Grade_X)


schema_Filter_Risk_Grade = t.StructType([
    t.StructField("FILTER_RISK_GRADE_1", t.IntegerType(), True),
    t.StructField("FILTER_RISK_GRADE_2", t.IntegerType(), True),
    t.StructField("FILTER_RISK_GRADE_3", t.IntegerType(), True),
    t.StructField("FILTER_RISK_GRADE_4", t.IntegerType(), True),
    t.StructField("FILTER_RISK_GRADE_5", t.IntegerType(), True),
    t.StructField("FILTER_RISK_GRADE_6", t.IntegerType(), True),
    t.StructField("FILTER_RISK_GRADE_7", t.IntegerType(), True),
    t.StructField("FILTER_RISK_GRADE_8", t.IntegerType(), True),
    t.StructField("FILTER_RISK_GRADE_9", t.IntegerType(), True),
    t.StructField("FILTER_RISK_GRADE_X", t.IntegerType(), True)
])

udf_Filter_Risk_Grade = f.udf(Filter_Risk_Grade, returnType=schema_Filter_Risk_Grade)


def Filter_Written_Off(SEQ, Delinquency_Trigger, Analysis_A, Analysis_B, Filter_Waterfall):
    """
    Filter accounts when the account is written off.  Based on what Cell C and CEC and Infinity tell us are written off
    accounts.  We are still missing the true write-off file here which is going to be problematic to verify true
    write-offs (not bad debt!).
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

        # The following fields are marked as /* TMP */:
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
