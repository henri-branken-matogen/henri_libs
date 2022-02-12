import pyspark.sql.functions as f
import pyspark.sql.types as t
import copy


def keep_digits(x):
    str_only_digits = int("".join([e for e in x if e.isdigit()]))
    return str_only_digits


udf_keep_digits = f.udf(keep_digits, returnType=t.StringType())


def Transaction_Debit_Credit(SRC, TDC, AMT):

    # Reverse all the Cell C Transactions in order to align with the Thanos signs.
    if SRC.upper().replace(" ", "") == "CELLC":
        AMT = -1 * AMT

    TXN_Debit, TXN_Credit = 0, 0
    AMT_Pos, AMT_Neg = 0, 0

    # Flag whether a Debit or a Credit:
    if TDC is not None:
        if TDC.upper().replace(" ", "") == "DR":
            TXN_Debit = 1  # Debit Transaction
        elif TDC.upper().replace(" ", "") == "CR":
            TXN_Credit = 1  # Credit Transaction
    elif AMT > 0:
        TXN_Debit = 1  # Debit Transaction
    elif AMT < 0:
        TXN_Credit = 1  # Credit Transaction
    else:
        TXN_Credit = 0
        TXN_Debit = 0

    # Flag whether the amounts are positive or negative so that one can double check the debit/credit flag.
    if AMT > 0 and TXN_Credit == 1:
        AMT_Pos = 1
    if AMT < 0 and TXN_Debit == 1:
        AMT_Neg = 1

    return [TXN_Credit, TXN_Debit, AMT_Pos, AMT_Neg]


schema_Transaction_Debit_Credit = t.StructType([
    t.StructField("txn_db_cr", t.ArrayType(t.IntegerType()), True)
])

udf_Transaction_Debit_Credit = f.udf(Transaction_Debit_Credit,
                                     returnType=schema_Transaction_Debit_Credit)


def Transaction_ID(TXN):
    TXN_RC = str("".join(e.upper() for e in TXN if e.isalpha()))[0: 3]

    if TXN_RC is not None:
        if TXN_RC == "SCZ":
            TXN_RC_Label = "Payment (Debit Order)"  # Infinity Debit Orders
        elif TXN_RC == "SCF":
            TXN_RC_Label = "Payment (DCA)"  # DCA Payments
        elif TXN_RC == "SCD":
            TXN_RC_Label = "Payment (DCA)"  # Infinity Debt Collection Agency
        elif TXN_RC == "SCA":
            TXN_RC_Label = "Payment (Cash)"  # Cell C Cash Payments
        elif TXN_RC == "SCR":
            TXN_RC_Label = "Payment (EFT)"  # Infinity Nedbank EFT
        elif TXN_RC == "SCM":
            TXN_RC_Label = "Payment (Credit Card)"  # Credit Card
        elif TXN_RC == "SCY":
            TXN_RC_Label = "Payment (EasyPay)"  # Cell C EasyPay
        elif TXN_RC == "SCJ":
            TXN_RC_Label = "Payment (EasyPay)"  # Infinity EasyPay
        elif TXN_RC == "SCN":
            TXN_RC_Label = "Payment (PBX)"  # Cell C PBX Payments (PayGate?)
        elif TXN_RC == "SCQ":
            TXN_RC_Label = "Payment (Debit Order Void)"  # Cell C NAEDO Voids
        elif TXN_RC == "SCU":
            TXN_RC_Label = "Payment (Debit Order Voic)"  # Infinity Voids
        elif TXN_RC == "CNJ":
            TXN_RC_Label = "Credit Note"  # Credit Note
        elif TXN_RC == "DNJ":
            TXN_RC_Label = "Debit Note"  # Debit Note
        else:
            TXN_RC_Label = None
    else:
        TXN_RC_Label = None

    return TXN_RC, TXN_RC_Label


schema_Transaction_ID = t.StructType([
    t.StructField("TXN_RC", t.StringType(), True),
    t.StructField("TXN_RC_Label", t.StringType(), True)
])

udf_Transaction_ID = f.udf(Transaction_ID,
                           returnType=schema_Transaction_ID)


def Transaction_Type(TXN, TXN_Credit, TXN_Debit, TXN_Receipt_Note, TXN_RC, TXN_RC_Label):

    if TXN_Credit is None:
        TXN_Credit = 0
    if TXN_Debit is None:
        TXN_Debit = 0
    if (TXN is not None) and (TXN != ""):
        TXN_Type = str("".join(e.upper() for e in TXN if e.isalpha()))[0: 3]  # The Transaction Type
    else:
        TXN_Type = ""

    # Infinity Transaction Codes clarified using the Receipt Code and whether a debit or credit transaction.

    if TXN_Type == "BLL":
        TXN_Type_Label = "Invoice (Subscription)"  # Bill Run.
    elif TXN_Type == "PAU" and TXN_Credit == 1:
        TXN_Type_Label = "Payment (Debit Order)"  # Payment Authorisation.  Debit Order Payment.
    elif TXN_Type == "PAU" and TXN_Debit == 1:
        TXN_Type_Label = "Payment (Debit Order Void)"  # Made void as a debit transaction.
    elif TXN_Type == "VOI":
        TXN_Type_Label = "Payment (Debit Order Void)"  # Void - Debit Order Returned.
    elif (TXN_Type == "POB") and (TXN_Receipt_Note == "Cash"):
        TXN_Type_Label = "Payment (Cash)"  # Payment of behalf Cash
    elif TXN_Type == "POB" and (TXN_Receipt_Note == "EasyPay"):
        TXN_Type_Label = "Payment (EasyPay)"  # Payment of behalf EasyPay
    elif (TXN_Type == "POB") and (TXN_Receipt_Note == "Credit Card"):
        TXN_Type_Label = "Payment (Credit Card)"  # Payment of behalf Credit Card
    elif TXN_Type == "POB":
        TXN_Type_Label = "Payment (EFT)"  # Payment of Behalf Direct Payment
    elif TXN_Type == "PRE":
        TXN_Type_Label = "Payment (ATM)"  # ATM Payment
    elif TXN_Type == "TRF":
        TXN_Type_Label = "Payment (Transfer)"  # Transfer
    elif TXN_Type == "RFN":
        TXN_Type_Label = "Payment (Refund)"
    elif (TXN_Type == "ADJ") and (TXN_Credit == 1):
        TXN_Type_Label = "Adjustment (Credit) (Cell C)"
    elif (TXN_Type == "ADJ") and (TXN_Debit == 1):
        TXN_Type_Label = "Adjustment (Debit) (Cell C)"
    elif (TXN_Type == "OCC") and (TXN_Credit == 1):
        TXN_Type_Label = "* Cell C TXN Type=OCC (Credit)"
    elif (TXN_Type == "OCC") and (TXN_Debit == 1):
        TXN_Type_Label = "* Cell C TXN Type=OCC (Debit)"
    elif TXN_Type == "SAL":
        TXN_Type_Label = "* Cell C TXN Type=SAL (sale?)"
    elif TXN_Type == "RTN":
        TXN_Type_Label = "* Cell C TXN Type=RTN (return?)"
    elif TXN_Type == "ADV":
        TXN_Type_Label = "* Advance (Cell C)"  # Advance
    elif TXN_Type == "REJ":
        TXN_Type_Label = "* Rejection (Cell C)"  # Rejection
    elif TXN_Type == "RCH":
        TXN_Type_Label = "* Recharge (Cell C)"  # Recharge
    elif TXN_Type == "DEP":
        TXN_Type_Label = "* Direct Deposit (Cell C)"  # Direct Deposit - no transactions found in the history
    elif (TXN_Type == "BIL") and (TXN_Credit == 1):
        TXN_Type_Label = "Invoice (Reversal)"
    elif (TXN_Type == "BIL") and (TXN_Debit == 1):
        TXN_Type_Label = "Invoice (Subscription)"
    elif (TXN_Type == "INV") and (TXN_Credit == 1):
        TXN_Type_Label = "Invoice (Reversal)"
    elif (TXN_Type == "INV") and (TXN_Debit == 1):
        TXN_Type_Label = "Invoice (Subscription)"
    elif (TXN_Type == "CSH") and (TXN_RC == "SCA") and (TXN_Credit == 1):
        TXN_Type_Label = TXN_RC_Label  # Cash
    elif (TXN_Type == "CSH") and (TXN_RC == "SCA") and (TXN_Debit == 1):
        TXN_Type_Label = "Payment (Cash Reversal)"
    elif (TXN_Type == "CSH") and (TXN_RC in ["SCJ", "SCY"]):
        TXN_Type_Label = TXN_RC_Label  # Easy Pay
    elif (TXN_Type == "CSH") and (TXN_RC == "SCN"):
        TXN_Type_Label = TXN_RC_Label  # PBX - Is this PayGate?
    elif (TXN_Type == "CSH") and (TXN_RC in ["SCQ", "SCU"]):
        TXN_Type_Label = "Payment (Debit Order Void)"  # Debit Order Reversal
    elif (TXN_Type == "CSH") and (TXN_RC == "SCR"):
        TXN_Type_Label = TXN_RC_Label  # Nedbank EFT
    elif (TXN_Type == "CSH") and (TXN_RC == "SCD"):
        TXN_Type_Label = TXN_RC_Label  # Debt Collector Payment
    elif (TXN_Type == "CSH") and (TXN_Credit == 1):
        TXN_Type_Label = "Payment (Debit Order)"  # Debit Order Payment
    elif TXN_Type == "CSH" and (TXN_Debit == 1):
        TXN_Type_Label = "Payment (Debit Order Void)"  # Debit Order Reversal
    elif (TXN_Type == "VSP") and (TXN_Credit == 1):
        TXN_Type_Label = "Payment (Debit Order)"  # Debit Order Payment
    elif (TXN_Type == "VSP") and (TXN_Debit == 1):
        TXN_Type_Label = "Payment (Debit Order Void)"  # Debit Order Reversal
    elif (TXN_Type == "JRN") and (TXN_Credit == 1):
        TXN_Type_Label = "Journal Entry (Credit)"
    elif (TXN_Type == "JRN") and (TXN_Debit == 1):
        TXN_Type_Label = "Journal Entry (Debit)"
    elif (TXN_Type == "CRN") and (TXN_Credit == 1):
        TXN_Type_Label = "Credit Note"
    elif (TXN_Type == "CRN") and (TXN_Debit == 1):
        TXN_Type_Label = "Credit Note (Reversal)"
    elif TXN_Type == "ADM":
        TXN_Type_Label = "Fee (Debit Order Void)"
    elif TXN_Type == "PST":
        TXN_Type_Label = "Fee (Postal Charge)"
    elif TXN_Type == "CRP":
        TXN_Type_Label = "* CEC TXN Type = CRP"
    elif TXN_Type == "REF":
        TXN_Type_Label = "* CEC TXN Type = REF"
    else:
        TXN_Type_Label = "* TXN Type Unknown"

    return TXN_Type_Label


udf_Transaction_Type = f.udf(Transaction_Type,
                             returnType=t.StringType())


def Transaction_Receipt(REC, REM, RES):
    """
    :param REC:  The Receipt Number.
    :param REM:  Remark attached to the Transaction.
    :param RES:  Reserved variable (8) used to store banking information.
    """
    if REC is not None:
        pass
    else:
        REC = ""

    if REC != "":
        TXN_Receipt = 1

    TXN_Receipt_Type_tmp = str("".join(e.upper() for e in REC if e.isalpha()))[0]  # 1st char indicates the receipt type
    if TXN_Receipt_Type_tmp == "P":
        TXN_Receipt_Type = "Payment"
    elif TXN_Receipt_Type_tmp == "V":
        TXN_Receipt_Type = "Void"
    elif TXN_Receipt_Type_tmp == "F":
        TXN_Receipt_Type = "Refund"
    elif TXN_Receipt_Type_tmp == "T":
        TXN_Receipt_Type = "Transfer"
    elif TXN_Receipt_Type_tmp == "R":
        TXN_Receipt_Type = "????"
    else:
        TXN_Receipt_Type = copy.deepcopy(TXN_Receipt_Type_tmp)

    # The Receipt Notes are only used by Cell C, but has valuable manual payment information.
    if REM is not None:
        REM = REM.upper()
    else:
        REM = ""

    if "REFUND" in REM:
        TXN_Receipt_Note = "Payment Refund"
    elif "CREDIT" in REM:
        TXN_Receipt_Note = "Credit Card"
    elif "EFT" in REM:
        TXN_Receipt_Note = "EasyPay"
    elif "EASYPAY" in REM:
        TXN_Receipt_Note = "EasyPay"
    elif "NEDLINK" in REM:
        TXN_Receipt_Note = "Nedbank"
    elif "ABSA" in REM:
        TXN_Receipt_Note = "ABSA"
    elif "HYPHEN" in REM:
        TXN_Receipt_Note = "Hyphen"  # DebiCheck
    elif "CASH" in REM:
        TXN_Receipt_Note = "Cash"
    elif "FCKNY" in REM:
        TXN_Receipt_Note = "* FCKNY"
    elif "FLAL" in REM:
        TXN_Receipt_Note = "* FLAL"
    elif "FMAP" in REM:
        TXN_Receipt_Note = "* FMAP"
    elif "FNMR" in REM:
        TXN_Receipt_Note = "* FNMR"
    else:
        TXN_Receipt_Note = ""

    # Proceed with extracting the Bank Name.

    if RES is not None:
        RES = "".join(e.upper() for e in RES if e.isalpha())
    else:
        RES = ""

    if "CAPITEC" in RES:
        TXN_Receipt_Bank = "Capitec"  # Number 1, Highest Volume.
    elif "FIRST" in RES:
        TXN_Receipt_Bank = "FNB"  # Number 2
    elif "RAND" in RES:
        TXN_Receipt_Bank = "FNB"
    elif "FNB" in RES:
        TXN_Receipt_Bank = "FNB"
    elif "STANDARD" in RES:
        TXN_Receipt_Bank = "SBSA"  # Number 3
    elif "STD" in RES:
        TXN_Receipt_Bank = "SBSA"
    elif "CLEARY" in RES:
        TXN_Receipt_Bank = "SBSA"
    elif "ABSA" in RES:
        TXN_Receipt_Bank = "ABSA"  # Number 4
    elif "TRUST" in RES:
        TXN_Receipt_Bank = "ABSA"
    elif "SAAMBOU" in RES:
        TXN_Receipt_Bank = "ABSA"
    elif "VOLK" in RES:
        TXN_Receipt_Bank = "ABSA"
    elif "NEDBANK" in RES:
        TXN_Receipt_Bank = "Nedbank"  # Number 5
    elif "PEOPLE" in RES:
        TXN_Receipt_Bank = "Nedbank"
    elif "PERM" in RES:
        TXN_Receipt_Bank = "Nedbank"
    elif "UNITED" in RES:
        TXN_Receipt_Bank = "United Bank"  # Number 6
    elif "NBS" in RES:
        TXN_Receipt_Bank = "NBS"  # Number 7
    elif "INVEST" in RES:
        TXN_Receipt_Bank = "Investec"  # Number 8
    elif "BIDVEST" in RES:
        TXN_Receipt_Bank = "Bidvest"  # Number 9
    elif "AFRICAN" in RES:
        TXN_Receipt_Bank = "African"  # Number 10
    elif "DISCOVERY" in RES:
        TXN_Receipt_Bank = "Discovery"
    elif "ITHALA" in RES:
        TXN_Receipt_Bank = "Ithala Bank"
    elif "ATHENS" in RES:
        TXN_Receipt_Bank = "Bank of Athens"
    elif "LISBON" in RES:
        TXN_Receipt_Bank = "Bank of Lisbon"
    elif "TRANSKEI" in RES:
        TXN_Receipt_Bank = "Bank of Transkei"
    elif "ALLIED" in RES:
        TXN_Receipt_Bank = "Allied Bank"
    elif "POST" in RES:
        TXN_Receipt_Bank = "Postbank"
    elif "POST" in RES:
        TXN_Receipt_Bank = "Postbank"
    elif "SASFIN" in RES:
        TXN_Receipt_Bank = "Sasfin Bank"
    elif "TEBA" in RES:
        TXN_Receipt_Bank = "Teba Bank"
    elif "TYME" in RES:
        TXN_Receipt_Bank = "Tyme Bank"
    elif "ALBARAKA" in RES:
        TXN_Receipt_Bank = "Albaraka Bank"
    elif "Commercial" in RES:
        TXN_Receipt_Bank = "Nambia"
    elif "WINDHOEK" in RES:
        TXN_Receipt_Bank = "Bank Windhoek"
    elif "BUSINESS" in RES:
        TXN_Receipt_Bank = "BOE Bank"
    elif "BOE" in RES:
        TXN_Receipt_Bank = "BOE Bank"
    elif "CITI" in RES:
        TXN_Receipt_Bank = "Citi Bank"
    elif "HABIB" in RES:
        TXN_Receipt_Bank = "Habib Bank"
    elif "HBZ" in RES:
        TXN_Receipt_Bank = "Habib Bank"
    elif "HSBC" in RES:
        TXN_Receipt_Bank = "HSBC"
    elif "MEEG" in RES:
        TXN_Receipt_Bank = "Meeg Bank"
    elif "OLDMUTUAL" in RES:
        TXN_Receipt_Bank = "Old Mutual Bank"
    elif "MERC" in RES:
        TXN_Receipt_Bank = "Mercantile Bank"
    elif "VBS" in RES:
        TXN_Receipt_Bank = "VBS Mutual Bank"
    elif "UNIBANK" in RES:
        TXN_Receipt_Bank = "UBank"
    elif "DUMMY" in RES:
        TXN_Receipt_Bank = ""
    elif "REMOTE" in RES:
        TXN_Receipt_Bank = ""
    elif "RIVONIA" in RES:
        TXN_Receipt_Bank = ""
    elif "UNIDENTI" in RES:
        TXN_Receipt_Bank = ""
    elif "UNKNOWN" in RES:
        TXN_Receipt_Bank = ""
    elif len(RES) == 0:
        TXN_Receipt_Bank = ""
    elif RES == "A":
        TXN_Receipt_Bank = ""
    else:
        if RES != "":
            TXN_Receipt_Bank = "* " + str(RES)
        else:
            TXN_Receipt_Bank = ""

    return TXN_Receipt_Type, TXN_Receipt_Note, TXN_Receipt_Bank


schema_Transaction_Receipt = t.StructType([
    t.StructField("TXN_Receipt_Type", t.StringType(), True),
    t.StructField("TXN_Receipt_Note", t.StringType(), True),
    t.StructField("TXN_Receipt_Bank", t.StringType(), True)
])


udf_Transaction_Receipt = f.udf(Transaction_Receipt,
                                returnType=schema_Transaction_Receipt)
