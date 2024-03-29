import pyspark.sql.types as t
import pyspark.sql.functions as f

from datetime import date
from dateutil.relativedelta import relativedelta


def Valid_ID(ID):
    def compress(orig_string, width):
        val = orig_string.replace(" ", "").strip()
        val_1 = val.ljust(width)
        return val_1

    def eliminate(orig_string, to_be_eliminated):
        return orig_string.translate({ord(c): None for c in to_be_eliminated})

    def repl_strip(orig_string):
        return orig_string.replace(" ", "").strip()
    ID = str(ID).lstrip()
    if ID.endswith(".0"):
        ID = ID[:-2]
    ID = ID.lstrip()
    IDKey = None
    IDFix = compress(ID.replace(" ", ""), 15)
    IDFailure = ""
    IDDOB = None
    IDDOBX = None
    IDExpand = ""
    IDAge = None
    IDGender = ""

    IDFix = compress(IDFix, 15)
    if IDFix[0:2] == "CK":
        IDFix = IDFix[2:]
    if (IDFix[0] in [".", "/"]) and (len(IDFix) > 2):
        IDFix = IDFix[1:]

    # ID TYPE FIRST PASS : Assign initially expected ID Type
    if len(repl_strip(IDFix)) >= 5:
        if (len(IDFix.strip()) == 13) and (eliminate(IDFix, "0123456789 ") == ""):
            IDType = "I"
        elif ((len(IDFix.strip()) == 14) and (IDFix[4] == "/") and (IDFix[11] == "/") and
              (eliminate(IDFix, "/0123456789 ") == "") and (IDFix[:4] > "1800")):
            print("No")
            IDType = "B"
        elif ((len(IDFix.strip()) == 13) and (IDFix[4] == "/") and (IDFix[10] == "/") and
              (eliminate(IDFix, "/0123456789 ") == "") and (IDFix[:4] > "1800")):
            IDType = "b"
        elif ((len(IDFix.strip()) == 12) and (IDFix[4] == "/") and (IDFix[9] == "/") and
              (eliminate(IDFix, "/0123456789 ") == "") and (IDFix[:4] > "1800")):
              IDType = "b"
        elif ((len(IDFix.strip()) == 12) and (eliminate(IDFix, "0123456789 ") == "") and
              (int(IDFix[:4]) > 1800)):
            IDFix = compress("/".join([IDFix[0:4], IDFix[4:10], IDFix[10:]]), 15)
            IDType = "b"
        elif ((len(IDFix.strip()) == 12) and (IDFix[2] == "/") and (IDFix[9] == "/") and
              (eliminate(IDFix, "/0123456789 ") == "")):
            IDFix = compress("00" + str(IDFix), 15)
            IDType = "b"
        elif ((len(IDFix.strip()) == 11) and (IDFix[2] == "/") and (IDFix[8] == "/") and
              (eliminate(IDFix, "/0123456789 ") == "")):
            IDFix = ("00" + str(IDFix)).rstrip()
            IDFix = compress(IDFix[0:5] + "0" + IDFix[5:], 15)
            IDType = "b"
        elif ((len(IDFix.strip()) == 10) and (IDFix[2] == "/") and (IDFix[7] == "/") and
              (eliminate(IDFix, "/0123456789 ") == "")):
            IDFix = ("00" + str(IDFix)).rstrip()
            IDFix = compress(IDFix[0:5] + "00" + IDFix[5:], 15)
            IDType = "b"
        elif (len(IDFix.strip()) >= 3) and (IDFix[0:2].upper() == "IT") and\
             (IDFix[2] in ["0", "1", "2", "3", "4", "5", "6", "7", "8", "9"]):
            IDType = "T"
        else:
            IDType = "X"
            IDFailure = "00 Unsure ID Type"
    else:
        IDType = "X"
        IDFailure = "00 Short ID (<=5)"
        if len(IDFix) == 0:
            IDFailure = "00 Blank ID"
# ----------------------------------------------------------------------------------------------------------------------------------

    if IDType == "I":
        if IDFix[0:2] in [str(x).zfill(2) for x in range(0, 6)]:
            Year = "20"
        else:
            Year = "19"
        if IDFix[2:4] not in [str(x).zfill(2) for x in range(1, 13)]:
            IDType = "X"
            IDFailure = "01 Month Not 01-12"
        if IDFix[4:6] not in [str(x).zfill(2) for x in range(1, 32)]:
            IDType = "X"
            IDFailure = "02 Days Not 01-31"
        val = IDFix[2:4]
        if val == "01":
            Mth = "Jan"
        elif val == "02":
            Mth = "Feb"
            if IDFix[4:6] in ["30", "31"]:
                IDType = "X"
                IDFailure = "03 FEB Day 29-31"
            elif (IDFix[4:6] == "29") and (IDFix[0:2] not in [str(x).zfill(2) for x in range(0, 97, 4)]):
                IDType = "X"
                IDFailure = "04 Not Leap Year"
            else:
                pass
        elif val == "03":
            Mth = "Mar"
        elif val == "04":
            Mth = "Apr"
            if IDFix[4:6] == "31":
                IDType = "X"
                IDFailure = "05 Apr Day 31"
        elif val == "05":
            Mth = "May"
        elif val == "06":
            Mth = "Jun"
            if IDFix[4:6] == "31":
                IDType = "X"
                IDFailure = "06 Jun Day 31"
        elif val == "07":
            Mth = "Jun"
        elif val == "08":
            Mth = "Aug"
        elif val == "09":
            Mth = "Sep"
            if IDFix[4:6] == "31":
                IDType = "X"
                IDFailure = "07 Sep Day 31"
        elif val == "10":
            Mth = "Oct"
        elif val == "11":
            Mth = "Nov"
            if IDFix[4:6] == "31":
                IDType = "X"
                IDFailure = "08 Nov Day 31"
        elif val == "12":
            Mth = "Dec"
        else:
            Mth = "XXX"
            IDType = "X"
            IDFailure = "09 Unknown Month"

        if IDType == "I":
            IDOdd = int(IDFix[0]) + int(IDFix[2]) + int(IDFix[4]) + int(IDFix[6]) + int(IDFix[8]) + int(IDFix[10])
            even_nums = int(str(IDFix[1]) + str(IDFix[3]) + str(IDFix[5]) + str(IDFix[7])
                            + str(IDFix[9]) + str(IDFix[11])) * 2
            sum_even_nums = 0
            for x in str(even_nums):
                sum_even_nums += int(x)
            total = sum_even_nums + IDOdd

            control_char = int(str(total)[len(str(total)) - 1])

            if control_char != 0:
                control_char = 10 - control_char

            if control_char == int(IDFix[12]):
                if IDFix[6] in [str(x) for x in range(0, 5)]:
                    IDGender = "F"
                else:
                    IDGender = "M"
            else:
                IDType = "X"
                IDFailure = "10 Failed Modulus 11"

        if IDType == "I":
            IDDOB = date(int(Year + IDFix[0:2]), int(IDFix[2:4]), int(IDFix[4:6]))
            IDDOBX = Year + IDFix[0:6]
            IDExpand = " ".join([IDFix[4:6], Mth, Year + IDFix[0:2]])
            IDAge = relativedelta(date.today(), IDDOB).years

        val2 = IDFix[10:12]
        if val2 in ["08", "09"]:
            IDType = "N"
        elif val2 in [str(x).zfill(2) for x in range(0, 8)]:
            IDType = "n"
        elif val2 in ["18", "19"]:
            IDType = "I"
        else:
            IDType = "i"

        IDKey = IDFix[0:10]

# ---------------------------------------------------------------------------------------------------------

    if IDType in ["B", "b"]:
        IDGender = "B"
        IDKey = IDFix[:]
        val3 = IDFix[12: 14]
        if val3 == "06":
            IDExpand = "Public"
        elif val3 == "07":
            IDExpand = "Private"
        elif val3 == "08":
            IDExpand = "Society"
        elif val3 == "09":
            IDExpand = "Association"
        elif val3 == "10":
            IDExpand = "Foreign"
        elif val3 in ["12", "14", "20"]:
            IDExpand = "Sundry"
        elif val3 == "21":
            IDExpand = "Professional"
        elif val3 == "23":
            IDExpand = "Closed Corp"
        elif val3 == "24":
            IDExpand = "Co-operative"
        elif val3 in ["25", "26"]:
            IDExpand = "Agriculture"
        else:
            IDType = "X"
            IDFailure = "11 Invalid Company Type"
            IDGender = ""
            IDKey = ""

    if IDType == "T":
        IDGender = "T"
        IDKey = IDFix[:]

    if IDFix[0: 10] == "0" * 10:
        IDKey = ""
        IDType = ""
        IDFailure = "0000000000 ID"
        IDDOB = None
        IDDOBX = ""
        IDExpand = ""
        IDAge = None
        IDGender = ""

    if IDFailure != "":
        IDFix = ""
        IDKey = ""

    if IDFailure == "" and IDType in ("I", "N"):
        IDReport = "N"
    elif IDFailure == "" and IDType in ("i", "n"):
        IDReport = "O"
    elif IDFailure == "00 Blank ID":
        IDReport = "-"
    else:
        IDReport = "X"

    IDFix = repl_strip(IDFix)

    # pos 1      2       3          4      5       6         7      8         9      10
    # idx 0      1       2          3      4       5         6      7         8      9
    ls = [IDKey, IDType, IDFailure, IDDOB, IDDOBX, IDExpand, IDAge, IDGender, IDFix, IDReport]
    return ls


schema_id = t.StructType([
    t.StructField("IDKey", t.StringType(), True),
    t.StructField("IDType", t.StringType(), True),
    t.StructField("IDFailure", t.StringType(), True),
    t.StructField("IDDOB", t.DateType(), True),
    t.StructField("IDDOBX", t.StringType(), True),
    t.StructField("IDExpand", t.StringType(), True),
    t.StructField("IDAge", t.IntegerType(), True),
    t.StructField("IDGender", t.StringType(), True),
    t.StructField("IDFix", t.StringType(), True),
    t.StructField("IDReport", t.StringType(), True)
])

udf_Valid_ID = f.udf(Valid_ID,
                     returnType=schema_id)
