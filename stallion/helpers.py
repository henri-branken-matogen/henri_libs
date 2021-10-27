from datetime import date
from dateutil.relativedelta import relativedelta
import pyspark.sql.functions as f


def corrupt_C_or_P_to_2(sdf_base, col_old, col_young, cycle_name_val):
    """
    :param sdf_base:  Our input dataframe.
    :param col_old:  The MTH stamp that is 1 month more historic with respect to col_young.
    :param col_young:  The MTH stamp that is 1 month younger with respect to col_old.
                       So, an example (col_old, col_young) combination would be (JUN21, JUL21).
    :param cycle_name_val:  An text description telling us in which monthly transition the problem occurred.
    :return:  Returns `sdf_return`, which is basically `sdf_base` with updated values for the columns
              ["problem", "interpretation", "cycle_id"].
    """
    sdf_return = sdf_base\
        .withColumn("problem", f.when((f.col(col_old) == "C") & (f.col(col_young) == "2"), "From C to 2")
                                .otherwise(f.col("problem")))\
        .withColumn("interpretation", f.when((f.col(col_old) == "C") & (f.col(col_young) == "2"), "aged 2 cycles")
                                       .otherwise(f.col("interpretation")))\
        .withColumn("cycle_id", f.when((f.col(col_old) == "C") & (f.col(col_young) == "2"), cycle_name_val)
                                 .otherwise(f.col("cycle_id")))\
        .withColumn("problem", f.when((f.col(col_old) == "P") & (f.col(col_young) == "2"), "From P to 2")
                                .otherwise(f.col("problem")))\
        .withColumn("interpretation", f.when((f.col(col_old) == "P") & (f.col(col_young) == "2"), "aged 2 cycles")
                                       .otherwise(f.col("interpretation")))\
        .withColumn("cycle_id", f.when((f.col(col_old) == "P") & (f.col(col_young) == "2"), cycle_name_val)
                                 .otherwise(f.col("cycle_id")))
    return sdf_return


def corrupt_C_or_P_to_3(sdf_base, col_old, col_young, cycle_name_val):
    """
    :param sdf_base:  Our input dataframe.
    :param col_old:  The MTH stamp that is 1 month more historic with respect to col_young.
    :param col_young:  The MTH stamp that is 1 month younger with respect to col_old.
                       So, an example (col_old, col_young) combination would be (JUN21, JUL21).
    :param cycle_name_val:  An text description telling us in which monthly transition the problem occurred.
                            An example would be "AUG21 to SEP21".
    :return:  Returns `sdf_return`, which is basically `sdf_base` with updated values for the columns
              ["problem", "interpretation", "cycle_id"].
    """
    sdf_return = sdf_base\
        .withColumn("problem", f.when((f.col(col_old) == "C") & (f.col(col_young) == "3"), "From C to 3")
                                .otherwise(f.col("problem")))\
        .withColumn("interpretation", f.when((f.col(col_old) == "C") & (f.col(col_young) == "3"), "aged 3 cycles")
                                       .otherwise(f.col("interpretation")))\
        .withColumn("cycle_id", f.when((f.col(col_old) == "C") & (f.col(col_young) == "3"), cycle_name_val)
                                 .otherwise(f.col("cycle_id")))\
        .withColumn("problem", f.when((f.col(col_old) == "P") & (f.col(col_young) == "3"), "From P to 3")
                                .otherwise(f.col("problem")))\
        .withColumn("interpretation", f.when((f.col(col_old) == "P") & (f.col(col_young) == "3"), "aged 3 cycles")
                                       .otherwise(f.col("interpretation")))\
        .withColumn("cycle_id", f.when((f.col(col_old) == "P") & (f.col(col_young) == "3"), cycle_name_val)
                                 .otherwise(f.col("cycle_id")))
    return sdf_return


def corrupt_C_or_P_to_4(sdf_base, col_old, col_young, cycle_name_val):
    """
    :param sdf_base:  Our input dataframe.
    :param col_old:  The MTH stamp that is 1 month more historic with respect to col_young.
    :param col_young:  The MTH stamp that is 1 month younger with respect to col_old.
                       So, an example (col_old, col_young) combination would be (JUN21, JUL21).
    :param cycle_name_val:  An text description telling us in which monthly transition the problem occurred.
                            An example would be "AUG21 to SEP21".
    :return:  Returns `sdf_return`, which is basically `sdf_base` with updated values for the columns
              ["problem", "interpretation", "cycle_id"].
    """
    sdf_return = sdf_base\
        .withColumn("problem", f.when((f.col(col_old) == "C") & (f.col(col_young) == "4"), "From C to 4")
                                .otherwise(f.col("problem")))\
        .withColumn("interpretation", f.when((f.col(col_old) == "C") & (f.col(col_young) == "4"), "aged 4 cycles")
                                       .otherwise(f.col("interpretation")))\
        .withColumn("cycle_id", f.when((f.col(col_old) == "C") & (f.col(col_young) == "4"), cycle_name_val)
                                 .otherwise(f.col("cycle_id")))\
        .withColumn("problem", f.when((f.col(col_old) == "P") & (f.col(col_young) == "4"), "From P to 4")
                                .otherwise(f.col("problem")))\
        .withColumn("interpretation", f.when((f.col(col_old) == "P") & (f.col(col_young) == "4"), "aged 4 cycles")
                                       .otherwise(f.col("interpretation")))\
        .withColumn("cycle_id", f.when((f.col(col_old) == "P") & (f.col(col_young) == "4"), cycle_name_val)
                                 .otherwise(f.col("cycle_id")))
    return sdf_return


def corrupt_zero_to_two(sdf_base, col_old, col_young, cycle_name_val):
    """
    :param sdf_base:  Our input dataframe.
    :param col_old:  The MTH stamp that is 1 month more historic with respect to col_young.
    :param col_young:  The MTH stamp that is 1 month younger with respect to col_old.
                       So, an example (col_old, col_young) combination would be (JUN21, JUL21).
    :param cycle_name_val:  An text description telling us in which monthly transition the problem occurred.
                            An example would be "AUG21 to SEP21".
    :return:  Returns `sdf_return`, which is basically `sdf_base` with updated values for the columns
              ["problem", "interpretation", "cycle_id"].
    """
    sdf_return = sdf_base\
        .withColumn("problem", f.when((f.col(col_old) == "0") & (f.col(col_young) == "2"), "From 0 to 2")
                                .otherwise(f.col("problem")))\
        .withColumn("interpretation", f.when((f.col(col_old) == "0") & (f.col(col_young) == "2"), "aged 2 cycles")
                                       .otherwise(f.col("interpretation")))\
        .withColumn("cycle_id", f.when((f.col(col_old) == "0") & (f.col(col_young) == "2"), cycle_name_val)
                                 .otherwise(f.col("cycle_id")))
    return sdf_return


def corrupt_zero_to_three(sdf_base, col_old, col_young, cycle_name_val):
    """
    :param sdf_base:  Our input dataframe.
    :param col_old:  The MTH stamp that is 1 month more historic with respect to col_young.
    :param col_young:  The MTH stamp that is 1 month younger with respect to col_old.
                       So, an example (col_old, col_young) combination would be (JUN21, JUL21).
    :param cycle_name_val:  An text description telling us in which monthly transition the problem occurred.
                            An example would be "AUG21 to SEP21".
    :return:  Returns `sdf_return`, which is basically `sdf_base` with updated values for the columns
              ["problem", "interpretation", "cycle_id"].
    """
    sdf_return = sdf_base\
        .withColumn("problem", f.when((f.col(col_old) == "0") & (f.col(col_young) == "3"), "From 0 to 3")
                                .otherwise(f.col("problem")))\
        .withColumn("interpretation", f.when((f.col(col_old) == "0") & (f.col(col_young) == "3"), "aged 3 cycles")
                                       .otherwise(f.col("interpretation")))\
        .withColumn("cycle_id", f.when((f.col(col_old) == "0") & (f.col(col_young) == "3"), cycle_name_val)
                                 .otherwise(f.col("cycle_id")))
    return sdf_return


def corrupt_zero_to_four(sdf_base, col_old, col_young, cycle_name_val):
    """
    :param sdf_base:  Our input dataframe.
    :param col_old:  The MTH stamp that is 1 month more historic with respect to col_young.
    :param col_young:  The MTH stamp that is 1 month younger with respect to col_old.
                       So, an example (col_old, col_young) combination would be (JUN21, JUL21).
    :param cycle_name_val:  An text description telling us in which monthly transition the problem occurred.
                            An example would be "AUG21 to SEP21".
    :return:  Returns `sdf_return`, which is basically `sdf_base` with updated values for the columns
              ["problem", "interpretation", "cycle_id"].
    """
    sdf_return = sdf_base\
        .withColumn("problem", f.when((f.col(col_old) == "0") & (f.col(col_young) == "4"), "From 0 to 4")
                                .otherwise(f.col("problem")))\
        .withColumn("interpretation", f.when((f.col(col_old) == "0") & (f.col(col_young) == "4"), "aged 4 cycles")
                                       .otherwise(f.col("interpretation")))\
        .withColumn("cycle_id", f.when((f.col(col_old) == "0") & (f.col(col_young) == "4"), cycle_name_val)
                                 .otherwise(f.col("cycle_id")))
    return sdf_return


def corrupt_one_to_three(sdf_base, col_old, col_young, cycle_name_val):
    """
    :param sdf_base:  Our input dataframe.
    :param col_old:  The MTH stamp that is 1 month more historic with respect to col_young.
    :param col_young:  The MTH stamp that is 1 month younger with respect to col_old.
                       So, an example (col_old, col_young) combination would be (JUN21, JUL21).
    :param cycle_name_val:  An text description telling us in which monthly transition the problem occurred.
                            An example would be "AUG21 to SEP21".
    :return:  Returns `sdf_return`, which is basically `sdf_base` with updated values for the columns
              ["problem", "interpretation", "cycle_id"].
    """
    sdf_return = sdf_base\
        .withColumn("problem", f.when((f.col(col_old) == "1") & (f.col(col_young) == "3"), "From 1 to 3")
                                .otherwise(f.col("problem")))\
        .withColumn("interpretation", f.when((f.col(col_old) == "1") & (f.col(col_young) == "3"), "aged 2 cycles")
                                       .otherwise(f.col("interpretation")))\
        .withColumn("cycle_id", f.when((f.col(col_old) == "1") & (f.col(col_young) == "3"), cycle_name_val)
                                 .otherwise(f.col("cycle_id")))
    return sdf_return


def corrupt_one_to_four(sdf_base, col_old, col_young, cycle_name_val):
    """
    :param sdf_base:  Our input dataframe.
    :param col_old:  The MTH stamp that is 1 month more historic with respect to col_young.
    :param col_young:  The MTH stamp that is 1 month younger with respect to col_old.
                       So, an example (col_old, col_young) combination would be (JUN21, JUL21).
    :param cycle_name_val:  An text description telling us in which monthly transition the problem occurred.
                            An example would be "AUG21 to SEP21".
    :return:  Returns `sdf_return`, which is basically `sdf_base` with updated values for the columns
              ["problem", "interpretation", "cycle_id"].
    """
    sdf_return = sdf_base\
        .withColumn("problem", f.when((f.col(col_old) == "1") & (f.col(col_young) == "4"), "From 1 to 4")
                                .otherwise(f.col("problem")))\
        .withColumn("interpretation", f.when((f.col(col_old) == "1") & (f.col(col_young) == "4"), "aged 3 cycles")
                                       .otherwise(f.col("interpretation")))\
        .withColumn("cycle_id", f.when((f.col(col_old) == "1") & (f.col(col_young) == "4"), cycle_name_val)
                                 .otherwise(f.col("cycle_id")))
    return sdf_return


def corrupt_two_to_four(sdf_base, col_old, col_young, cycle_name_val):
    """
    :param sdf_base:  Our input dataframe.
    :param col_old:  The MTH stamp that is 1 month more historic with respect to col_young.
    :param col_young:  The MTH stamp that is 1 month younger with respect to col_old.
                       So, an example (col_old, col_young) combination would be (JUN21, JUL21).
    :param cycle_name_val:  An text description telling us in which monthly transition the problem occurred.
                            An example would be "AUG21 to SEP21".
    :return:  Returns `sdf_return`, which is basically `sdf_base` with updated values for the columns
              ["problem", "interpretation", "cycle_id"].
    """
    sdf_return = sdf_base\
        .withColumn("problem", f.when((f.col(col_old) == "2") & (f.col(col_young) == "4"), "From 2 to 4")
                                .otherwise(f.col("problem")))\
        .withColumn("interpretation", f.when((f.col(col_old) == "2") & (f.col(col_young) == "4"), "aged 2 cycles")
                                       .otherwise(f.col("interpretation")))\
        .withColumn("cycle_id", f.when((f.col(col_old) == "2") & (f.col(col_young) == "4"), cycle_name_val)
                                 .otherwise(f.col("cycle_id")))
    return sdf_return


def compare(value, cutoffs, label):
    """
    Compares the value of a percentage deviation with the lower and upper cutoffs.
    If it is within the cutoffs, then a one-liner message is printed on the screen, telling you the value is acceptable.
    Else, an Exception is raised.
    :param value: The percentage deviation, normalised to a percentage value (not a ratio value).
    A Real Number.
    :param cutoffs: A tuple of real numbers specifying the lower and upper cutoffs.  [a, b].
    :param label: A string text description telling us what the comparison is performed for.
    :return: Nothing is returned.  Only a stringtext is printed on the screen.
    """
    if (value >= cutoffs[0]) and (value <= cutoffs[1]):
        print(f"% deviation for '{label}' are within accepted tolerances.")
    else:
        raise Exception(f"% deviation of {value:.2f}, for '{label}', are not within the boundaries of {cutoffs}.")
    return None


def gen_stamp_suffixes(ccyymm_01, steps, reversal=False):
    """
    Generate and return a list of 36 ccyymm stamps.
    The first stamp in the list is the most recent, and corresponds to month 01.
    The last stamp in the list is the most historic, and corresponds to month 36.
    :param ccyymm_01: The latest ccyymm stamp, in string format.
    :param steps: The number of stamps we must generate.  It corresponds to the length of the returned list.
    steps must be in integer format.
    :return:  A list of `steps` ccyymm stamps that are in string format.
    E.g.: ["202101", "202012", ..., "201803", "201802"].
    """
    yyyy = int(ccyymm_01[:4])
    mm = int(ccyymm_01[4:])
    dte_01 = date(year=yyyy, month=mm, day=1)
    dte_00 = dte_01 + relativedelta(months=1)  # 1 month into the future with respect to `dte_01`.
    lookup_dict = dict()
    if not reversal:
        for n_months in range(1, steps + 1):
            dte = dte_00 - relativedelta(months=n_months)
            stamp = dte.strftime("%Y%m")
            lookup_dict[n_months] = stamp
        return lookup_dict
    else:
        for n_months in range(1, steps + 1):
            x = steps - n_months
            dte = dte_01 - relativedelta(months=x)
            stamp = dte.strftime("%Y%m")
            lookup_dict[n_months] = stamp
        return lookup_dict


def gen_year_month_str_pairs(yyyy_latest=9999, mm_latest=99, today_switch=True):
    """
    Generate a tuple of (y_2, m_2, y_1, m_1) strings where (y_2, m_2) pertains to the year and month number of the
    latest/current month.  (y_1, m_1) pertains to the year and month number of exactly 1 month before (y_2, m_2).
    :param yyyy_latest:  The latest year.  Of type String.
    :param mm_latest:  The latest month.  Of type String.
    :param today_switch:  A Boolean value (True or False).  True by default.
    If True, then override `yyyy_latest` and `mm_latest`, and infer `y_2`, `m_2`, `y_1`, `m_1` from the date data of
    today.
    If False, use the user-supplied `yyyy_latest` and `mm_latest`, and infer `y_2`, `m_2`, `y_1`, `m_1` from those two
    parameters.
    :return:  Return the four values (y_2, m_2, y_1, m_1) in tuple format.  All values are of type string.
    """
    # Use the date-data pertaining to today's date.
    if today_switch:
        # Extract the year and month pertaining to today's date.
        yyyy_latest = date.today().year
        mm_latest = date.today().month
        # Construct a date from `yyyy_latest` and `mm_latest`.
        dte_latest = date(year=yyyy_latest, month=mm_latest, day=10)
        # Go back 1 month into the past w.r.t. `dte_latest`, therefore generating `dte_prev`.
        dte_prev = dte_latest - relativedelta(months=1)
        # Compute `yyyy_slaf` and `mm_slaf` from `dte_latest`.
        yyyy_slaf = str(dte_latest.year)
        mm_slaf = str(dte_latest.month)
        # Compute `yyyy_prev` and `mm_prev` from `dte_prev`.
        yyyy_prev = str(dte_prev.year)
        mm_prev = str(dte_prev.month)
        # Return the four values (y_2, m_2, y_1, m_1) based on today's date-related data.
        return yyyy_slaf, mm_slaf, yyyy_prev, mm_prev
    else:
        # Construct a date from `yyyy_latest` and `mm_latest`.
        dte_latest = date(year=yyyy_latest, month=mm_latest, day=10)
        # Go back 1 month into the past w.r.t. `dte_latest`, therefore generating `dte_prev`.
        dte_prev = dte_latest - relativedelta(months=1)
        # Compute `yyyy_slaf` and `mm_slaf` from `dte_latest`.
        yyyy_slaf = str(dte_latest.year)
        mm_slaf = str(dte_latest.month)
        # Compute `yyyy_prev` and `mm_prev` from `dte_prev`.
        yyyy_prev = str(dte_prev.year)
        mm_prev = str(dte_prev.month)
        # Return the four values (y_2, m_2, y_1, m_1) based on `yyyy_latest` and `mm_latest` parameters.
        return yyyy_slaf, mm_slaf, yyyy_prev, mm_prev
