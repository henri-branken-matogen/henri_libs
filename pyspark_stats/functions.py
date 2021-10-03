import pyspark.sql.functions as F
from dateutil.relativedelta import relativedelta
from datetime import date


def compare_two_columns(sdf_a, sdf_b, on_column_name, col_a_name, col_b_name, join_type="inner"):
    """
    :param sdf_a:  The one dataframe containing the one field to be compared.  Of type PySpark DataFrame.
    :param sdf_b:  The other dataframe containing the other field to be compared with.  Of type PySpark DataFrame.
    :param on_column_name:  The intersection of sdf_a and sdf_b.  I.e., on what column the two should be joined on.
                            Of DType String.
    :param col_a_name:  The one column, contained in sdf_a, that we need to compare.  Of DType string.
    :param col_b_name:  The other column, contained in sdf_b, that we need to compare.  Of DType string.
    :param join_type:  How sdf_a and sdf_a should be joined on the `on_column_name`.  Of DType string.  Default value is 'inner'.
    :return:  ls_vals.  Of DType Python list.  A list of values on which the two dataframes don't agree upon.
    """
    a_ref = col_a_name + "_a"
    a_join = on_column_name + "_a"
    b_ref = col_b_name + "_b"
    b_join = on_column_name + "_b"
    sdf_a = sdf_a\
        .withColumnRenamed(col_a_name, a_ref)\
        .withColumnRenamed(on_column_name, a_join)\
        .select(*[a_join, a_ref])
    sdf_b = sdf_b\
        .withColumnRenamed(col_b_name, b_ref)\
        .withColumnRenamed(on_column_name, b_join)\
        .select(*[b_join, b_ref])
    sdf_comp = sdf_a\
        .join(sdf_b,
              on=(sdf_a[a_join] == sdf_b[b_join]),
              how=join_type)
    sdf_comp_1 = sdf_comp\
        .withColumn("comparison",
                    F.when(((F.col(a_ref).isNull()) & (F.col(b_ref).isNull())), F.lit("equality"))\
                     .when(((F.col(a_ref).isNull()) & (F.col(b_ref).isNotNull())), F.lit("ineq left value is NULL"))\
                     .when(((F.col(a_ref).isNotNull()) & (F.col(b_ref).isNull())), F.lit("ineq right value is NULL"))\
                     .when(F.col(a_ref) == F.col(b_ref), F.lit("equality"))\
                     .when(F.col(a_ref) != F.col(b_ref), F.lit("inequality"))\
                     .otherwise(F.lit("ineq for other reason")))

    # Display all the inequal records.
    sdf_ineq = sdf_comp_1\
        .select(*[a_join, b_join, a_ref, b_ref, "comparison"])\
        .filter(F.col("comparison") != "equality")
    sdf_ineq.display()

    ls_vals = sdf_ineq\
        .select(*[a_join])\
        .rdd\
        .flatMap(lambda x: x)\
        .collect()

    # Get a distribution on the "comparison" column.
    count_distribution(sdf_comp_1, "comparison")
    return ls_vals


def count_distribution(sdf_base, col_check, fancy=False):
    """
    Shows what the unique elements are for a certain column.
    Also shows the relative contribution of each element in raw counts and percentages.
    :param sdf_base: The base dataframe on which we want to do some inspections.
    :param col_check:  The column whose distribution we want to investigate.
    :param fancy:  Whether to display, or show the DataFrame.
    :return:  Returns nothing.  Prints/Displays statistics on the screen in table format.
    """
    n_len = sdf_base.count()
    sdf_base_1 = sdf_base\
        .withColumn("idx", F.monotonically_increasing_id())
    sdf_return = sdf_base_1\
        .groupBy(F.col(col_check))\
        .agg(F.count(F.col("idx")).alias("n_entries"),
                    F.format_number(F.count(F.col("idx")) / n_len * 100.0, 5).alias("perc_entries"))\
        .orderBy(F.col("n_entries").desc())
    if fancy:
        sdf_return.display(truncate=False)
    else:
        sdf_return.show(truncate=False)
    return None


def distinct_count(sdf_base, col_name):
    """
    Calculate the number and percentage of unique entries in `col_name`.  Returned as a tuple.
    :param sdf_base: The base dataframe.
    :param col_name: The column for which we want to determine the distinct number of entries.
    :return: Return the raw number AND percentage of unique entries contained within the column.
    """
    n_len = sdf_base.count()
    val_raw = sdf_base.select(F.countDistinct(col_name)).collect()[0][0]
    val_pct = val_raw / n_len * 100.0
    dup_raw = n_len - val_raw
    dup_pct = dup_raw / n_len * 100
    print("unq_raw = {:.0f} | unq_pct = {:.6f}%.".format(val_raw, val_pct))
    print("dup_raw = {:.0f} | dup_pct = {:.6f}%.".format(dup_raw, dup_pct))
    return None


def distinct_row_count(sdf_base):
    """
    Calculate the number and percentage of unique observations.
    :param sdf_base: The base dataframe.
    :return: Return the raw number AND percentage of unique observations and duplicates.
    """
    n_len = sdf_base.count()

    cnt_unq = sdf_base.distinct().count()
    cnt_dup = n_len - cnt_unq
    pct_unq = cnt_unq / n_len * 100.0
    pct_dup = 100 - pct_unq
    print(f"Unique, and duplicate raw counts are: {cnt_unq:.0f}, {cnt_dup:.0f}.")
    print(f"Unique, and duplicate percentages are: {pct_unq:.6f}%, {pct_dup:.6f}%.")
    return None


def distinct_stats(sdf_base, *args):
    """
    Show both raw counts AND percentages of the distinct number of elements per column.
    :param sdf_base: The base PySpark DataFrame for which we want to calculate 'distinct' statistics.
    :param args: The column names for which we want to calculate 'distinct' statistics.
    :return: Returns nothing.  Displays tabular results on the screen for the user.
    """
    n_len = sdf_base.count()
    sdf_return = sdf_base.select(*(F.countDistinct(c).alias(c + "_CNT") for c in tuple(args)))
    sdf_return.show()
    sdf_return = sdf_base.select(*(F.format_number(F.countDistinct(c) / n_len * 100.0, 6)
                                 .alias(c + "_PCT") for c in tuple(args)))
    sdf_return.show()
    return None


def equal_comp(sdf_base, col_a_name, col_b_name, on_col_name, id_col_names):
    sdf_comp = sdf_base\
        .select(*[id_col_names[0], id_col_names[1], on_col_name, col_a_name, col_b_name])
    sdf_comp_1 = sdf_comp\
        .withColumn("comparison",
                    F.when((F.col(col_a_name).isNull() & F.col(col_b_name).isNull()), "equality")
                     .when(F.col(col_a_name).isNull(), "left value is NULL")
                     .when(F.col(col_b_name).isNull(), "right value is NULL")
                     .when(F.col(col_a_name) == F.col(col_b_name), "equality")
                     .when(F.col(col_a_name) != F.col(col_b_name), "no equality")
                    )
    sdf_sample = sdf_comp_1\
        .filter(sdf_comp_1.comparison == "no equality")
    sdf_sample.display()
    count_distribution(sdf_comp_1, "comparison")
    return None





def extract_date_tag(val, override=False, **dte_data):
    """
    Function that returns a MMMYY tag, for example JAN21.
    :param val: An integer that is either -1, 0, 1.
    -1 will give a tag for the previous month.
    0 will give a tag for the current month.
    1 will give a tag for the following month.
    :param override: A Boolean value.  If True, then ignore `val` and construct a date tag based on dte_data.
    :param dte_data: A dictionary that containing 2 key-val pairs.  {"year": y_val, "month": m_val}.
    Both y_val and m_val should be integers.
    :return: A MMMYY tag that is of type string.  For example "JAN21" for the month January in the year 2021.
    """
    def get_string_version(dte):
        return dte.strftime("%b%y").upper()
    x = date.today()
    if override:
        year = dte_data["year"]
        month = dte_data["month"]
        day = 15
        my_date = date(year=year, month=month, day=day)
        my_date = get_string_version(my_date)
        return my_date
    if val == -1:  # Get the tag associated with the previous month.
        w = x - relativedelta(months=1)
        w = get_string_version(w)
        return w
    elif val == 0:  # Get the tag associated with the current month.
        x = get_string_version(x)
        return x
    elif val == 1:  # Get the tag associated with the next month.
        y = x + relativedelta(months=1)
        y = get_string_version(y)
        return y
    else:
        return None


def get_extrema(sdf_base, colname):
    x_min = sdf_base\
        .groupby()\
        .agg(F.min(colname))\
        .first()[0]
    x_max = sdf_base\
        .groupby()\
        .agg(F.max(colname))\
        .first()[0]
    print(f"Minimum value of {colname} is:    {x_min}.")
    print(f"Maximum value of {colname} is:    {x_max}.")


def isolate(sdf_base, pkey_name, pkey_vals, *cols):
    """
    A very handy function that can be used for QA purposes and troubleshooting bugs.
    :param sdf_base:  The original spark dataframe on which you want to perform filtering on.
    :param pkey_name:  The name of the field (in string format) that you want to filter the dataframe by.
    :param pkey_vals:  A list of value(s) that you want to retain in your filtered dataframe.
    :param cols: All the field/column names that you want to be displayed in the filtered DataFrame.
    :return:  Nothing is returned by this function.  However, it displays the filtered dataframe in tabular format.
    """
    ls = [pkey_name] + list(cols)
    sdf_iso = sdf_base \
        .filter(F.col(pkey_name).isin(pkey_vals))\
        .select(*ls) \
        .orderBy(pkey_name)

    sdf_iso.display()


def null_percs_entire(sdf_base, fancy=False):
    """
    Show the percentage of NULL values for each column in the sdf_base Spark DataFrame.
    :param sdf_base: The base Spark dataframe from which we want to calculate NULL statistics.
    :param fancy:  Whether to .display() or .show() the result.
    :return: Returns nothing.  Displays results on screen in tabular format.
    """
    n_len = sdf_base.count()
    sdf_return = sdf_base\
        .select(*(F.format_number(F.sum(F.col(c).isNull().cast("int")) / n_len * 100.0, 5)
                .alias(c + "_PERC") for c in list(sdf_base.columns)))
    if fancy:
        sdf_return.display()
    else:
        sdf_return.show()
    return None


def pct_dev(val_old, val_new):
    """
    Calculates the percentage deviation of `val_new` away from `val_old`.
    The calculation is performed via [(val_new - val_old) / val_old * 100].
    In case `val_old` is 0, then 99999 is returned to avoid DivisionByZero error.
    :param val_old: The previous value in the time series.  A Real Number.
    :param val_new:  The latest value in the time series.  A Real Number.
    :return:  The percentage deviation normalised to percentage format.
    The returned value is therefore not in ratio format.  A Real Number.
    """
    numerator = (val_new - val_old) * 100.0
    if val_old == 0:  # To prevent DivisionByZero Error.
        return 99999
    else:
        return numerator / val_old


def show_null_stats(sdf_base, *args):
    """
    Give the raw count AND percentage of NULL values for the columns specified in *args.
    :param sdf_base: Specifies the base spark dataframe.
    :param args: Specifies the column names for which we want to calculate NULL statistics.
    :return: Does not return anything.  Prints out results in tabular format.
    """
    n_len = sdf_base.count()
    sdf_return = sdf_base\
        .select(*(F.sum(F.col(c).isNull().cast("int"))
                .alias(c + "_N_NULL") for c in tuple(args)))
    for c in tuple(args):
        sdf_return = sdf_return\
            .withColumn(c + "_PERC", F.format_number(F.col(c + "_N_NULL") / n_len * 100.0, 5))
    sdf_return.show()
    return None
