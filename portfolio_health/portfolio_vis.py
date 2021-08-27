import numpy as np
import pandas as pd
import pyspark.sql.types as t
import pyspark.sql.functions as F
import portfolio_health.spark_session as ss
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick


def compute_sums(df_input):
    """
    A function that computes totals for the number of entities that have (a) cured, (b) milled,
    (c) worsened for a specific bucket.  The additional column names are basically ["cure", "mill", "worsen"].
    :param df_input:  Input DataFrame showing the transitions between different states.
    These transitions must be integer numbers with no normalisation applied to them.
    :return: An updated dataframe with additional columns for `cure`, `mill` and `worsen` totals.
    """
    canon_buckets = list(df_input.iloc[:, 0])  # I need to iterate over this.

    placeholder = []
    for bucket_i in canon_buckets:
        bucket_i_idx = df_input.index[df_input.iloc[:, 0] == bucket_i][0]  # This is a string

        cure_buckets = [x for x in canon_buckets if int(x) < int(bucket_i)]
        if not cure_buckets:
            cure_sum = 0
        else:
            cure_sum = df_input.loc[bucket_i_idx, cure_buckets].sum()

        worsen_buckets = [x for x in canon_buckets if int(x) > int(bucket_i)]
        if not worsen_buckets:
            worsen_sum = 0
        else:
            worsen_sum = df_input.loc[bucket_i_idx, worsen_buckets].sum()

        mill_sum = df_input.loc[bucket_i_idx, bucket_i].sum()
        placeholder.append([cure_sum, mill_sum, worsen_sum])

    arr = np.array(placeholder)
    df = pd.DataFrame(data=arr, columns=["cure", "mill", "worsen"])
    return pd.concat([df_input, df], axis=1)


def construct_transition_matrix(n_months_lookback, delta_min, delta_max, sdf_orig, outside_str, ls_buckets):
    """
    Create a transmition matrix for data stationed at two different points in time.
    A square matrix should be returned.
    :param n_months_lookback: Number of lookback months.  Basically, the number of monthly files you have
    available.  Of type integer.
    :param delta_min: The minimum change in Month duration.  Say the progenitor month is "Date + 1 Months",
    then `delta_min` should be equal to 1.
    This parameter should be of type Integer.
    :param delta_max: The maximum change in Month duration.  Say the outcome period is "Date + 3 Months",
    then `delta_max` should be equal to 3.
    This parameter should be of type Integer.
    :param sdf_orig:  The DataFrame containing all the monthly data of `BEH_Account_<yyyymm>`.
    I.e., 1 column per month of data.
    This parameter should be a pandas DataFrame.
    :return: A crosstabulation matrix that has square dimensions.
    """
    x_scalar = 1 - delta_max
    if delta_min == 0:
        progen_col_name = f"Date"
    else:
        progen_col_name = f"Date + {delta_min}M"
    outcome_col_name = f"Date + {delta_max}M"

    # Initialise an empty Transition DataFrame storing two columns:
    # [1] The Progenitor Month | [2] The Outcome Month.
    # Later on, we will perform a crosstab on this DataFrame to obtain a square matrix as output.
    myschema = t.StructType([
        t.StructField(progen_col_name, t.StringType(), True),
        t.StructField(outcome_col_name, t.StringType(), True)
    ])
    # Initialise a blank "Transition Matrix" DataFrame.
    sdf_tr = spark\
        .createDataFrame([], myschema)

    # ------------------------------------------------------------------------------------------------------------------
    # Pair all the ["Current Date", "Current Date" + 1M] yyyymm couples together in following for-loop.
    # Concatenate the pairs into a skinny DF.
    month_pairs = []
    for i in range(1, n_months_lookback + x_scalar):
        c_left = sdf_orig.columns[i + delta_min]  # extract the LHS column name.
        c_right = sdf_orig.columns[i + delta_max]  # extract the RHS column name.
        month_pairs.append((c_left, c_right))  # see what month combination was used.

        sdf_chunk = sdf_orig \
            .select(c_left, c_right)  # Create Chunk DataFrame from `sdf_orig`.

        cs_old = sdf_chunk.columns
        cs_new = sdf_tr.columns
        cs_pairs = list(zip(cs_old, cs_new))

        for c_o, c_n in cs_pairs:
            sdf_chunk = sdf_chunk \
                .withColumnRenamed(c_o, c_n)

        # We cannot infer transition(s) from Null values.  Rows containing these must be dropped.
        if outside_str is None:
            sdf_chunk = sdf_chunk \
                .dropna()
        else:
            sdf_chunk = sdf_chunk \
                .dropna() \
                .filter(F.col(progen_col_name) != outside_str)

        sdf_tr = sdf_tr \
            .union(sdf_chunk)

    # ------------------------------------------------------------------------------------------------------------------
    # Create crosstabulation via PySpark.

    sdf_cross = sdf_tr \
        .crosstab(progen_col_name, outcome_col_name)

    sdf_cross.show()

    # Convert crosstab to Pandas DataFrame.
    pdf_cross = sdf_cross.toPandas()

    # Arrange the 1st column values to match the order of `ls_outside` bucket
    if outside_str is None:
        ls_canon_buckets = [x for x in ls_buckets]
    else:
        ls_canon_buckets = [x for x in ls_buckets if x.lower() != outside_str.lower()]
    index_order = []
    for x in ls_canon_buckets:
        res = (pdf_cross.loc[:, pdf_cross.columns[0]] == x)
        index_order.append(pdf_cross[res].index[0])
    # Actual rearrangement of rows take place.
    pdf_cross = pdf_cross.reindex(index_order)
    # Reset the index to make it monotonically increasing: [0, 1, 2, ..., etc.]
    pdf_cross = pdf_cross.reset_index(drop=True, inplace=False)

    # Set the Correct Column Order.
    new_col_order = [pdf_cross.columns[0]] + list(sdf_cross.columns[1:])
    pdf_cross = pdf_cross[new_col_order]

    # Calculate the "volume" and "logvol" for each progenitor bucket.
    pdf_cross["volume"] = pdf_cross[pdf_cross.columns[1:]].sum(axis=1)
    pdf_cross["logvol"] = np.log10(pdf_cross["volume"])

    # Rename the first column.
    pdf_cross.rename(columns={
        pdf_cross.columns[0]: "_"
    }, inplace=True)

    return month_pairs, pdf_cross


def convert_to_percs(df_input, sum_name="volume", logvol_name="logvol",
                     cure_name="cure", mill_name="mill", worsen_name="worsen", exit_name="outside"):
    """
    Takes in a pandas dataframe containing the raw numbers for the transitions between different buckets as input.
    These figures are then converted to percentages (rates) for better interpretability.
    The only exception is that we retain the total Volume of Accounts for each progenitor bucket.
    :param df_input:  The input Pandas DataFrame containing the raw figures that we want to convert to percentages.
    :param exit_name:  The name of the "exit" bucket serving as a catchall for buckets not belonging to the canonical
     buckets.  Of type String.
    :param sum_name:  The name of the column containing the total volume of accounts for each progenitor bucket.
    Of type String.
    :param logvol_name:  The name of the column containing LOGARITHM(volume of accounts) for each progenitor bucket.
    Of type String.
    :param cure_name:  The column name containing the `cure` volumes.  Of type String.
    :param mill_name:  The column name containing the `mill` volumes.  Of type String.
    :param worsen_name:  The column nam containing the `worsen` volumes.  Of type String.
    :param exit_name:  The name of the "exit" bucket serving as a catchall for buckets not belonging to the canonical
     buckets. In some cases it can be interpreted as the number of accounts that left the portfolio.  Of type String.
    :return:  Return a normalised dataframe containing percentages for better readability.
    """
    # Initialise an empty NumPy Array containing zeros.
    # In for-loop iterations it will be updated with the desire results.
    arr = np.zeros(df_input.shape)

    # Get the progenitor buckets
    ls_buckets = list(df_input.iloc[:, 0])

    ls_col_buckets = list(df_input.columns[1:])
    ls_col_buckets = [x for x in ls_col_buckets if x not in (sum_name, logvol_name)]

    # Stipulate the "to_<x>_rate" endpoints.
    to_names = ["to_" + str(x) + "_rate" for x in ls_buckets]

    # Stipulate all the column names.
    if exit_name is None:
        col_names = ["_", *to_names, sum_name, logvol_name, "cure_rate", "mill_rate", "worsen_rate"]
    else:
        col_names = ["_", *to_names, "to_exit_rate", sum_name, logvol_name, "cure_rate", "mill_rate", "worsen_rate"]

    # Identify the columns that need to be rounded to fewer decimals (to reduce the noise).
    rounded_cols = [x for x in col_names if "_rate" in x]

    for idx in list(df_input.index):
        iter_row = df_input.loc[idx, :]
        denom = iter_row[sum_name]  # The total Sum of accounts for the progenitor bucket in current iteration.
        logvol = iter_row[logvol_name]
        bucket_rates = [(iter_row[idx] / denom * 100) for idx in ls_col_buckets]
        cure_rate = iter_row[cure_name] / denom * 100
        mill_rate = iter_row[mill_name] / denom * 100
        worsen_rate = iter_row[worsen_name] / denom * 100
        # Capture all elements of new row into a single list.
        if exit_name is None:
            arr[idx, 1:] = bucket_rates + [denom, logvol, cure_rate, mill_rate, worsen_rate]
        else:
            exit_rate = iter_row[exit_name] / denom * 100
            new_row = bucket_rates + [exit_rate, denom, logvol, cure_rate, mill_rate, worsen_rate]
            # Assign `new_row` to the NumPy Array.
            arr[idx, 1:] = new_row

    # Construct a Pandas DataFrame from the NumPy Array.
    df_return = pd.DataFrame(data=arr, columns=col_names)

    df_return.loc[:, "_"] = ls_buckets

    # We don't need a lot of decimal places in our percentages.
    # To reduce the noise, we only keep one decimal place.
    df_return[rounded_cols] = df_return[rounded_cols].round(decimals=1)
    return df_return


def convert_to_percs_bland(df_input, sum_name="volume",
                           logvol_name="logvol", exit_name="outside"):
    """
    Takes in a pandas dataframe containing the raw numbers for the transitions between different buckets as input.
    These figures are then converted to percentages (rates) for better interpretability.
    The only exception is that we retain the total Volume of Accounts for each progenitor bucket.
    :param df_input:  The input Pandas DataFrame containing the raw figures that we want to convert to percentages.
    :param exit_name:  The name of the "exit" bucket serving as a catchall for buckets not belonging to the canonical
    buckets.  Of type String.
    :param sum_name:  The name of the column containing the total volume of accounts for each progenitor bucket.
    Of type String.
    :param logvol_name:  The name of the column containing LOGARITHM(volume of accounts) for each progenitor bucket.
    :param exit_name:  The name of the "exit" bucket serving as a catchall for buckets not belonging to the canonical
    buckets.  In some cases it can be interpreted as the number of accounts that left the portfolio.  Of type String.
    :return:  Return a normalised dataframe containing percentages for better readability.
    """
    # Initialise an empty NumPy Array containing zeros.
    arr = np.zeros(df_input.shape)
    # In the for-loop iterations it will be updated with the desired results.

    # Get all the progenitor buckets from the first Column.
    ls_buckets = list(df_input.iloc[:, 0])

    ls_col_buckets = list(df_input.columns[1:])
    ls_col_buckets = [x for x in ls_col_buckets if x not in (sum_name, logvol_name)]

    # Stipulate the "to_<x>_rate" endpoints from `ls_buckets`.
    to_names = ["to_" + str(x) + "_rate" for x in ls_col_buckets]

    # Stipulate all the column names.
    if exit_name is None:
        col_names = ["_", *to_names, sum_name, logvol_name]
    else:
        col_names = ["_", *to_names, "to_exit_rate", sum_name, logvol_name]

    # Identify the columns that need to be rounded to fewer decimals (to reduce the noise).
    rounded_cols = [x for x in col_names if "_rate" in x]

    for idx in list(df_input.index):
        iter_row = df_input.loc[idx, :]
        denom = iter_row[sum_name]  # The Total Sum of accounts for the progenitor bucket in the current iteration.
        logvol = iter_row[logvol_name]
        bucket_rates = [(iter_row[elem] / denom * 100) for elem in ls_col_buckets]
        # Capture all the elements of the new row into a single list, and assign to Numpy Array.
        if exit_name is None:
            arr[idx, 1:] = bucket_rates + [denom, logvol]
        else:
            exit_rate = iter_row[exit_name] / denom * 100
            new_row = bucket_rates + [exit_rate, denom, logvol]
            arr[idx, 1:] = new_row

    # Construct a Pandas DataFrame from the Numpy Array:
    df_return = pd.DataFrame(data=arr, columns=col_names)

    df_return.loc[:, "_"] = ls_buckets

    # We don't need a lot of decimal places in our percentages.
    # To reduce the noise, we only keep one decimal place.
    df_return[rounded_cols] = df_return[rounded_cols].round(decimals=1)
    return df_return


def make_heatmap(df_input, delta_min, delta_max, cmap, epsilon, make_mask=False, x_rot=0, y_rot=0):
    """
    A function that visualizes the data input.
    :param df_input:  The output from `construct_transition_matrix`.  It is a crosstabulation,
    and should therefore be a square matrix.
    :param delta_min: The minimum change in Month duration.  Say the progenitor month is "Date + 1 Months",
    then `delta_min` should be equal to 1.
    This parameter should be of type Integer.
    :param delta_max: The maximum change in Month duration.  Say the outcome period is "Date + 3 Months",
    then `delta_max` should be equal to 3.
    This parameter should be of type Integer.
    :param cmap:  Of type string.  Please use a color palette from
    https://seaborn.pydata.org/tutorial/color_palettes.html
    to your liking.
    :param make_mask:  Boolean.
    If True, then mask all the components below the Diagonal of the Square Matrix.
    :param x_rot:  The amount of degrees by which you'd like to rotate the xtick labels.
    :param y_rot:  The amount of degrees by which you'd like to rotate the ytick labels.
    :return: Nothing.  This function should display a figure on the screen.
    """
    df_input = df_input.set_index(df_input.columns[0], inplace=False)
    ls_canon_buckets = [c for c in df_input.columns if c.startswith("to_") and c.endswith("_rate")]
    df_input = df_input[ls_canon_buckets]

    if delta_min == 0:
        ylabel = r"$\bf{" + "Start" + "}$" + ": Date"
    elif delta_min == 1:
        ylabel = r"$\bf{" + "Start" + "}$" + f": Date + {delta_min} Month"
    else:
        ylabel = r"$\bf{" + "Start" + "}$" + f": Date + {delta_min} Months"

    if delta_max == 0:
        xlabel = r"$\bf{" + "End" + "}$" + ": Date"
    elif delta_max == 1:
        xlabel = r"$\bf{" + "End" + "}$" + f": Date + {delta_max} Month"
    else:
        xlabel = r"$\bf{" + "End" + "}$" + f": Date + {delta_max} Months"

    # Visualise the transition matrix.
    fig, ax = plt.subplots(figsize=(9, 7))
    if make_mask:
        mask = np.tril(np.ones_like(df_input.values))
        np.fill_diagonal(mask, 0)
        mask_2 = (df_input < epsilon)
        mask_3 = mask + mask_2
        mask_3 = (mask_3 > 0)
        ax = sns.heatmap(df_input, annot=True, cmap=cmap, linewidths=0.1, linecolor="white", ax=ax, fmt=".1f",
                         cbar_kws={'format': '%.0f%%'}, annot_kws={"fontsize": 16}, mask=mask_3)
    else:
        mask = (df_input.values < epsilon)
        ax = sns.heatmap(df_input, annot=True, cmap=cmap, linewidths=0.1, linecolor="white", ax=ax, fmt=".1f",
                         cbar_kws={'format': '%.0f%%'}, annot_kws={"fontsize": 16}, mask=mask)
    for t in ax.texts: t.set_text(t.get_text() + " %")
    ax.set_facecolor("white")
    ax.xaxis.tick_top()
    ax.xaxis.set_label_position('top')
    ax.set_xlabel(xlabel, fontsize=16, labelpad=30)
    ax.set_ylabel(ylabel, fontsize=16, labelpad=30)
    ax.tick_params(left=False, top=False, axis='both', labelsize=14)
    plt.xticks(rotation=x_rot)
    plt.yticks(rotation=y_rot)
    plt.show()
    return None


def stacked_rates(df_input, x_label, y_label, title, plot_logvol=True):
    """
    :param df_input: The pandas DataFrame containing the rates for different buckets.
    :param x_label:  The x axis label of the final graph.  Of type String.
    :param y_label:  The y axis labl of the final graph.  Of type String.
    :param title:  The title of the final graph.  Of type String.
    """
    def pin_col_name(ls_column_names, substring):
        """
        :param ls_column_names:  A list of column names of the input pandas Dataframe.
        :param substring:  A string for which we search if it has a match in one of ls_column_names.
        :return:  Return the column-name that corresponds with `substring`.
        """
        for c_0 in ls_column_names:
            c_1 = c_0.lower()
            substring = substring.lower()
            if substring in c_1:
                return c_0
        else:
            return None

    col_names = list(df_input.columns)
    # Get the name of the first column in `df_input`.
    first_col = list(df_input.columns)[0]

    # Get the correct names for the different types of rates.
    c_exit_name = pin_col_name(col_names, "exit")
    c_cure_name = pin_col_name(col_names, "cure")
    c_mill_name = pin_col_name(col_names, "mill")
    c_worsen_name = pin_col_name(col_names, "worsen")

    if c_exit_name is None:
        df_nb = df_input[[first_col, c_cure_name, c_mill_name, c_worsen_name]]
        cdict = {
            c_cure_name: "limegreen",
            c_worsen_name: "hotpink",
            c_mill_name: "aqua"
        }
    else:
        df_nb = df_input[[first_col, c_exit_name, c_cure_name, c_mill_name, c_worsen_name]]
        cdict = {
            c_exit_name: "lightgrey",
            c_cure_name: "limegreen",
            c_worsen_name: "hotpink",
            c_mill_name: "aqua"
        }
    ax = df_nb.iloc[:, 1:].plot.bar(stacked=True, figsize=(9, 7), color=cdict,
                                    edgecolor="black")
    for p in ax.patches:
        width, height = p.get_width(), p.get_height()
        if height < 5:
            continue
        x, y = p.get_xy()
        ax.text(x + width / 2,
                y + height / 2,
                f'{height:.0f} %',
                horizontalalignment="center",
                verticalalignment="center")
    ax.yaxis.set_major_formatter(mtick.PercentFormatter())
    _ = plt.setp(ax.xaxis.get_majorticklabels(), rotation=0)
    ax.set_ylabel(y_label)
    ax.set_xlabel(x_label)
    ax.set_title(title)

    if plot_logvol:
        ax2 = df_input.loc[:, "logvol"].plot(secondary_y=True, marker="o", alpha=0.4, color="purple")
        ax2.set_ylabel(r"$\log_{10}({\mathrm{Volume}})$")

    ax.legend(title="Rate Types", bbox_to_anchor=(1.1, 1))
    plt.show()
    return None
