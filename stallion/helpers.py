from datetime import date
from dateutil.relativedelta import relativedelta


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


def gen_stamp_suffixes(ccyymm_01, steps):
    """
    Generate and return a list of 36 ccyymm stamps.
    The first stamp in the list is the most recent, and corresponds to month 01.
    The last stamp in the list is the most historic, and corresponds to month 36.
    :param ccyymm_01: The ccyymm stamp, in string format, that is associated with month 01.
    :param steps: The number of stamps we must generate.  It corresponds to the length of the returned list.
    steps must be in integer format.
    :return:  A list of 36 ccyymm stamps that are in string format.
    E.g.: ["202101", "202012", ..., "201803", "201802"].
    """
    yyyy = int(ccyymm_01[:4])
    mm = int(ccyymm_01[4:])
    dte_01 = date(year=yyyy, month=mm, day=1)
    dte_00 = dte_01 + relativedelta(months=1)  # 1 month into the future with respect to `dte_01`.
    lookup_dict = dict()
    for n_months in range(1, steps + 1):
        dte = dte_00 - relativedelta(months=n_months)
        stamp = dte.strftime("%Y%m")
        lookup_dict[n_months] = stamp
    return lookup_dict
