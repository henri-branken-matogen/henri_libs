from datetime import date
from dateutil.relativedelta import relativedelta


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
