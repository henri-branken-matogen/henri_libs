import pyspark.sql.functions as f
import pyspark.sql.types as t
from pyspark.sql import Window


def Flag_DUP_Applicant(SRT, sdf_inp, DAY=28):
    """
    This is the Python translation of the SAS Code `Flag_DUP_Applicant.sas`.
    """
    if SRT.upper() == "ASCENDING":
        sdf_0 = sdf_inp\
            .repartition(1)\
            .orderBy([f.col("IDKey").asc(),
                      f.col("APP_Date").asc(),
                      f.col("DUP_Application").asc()])

        windowspecIDKEY = Window \
            .partitionBy(f.col("IDKey")) \
            .orderBy([f.col("IDKey").asc(),
                      f.col("APP_Date").asc(),
                      f.col("DUP_Application").asc()])

        # windowspecAPPDATE = Window \
        #     .partitionBy(f.col("IDKey")) \
        #     .orderBy(f.col("IDKey").asc(),
        #              f.col("APP_Date").asc(),
        #              f.col("DUP_Application").asc())

        sdf_1 = sdf_0 \
            .repartition(1) \
            .withColumn("RET_IDKey", f.lag(f.col("IDKey"), 1).over(windowspecIDKEY))\
            .withColumn("RET_Date", f.lag(f.col("APP_Date"), 1).over(windowspecIDKEY))\
            .withColumn("DUP_Applicant", f.when(f.col("IDKey") == f.col("RET_IDKey"), f.lit("Y"))
                                          .otherwise(f.lit(None)))
    else:
        sdf_0 = sdf_inp\
            .repartition(1)\
            .orderBy([f.col("IDKey").asc(),
                      f.col("APP_Date").desc(),
                      f.col("DUP_Application").desc()])

        windowspecIDKEY = Window \
            .partitionBy(f.col("IDKey")) \
            .orderBy([f.col("IDKey").asc(),
                      f.col("APP_Date").desc(),
                      f.col("DUP_Application").desc()])

        # windowspecAPPDATE = Window \
        #     .partitionBy(f.col("IDKey")) \
        #     .orderBy(f.col("IDKey").asc(),
        #              f.col("APP_Date").desc(),
        #              f.col("DUP_Application").desc())

        sdf_1 = sdf_0 \
            .repartition(1) \
            .withColumn("RET_IDKey", f.lag(f.col("IDKey"), 1).over(windowspecIDKEY)) \
            .withColumn("RET_Date", f.lag(f.col("APP_Date"), 1).over(windowspecIDKEY))\
            .withColumn("DUP_Applicant", f.when(f.col("IDKey") == f.col("RET_IDKey"), f.lit("Y"))
                                          .otherwise(f.lit(None)))

    if SRT.upper() == "ASCENDING":
        sdf_2 = sdf_1\
            .withColumn("DUP_DaysBetweenApplications", f.when(f.col("IDKey") == f.col("RET_IDKey"),
                                                              f.datediff(f.col("APP_Date"), f.col("RET_Date")))
                                                        .otherwise(f.lit(None)))
    else:
        sdf_2 = sdf_1 \
            .withColumn("DUP_DaysBetweenApplications", f.when(f.col("IDKey") == f.col("RET_IDKey"),
                                                              f.datediff(f.col("RET_Date"), f.col("APP_Date")))
                                                        .otherwise(f.lit(None)))

    sdf_3 = sdf_2\
        .withColumn("DUP_Application", f.when((f.col("DUP_DaysBetweenApplications") >= f.lit(0)) &
                                              (f.col("DUP_DaysBetweenApplications") <= f.lit(DAY)), f.lit(1))
                                        .otherwise(f.col("DUP_Application")))\
        .drop(*["RET_IDKey", "APP_Date"])

    return sdf_3


def DUP_subroutine(sdf_inp):
    """
    This is the Python translation of STEP 4 in `Input_Applications_DMP.sas`.
    In STEP 4, we sort by the optimal decision services outcome, best risk grade obtained, and highest subscription
    limit within the acceptable period.
    """
    def change_day():
        return lambda x: x.replace(day=1)
    udf_change_day = f.udf(change_day, returnType=t.DateType())

    sdf_0 = sdf_inp\
        .repartition(1)\
        .orderBy([f.col("IDKey").asc(),
                  f.col("DUP_Application").asc(),
                  f.col("Filter_Decision_Outcome_SEQ").asc(),
                  f.col("APP_RiskGrade").asc(),
                  f.col("APP_SubscriptionLimit").desc(),
                  f.col("APP_Date").asc()])

    windowspecIDKEY = Window \
        .partitionBy(f.col("IDKey")) \
        .orderBy([f.col("IDKey").asc(),
                  f.col("DUP_Application").asc(),
                  f.col("Filter_Decision_Outcome_SEQ").asc(),
                  f.col("APP_RiskGrade").asc(),
                  f.col("APP_SubscriptionLimit").desc(),
                  f.col("APP_Date").asc()])

    sdf_1 = sdf_0\
        .withColumn("RET_IDKey", f.lag(f.col("IDKey"), 1).over(windowspecIDKEY))\
        .withColumn("RET_Date", f.lag(f.col("APP_Date"), 1).over(windowspecIDKEY))\
        .withColumn("RET_Application", f.lag(f.col("DUP_Application"), 1).over(windowspecIDKEY))\
        .withColumn("APP_Month_dte", f.to_date(f.col("APP_Month"), "yyyyMMdd"))\
        .withColumn("APP_Month_dte", udf_change_day(f.col("APP_Month_dte")))\
        .withColumn("RET_Month", f.lag(f.col("APP_Month"), 1).over(windowspecIDKEY))\
        .withColumn("RET_Month_dte", f.to_date(f.col("RET_Month"), "yyyyMMdd"))\
        .withColumn("RET_Month_dte", udf_change_day(f.col("RET_Month_dte")))\
        .withColumn("RET_DecisionOutcome", f.lag(f.col("Filter_Decision_Outcome_SEQ"), 1).over(windowspecIDKEY))\
        .withColumn("RET_RiskGrade", f.lag(f.col("APP_RiskGrade"), 1).over(windowspecIDKEY))

    sdf_2 = sdf_1\
        .withColumn("DUP_DaysBetweenApplications", f.when((f.col("IDKey") == f.col("RET_IDKey")) &
                                                          (f.col("RET_Application") == 1),
                                                          f.datediff(f.col("APP_Date"), f.col("RET_Date")))
                                                    .otherwise(f.lit(None)))\
        .withColumn("DUP_Applicant", f.when((f.col("IDKey") == f.col("RET_IDKey")) &
                                            (f.col("RET_Application") == 1),
                                            f.lit("Z"))
                                      .otherwise(f.col("DUP_Applicant")))\
        .withColumn("DUP_CalendarMonthsSkipped", f.when((f.col("IDKey") == f.col("RET_IDKey")) &
                                                        (f.col("RET_Application") == 1) &
                                                        (f.col("APP_Month") != f.col("RET_Month")),
                                                        f.months_between(f.col("APP_Month_dte"), f.col("RET_Month_dte")))
                                                  .otherwise(f.lit(None)))\
        .withColumn("DUP_DecisionOutcome", f.when((f.col("IDKey") == f.col("RET_IDKey")) &
                                                  (f.col("RET_Application") == 1) &
                                                  f.col("Filter_Decision_Outcome_SEQ").isin([1, 2, 3]) &
                                                  f.col("RET_DecisionOutcome").isin([1, 2, 3]) &
                                                  (f.col("Filter_Decision_Outcome_SEQ") != f.col("RET_DecisionOutcome")),
                                                  f.col("RET_DecisionOutcome") - f.col("Filter_Decision_Outcome_SEQ"))
                                            .otherwise(f.lit(None)))\
        .withColumn("DUP_RiskGrade", f.when((f.col("IDKey") == f.col("RET_IDKey")) &
                                            (f.col("RET_Application") == 1) &
                                            f.col("APP_RiskGrade").isNotNull() &
                                            f.col("RET_RiskGrade").isNotNull() &
                                            (f.col("APP_RiskGrade") != f.col("RET_RiskGrade")),
                                            f.col("RET_RiskGrade").astype(int) - f.col("APP_RiskGrade").astype(int))
                                      .otherwise(f.lit(None)))\
        .drop(*["RET_IDKey",
                "RET_Date",
                "RET_Application",
                "RET_Month",
                "RET_DecisionOutcome",
                "RET_RiskGrade"])

    return sdf_2
