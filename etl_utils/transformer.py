import pyspark.sql.functions as f
from pyspark.sql.window import Window
from pyspark.sql.types import StringType
import sys


# @f.udf(StringType())
# def impute_udf(spark_userid, pandas_df):
#     return pandas_df.loc[pandas_df["UserID"] == spark_userid].value[0]

def transform_to_date(df, column, new_column=None, *args, **kwargs):

    df = df.withColumn('datetemp',
                  f.when(f.to_date(f.col(column)).isNotNull(), f.to_date(f.col(column)))
                  .when(f.from_unixtime(f.col(column)).isNotNull(),
                        f.to_date(f.from_unixtime(f.col(column) / 1000000000))))

    if new_column:
        df = df.withColumnRenamed('datetemp', new_column)
    else:
        df = df.drop(column).withColumnRenamed('datetemp', column)

    return df


def transform_to_timestamp(df, column, new_column=None, *args, **kwargs):

    df = df.withColumn('datetimetemp',
                       f.when(f.to_timestamp(f.col(column)).isNotNull(), f.to_timestamp(f.col(column)))
                       .when(f.from_unixtime(f.col(column)).isNotNull(),
                             f.to_timestamp(f.from_unixtime(f.col(column) / 1000000000))))

    if new_column:
        df = df.withColumnRenamed('datetimetemp', new_column)
    else:
        df = df.drop(column).withColumnRenamed('datetimetemp', column)

    return df


def get_window(partitionBy_cols=None, orderBy_cols=None, rowsbetween_min=None, rowsbetween_max=None, *args, **kwargs):

    if not partitionBy_cols:
        raise Exception("Partion by columns is required to create window")
    if orderBy_cols:
        if rowsbetween_max and rowsbetween_min:
            window = Window.partitionBy(partitionBy_cols).orderBy(orderBy_cols).rowsBetween(rowsbetween_min,
                                                                                          rowsbetween_max)
        elif rowsbetween_max:
            window = Window.partitionBy(partitionBy_cols).orderBy(orderBy_cols).rowsBetween(0, rowsbetween_max)
        else:
            window = Window.partitionBy(partitionBy_cols).orderBy(orderBy_cols).rowsBetween(rowsbetween_min, 0)

    elif rowsbetween_max or rowsbetween_min:
        if rowsbetween_max and rowsbetween_min:
            window = Window.partitionBy(partitionBy_cols).rowsBetween(rowsbetween_min, rowsbetween_max)
        elif rowsbetween_max:
            window = Window.partitionBy(partitionBy_cols).rowsBetween(0, rowsbetween_max)
        else:
            window = Window.partitionBy(partitionBy_cols).rowsBetween(rowsbetween_min, 0)

    else:
        window = Window.partitionBy(partitionBy_cols)

    return window


def convert_to_upper_case(df, columns):

    for column in columns:
        df = df.withColumn(column, f.upper(f.col(column)))

    return df


def transformations_step1(spark, df, before_7_days_sdate, before_15_days_sdate):

    df.registerTempTable('df_sql')
    out_df = spark.sql(f"""
                        SELECT UserID, 
                        count(distinct visitDate) filter (where visitDate > "{before_7_days_sdate}") as No_of_days_Visited_7_Days,
                        count(distinct ProductID) filter (where visitDate > "{before_15_days_sdate}") AS No_Of_Products_Viewed_15_Days,
                        count(*) filter (where Activity = 'PAGELOAD' and visitDate > "{before_7_days_sdate}") as Pageloads_last_7_days,
                        count(*) filter (where Activity = 'CLICK' and visitDate > "{before_7_days_sdate}") as Clicks_last_7_days
                        FROM df_sql  GROUP BY UserID
                    """)
    return out_df


def transformations_step2(df, before_7_days_sdate):
    w3 = get_window(partitionBy_cols=['UserID'])
    w99 = get_window(partitionBy_cols=['UserID', 'ProductID'], orderBy_cols=['visitTime'],
                     rowsbetween_min=-sys.maxsize, rowsbetween_max=sys.maxsize)

    top_product = df.filter(f'visitDate > "{before_7_days_sdate}" and Activity = "PAGELOAD"')\
        .withColumn("PCount", f.count("*").over(w99))\
        .withColumn("rn", f.row_number().over(w3.orderBy(f.desc("PCount"), "visitTime")))\
        .filter("rn=1").drop("rn").select("UserID", "ProductID")\
        .withColumnRenamed("ProductID", "Most_Viewed_product_15_Days")

    top_os = df.withColumn("oscount", f.count("*").over(w3))\
        .withColumn("rn", f.row_number().over(w3.orderBy(f.desc("oscount"))))\
        .filter("rn=1").drop("rn").select("OS", "UserID").withColumnRenamed("OS", "Most_Active_OS")

    recent_product = df.withColumn("Recently_Viewed_Product",
                               f.when(f.col('Activity') == 'PAGELOAD', f.first("ProductID")
                                      .over(w3.orderBy(f.desc("visitTime")))))\
        .groupBy('UserID').agg(f.max('Recently_Viewed_Product').alias('Recently_Viewed_Product'))

    return top_product, top_os, recent_product


def get_userVintage(df, process_date):

    df = df.withColumn('User_Vintage', f.datediff(f.lit(process_date), f.col("Signup Date")))

    return df


def drop_columns(df, columns):
    for column in columns:
        df = df.drop(column)

    return df


def fill_nullProducts_with_101(df, columns={}, *args, **kwargs):
    for column, filler in columns.items():
        df = df.withColumn(column, f.when(f.col(column).isNull(), filler).otherwise(f.col(column)))

    return df


def impute_visitDateTimeColumn(df, *args, **kwargs):
    w = get_window(partitionBy_cols=['webClientID', 'UserID'], orderBy_cols=['visitTime'], rowsbetween_min=-15)
    df = df.withColumn('templcol1', f.when(f.col('visitDate').isNull(), f.max(df['visitDate']).over(w))
                       .otherwise(f.col('visitDate'))).drop('visitDate').withColumnRenamed('templcol1', 'visitDate')

    df = df.withColumn('tempcol2', f.when(f.col('visitTime').isNull(), f.max(df['visitTime']).over(w))
                       .otherwise(f.col('visitTime'))).drop('visitTime').withColumnRenamed('tempcol2', 'visitTime')

    return df


def impute_productid(df, *args, **kwargs):
    w = get_window(partitionBy_cols=['UserID', 'ProductID'])

    product_df = df.filter("ProductID is not null").withColumn("product_count", f.count("*").over(w.orderBy('visitTime')))\
        .withColumn("rownum", f.row_number().over(w.orderBy(f.desc(f.col('product_count')))))\
        .drop("rownum").select("UserID", "ProductID").withColumnRenamed("ProductID", "UpdatedProductID")

    new_df = df.join(product_df, on="UserID", how="left")

    new_df = new_df.withColumn("FilledProductID", f.when(f.col("ProductID").isNull(), f.col("UpdatedProductID"))
                               .otherwise(f.col("ProductID"))).drop("ProductID")\
        .withColumnRenamed("FilledProductID", "ProductID").drop("UpdatedProductID")

    return new_df


def impute_activity(df, *args, **kwargs):
    w = get_window(partitionBy_cols=['UserID', 'Activity'])
    activity_df = df.filter("Activity is not null").withColumn("activity_count", f.count("*").over(w.orderBy('visitTime')))\
        .withColumn("rownum", f.row_number().over(w.orderBy(f.desc(f.col('activity_count')))))\
        .drop("rownum").select("UserID", "Activity").withColumnRenamed("Activity", "UpdatedActivity")

    new_df = df.join(activity_df, on="UserID", how="left")

    new_df = new_df.withColumn("FilledActivity", f.when(f.col("Activity").isNull(), f.col("UpdatedActivity"))
                               .otherwise(f.col("Activity"))).drop("Activity")\
        .withColumnRenamed("FilledActivity", "Activity").drop("UpdatedActivity")

    return new_df

