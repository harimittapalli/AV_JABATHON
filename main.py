from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.functions import col, when, to_date, from_unixtime
from pyspark.sql import Window
import pyspark as spark
import datetime
import logging

from etl_utils import extractor, loader, transformer


class FeatureEtractor():

    def __init__(self,
                 process_date,
                 spark_appName,
                 user_file,
                 visitorlog_file,
                 *args,
                 **kwargs):

        self.process_date = process_date
        self.spark_appName = spark_appName or "AV_JABATHON"
        self.user_file = user_file
        self.visitorlog_file = visitorlog_file
        logging.basicConfig(level=logging.INFO)

    def createSparkSession(self, app_name="AV_Hackathon", *args, **kwargs):
        return SparkSession.builder.master('local[*]').appName(app_name).getOrCreate()

    def closeSparkSession(self, spark_session, *args, **kwargs):
        """
        :param spark_session: close spark session
        :return:
        """
        spark_session.stop()

    def extract_process_dates(self, dateformat="%Y-%m-%d"):
        pdate = datetime.datetime.strptime(self.process_date, dateformat).date()

        self.visitorlog_edate = (pdate - datetime.timedelta(1)).strftime(dateformat)
        self.visitorlog_sdate = (pdate - datetime.timedelta(21)).strftime(dateformat)

        self.last_7_days_sdate = (pdate - datetime.timedelta(7)).strftime(dateformat)
        self.last_7_days_edate = (pdate - datetime.timedelta(1)).strftime(dateformat)
        self.last_15_days_sdate = (pdate - datetime.timedelta(15)).strftime(dateformat)
        self.last_15_days_edate = (pdate - datetime.timedelta(1)).strftime(dateformat)

    def process(self):

        spark = self.createSparkSession(app_name=self.spark_appName)

        user_df = extractor.read_csv(spark, self.user_file, header=True)
        visitorlog_df = extractor.read_csv(spark, self.visitorlog_file, header=True)

        visitorlog_df = transformer.transform_to_date(visitorlog_df, column="VisitDateTime", new_column="visitDate")
        visitorlog_df = transformer.transform_to_timestamp(visitorlog_df, column="VisitDateTime",
                                                           new_column="visitTime")

        # visitorlog_df = transformer.impute_visitDateTimeColumn(visitorlog_df)
        # visitorlog_df = transformer.convert_to_upper_case(visitorlog_df,
        #                                                   columns=['ProductID', 'Activity', 'OS', 'UserID'])
        # print(visitorlog_df.rdd.getNumPartitions())
        # visitorlog_df = transformer.impute_productid(visitorlog_df)
        # visitorlog_df = visitorlog_df.coalesce(8)
        # print(visitorlog_df.rdd.getNumPartitions())
        # visitorlog_df = transformer.impute_activity(visitorlog_df)
        # visitorlog_df = visitorlog_df.coalesce(8)
        # print(visitorlog_df.rdd.getNumPartitions())

        logging.info("extracting processing dates")
        self.extract_process_dates()

        logging.info(f'filtering records where visitDate between "{self.visitorlog_sdate}", "{self.visitorlog_edate}"')
        filtered_df = visitorlog_df.filter(f'visitDate between "{self.visitorlog_sdate}" and "{self.visitorlog_edate}"')

        logging.info("Joining user_df with filtered_df on UserID")
        out_df = user_df.join(filtered_df, on="UserID", how="left")

        out_df1 = transformer.transformations_step1(spark, out_df, self.last_7_days_sdate,
                                                    self.last_15_days_sdate)

        top_product, top_os, recent_product = transformer.transformations_step2(out_df, self.last_7_days_sdate)

        out_df2 = user_df.join(top_product, how='left', on="UserID")\
            .join(top_os, how='left', on="UserID")\
            .join(recent_product, how='left', on="UserID")

        out_df2 = transformer.get_userVintage(out_df2, process_date=process_date)

        out_df2 = transformer.fill_nullProducts_with_101(out_df2, columns={"Recently_Viewed_Product": "Product101",
                                                                           "Most_Viewed_product_15_Days": "Product101"})

        out_df2 = transformer.drop_columns(out_df2, columns=['Signup Date', 'User Segment'])
        final_df = out_df1.join(out_df2, how="inner", on="UserID").orderBy("UserID")

        # user_df.show()
        # out_df1.show()
        # final_df.show()

        logging.info("Writing output to CSV")
        loader.write_to_csv(final_df, "output", num_partitions=1, header=True, mode="overwrite")

        featue_extractor.closeSparkSession(spark)


if __name__ == "__main__":

    process_date = "2018-05-28"

    featue_extractor = FeatureEtractor(process_date=process_date, spark_appName="AV_JABATHON",
                                       user_file='data/userTable.csv', visitorlog_file='data/VisitorLogsData.csv')

    featue_extractor.process()