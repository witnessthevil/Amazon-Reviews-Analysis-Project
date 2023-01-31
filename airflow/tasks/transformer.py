import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,from_unixtime,udf,substring
from pyspark.sql.types import *
import os
from Logger import Logger
import pyspark.sql.dataframe
import psutil
from multiprocessing import Process


logger = Logger().get_logger(__name__)

class Transformer:
    def __init__(self,category):
        self.spark = SparkSession.builder.appName('realityisdead').getOrCreate()
        self.category = category
        
        
    def data_cleasing(self):

        try:

            product_df:pyspark.sql.dataframe.DataFrame = self.spark.read.option('inferSchema','true')\
                                .option('header','true')\
                                .json(f'/Users/danie/new_project_pipeline_plus_recommanded_system/product_{self.category}.json')

            metadata_df:pyspark.sql.dataframe.DataFrame = self.spark.read.option('inferSchema','true')\
                                .option('header','true')\
                                .json(f'/Users/danie/new_project_pipeline_plus_recommanded_system/metadata_{self.category}.json')

            no_dupli_df:pyspark.sql.dataframe.DataFrame = product_df.withColumn('unixReviewTime',from_unixtime(col("unixReviewTime"),"MM-dd-yyyy")).drop('reviewTime')

            no_dupli_df.createOrReplaceTempView('product_table')

            metadata_df.createOrReplaceTempView('metadata_df')

            sql = """
                select 
                case when title is not null then title
                else P.asin
                end as product_name,
                description,reviewerName,summary,reviewText,overall,unixReviewTime 
                from product_Table p 
                left join metadata_df m
                on p.asin = m.asin   
                """

            
            real_df = self.spark.sql(sql).dropDuplicates()
            real_df.persist()

        except:
            with Exception as e:
                logger.error(e)

        return real_df

    def import_file(self):

        logger.info("now transforming the combined df to json")
        try:
            df = self.data_cleasing()
            df.write.format("json").save("/Users/danie/new_project_pipeline_plus_recommanded_system/final_df")
            logger.info("successfully transform combined df to json")
        except:
            with Exception as e:
                logger.error(e)

    def find_most_rated_goods(self):
        
        logger.info("now find the most rated goods in all product")
        
        try: 

            dataframe = self.data_cleasing()

            rating_df = dataframe.select('product_name','overall')

            rating_groupby_df = rating_df.groupBy('product_name').avg('overall').orderBy(col('avg(overall)').desc()).withColumnRenamed('avg(overall)','avg_rating')

            rating_groupby_df.write.format("json").save('/Users/danie/new_project_pipeline_plus_recommanded_system/final_rating')

            logger.info("successfully transform most rated good df to json")
        
        except:
            with Exception as e:
                logger.error(e)
    
    def amount_of_review_in_each_year(self):

        logger.info("now finding the amount of review on each year")

        try:

            dataframe = self.data_cleasing()

            time_df = dataframe.select(substring(col('unixReviewTime'),-4,4).alias('year'))

            time_groupBy_df = time_df.groupBy('year').count().orderBy(col('year').asc()).withColumnRenamed('count(year)','avg_review')

            time_groupBy_df.write.format("json").save('/Users/danie/new_project_pipeline_plus_recommanded_system/final_time')

            logger.info("successfully transform review each year df to json")
        
        except:

            with Exception as e:

                logger.error(e)

    #def interpret_review(self,category):
    #    df = self.data_cleasing(category)
    #    sentiment = udf(lambda x:TextBlob(x).sentiment[0])
    #    self.spark.udf.register("sentiment",sentiment)
    #    score_df = df.withColumn('reviewText',sentiment('reviewText'))
    #    score_df.show()
        
if __name__ == "__main__":

    logger.info("Now perform the transformation part")
    a = Transformer('Video_Games')

    start_time = time.time()
    #a.list_object_('topics')
    p1 = Process(a.import_file())
    p1.start()
    p2 = Process(a.find_most_rated_goods())
    p2.start()
    p3 = Process(a.amount_of_review_in_each_year())
    p3.start()
    

    transform_time = time.time() - start_time

    logger.info(f'Extract CPU usage {psutil.cpu_percent()}')
    logger.info(f"Extract part has took {transform_time}s")
    
    logger.info("You have sucessfully finished the transformation part, now move on to the load part")
