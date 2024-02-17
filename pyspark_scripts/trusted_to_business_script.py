from pyspark.sql.functions import *
from pyspark.sql import SparkSession
import logging
import os
import great_expectations as gx
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def create_spark_session():
    
    """
    Create the spark session with the passed configs.
    """
    
    spark = SparkSession \
        .builder \
        .appName("How-Desafio-3")\
        .getOrCreate()

    return spark

def process_cutomers(spark, input_data, output_data):

    """
    Perform ETL on orders to create the customers
      
    Parameters:
    - spark: spark session
    - input_data : path to input files
    - output_data : path to output files
    """


    #reading json files
    weather_file_Path = input_data
    
    weather_trusted=spark.read.parquet(weather_file_Path)

    weather_business = (weather_trusted.select(col('list')[0].alias('list_0'), 'city', 'latitude', 'longitude', 'date_partition')
                                   .select('city',
                                           'latitude',
                                           'longitude',                                           
                                           col('list_0.weather.main')[0].alias('main_weather'),
                                           col('list_0.weather.description')[0].alias('main_weather_description'),
                                           'list_0.main.temp',
                                           'list_0.main.feels_like',
                                           'list_0.main.pressure',
                                           'list_0.main.humidity',
                                           'list_0.main.temp_min',
                                           'list_0.main.temp_max',
                                           'list_0.wind.speed',
                                           'list_0.wind.deg',
                                           col('list_0.clouds.all').alias('clouds'),
                                           col('list_0.dt').alias('collect_timestamp'),
                                          col('date_partition').cast(StringType()))
                    )

    data_quality(weather_business)

    weather_business.write.partitionBy('date_partition').parquet(os.path.join(output_data, 'weather_business'), 'overwrite')

    
    print("--- weather_business.parquet completed ---")

def data_quality(input_dataset):
    
    gx_context = gx.get_context()
    datasource = gx_context.sources.add_spark("my_spark_datasource")

    data_asset = datasource.add_dataframe_asset(name="my_df_asset", dataframe=input_dataset).build_batch_request()
    
    gx_context.add_or_update_expectation_suite("my_expectation_suite")
    
    #my_batch_request = data_asset
    
    validator = gx_context.get_validator(
    batch_request=data_asset,
    expectation_suite_name="my_expectation_suite"
                                        )
    
    weather_null = validator.expect_column_values_to_not_be_null(column="city")
    date_format = validator.expect_column_values_to_match_strftime_format("date_partition", "%Y-%m-%d")
    rows_number = validator.expect_table_row_count_to_be_between(27,27)

    
    if weather_null.success == False :
      raise ValueError(f"Data quality check failed {weather_null.expectation_config.kwargs['column']} is null.")
    else : logger.info(f"Data quality check success {weather_null.expectation_config.kwargs['column']} is not null.")
    
    if date_format.success == False :
      raise ValueError(f"Data quality check failed {date_format.expectation_config.kwargs['column']} is in the wrong format.")
    else: logger.info(f"Data quality check success {date_format.expectation_config.kwargs['column']}  is in the right format.")
        
    if rows_number.success == False :
      raise ValueError(f"Data quality check failed, dataset has unexpected number of rows.")
    else: logger.info(f"Data quality check success, dataset has the expected number of rows.")
       
    logger.info(f"All validators passed with success!")

def main():
    
    """
    Build ETL Pipeline for How desafio 2:
    
    Call the function to create a spark session;
    Instantiate the input and output paths;
    Call the process functions.
    """
    
    spark = create_spark_session()
    trusted = "s3://how-desafio-3/trusted/"
    business = "s3://how-desafio-3/business/"
    
    process_cutomers(spark, trusted, business)
    #data_quality(spark, input_data, output_data)

if __name__ == "__main__":
    main()