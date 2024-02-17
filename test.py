from pyspark.sql.functions import *
from pyspark.sql import SparkSession
import logging
import os
from datetime import datetime
import boto3
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

def list_files(bucket, prefix):
    files = []
    s3 = boto3.client('s3')
    result = s3.list_objects(Bucket=bucket, Prefix=prefix)
    for obj in result['Contents']:
        files.append(obj['Key'])
    return files

def recent_date_files(file_list):
   # Filtrar apenas os itens que começam com "raw/"
    raw_files = [f for f in file_list if f.startswith('raw/')]

    # Inicializar variáveis para armazenar a data, hora e minuto mais recentes
    most_recent_date = None
    most_recent_hour = None
    most_recent_minute = None
    most_recent_datetime = None

    # Iterar sobre os arquivos raw e atualizar as variáveis com a data, hora e minuto mais recentes
    for f in raw_files:
        # Extrair a data e a hora do nome do arquivo
        parts = f.split('/')
        date_str = parts[1]
        hour_str = parts[2]
        minute_str = parts[3].split('/')[0]
        # Criar um objeto datetime a partir das strings extraídas
        file_datetime = datetime.strptime(f"{date_str} {hour_str}:{minute_str}", '%Y-%m-%d %H:%M')
        # Atualizar a data, hora e minuto mais recentes se necessário
        if most_recent_datetime is None or file_datetime > most_recent_datetime:
            most_recent_datetime = file_datetime
            most_recent_date = date_str
            most_recent_hour = hour_str
            most_recent_minute = minute_str

    return most_recent_date, most_recent_hour, most_recent_minute

def main():
    
    """
    Build ETL Pipeline for How desafio 2:
    
    Call the function to create a spark session;
    Instantiate the input and output paths;
    Call the process functions.
    """
    
    spark = create_spark_session()
    bucket = "how-desafio-3"
    
    files = list_files(bucket, 'raw/')
    recent_file = recent_date_files(files)
    input_data = f"s3://how-desafio-3/raw/{recent_file[0]}/{recent_file[1]}/{recent_file[2]}/*.json"
    print(input_data)
    #output_data = f"s3://how-desafio-3/trusted"
    #process_weather_data(spark, input_data, output_data) 
#
if __name__ == "__main__":
    main()