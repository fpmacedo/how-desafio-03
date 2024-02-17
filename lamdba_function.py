import json
import boto3
import requests
import logging
import time
import csv
import datetime
import re
import unicodedata

from botocore.exceptions import BotoCoreError, ClientError

# Configurando o logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client('s3')
def normalizar_texto(texto):
    # Normaliza o texto para a forma NFD e remove os acentos
    texto_sem_acentos = unicodedata.normalize('NFD', texto)
    texto_sem_acentos = texto_sem_acentos.encode('ascii', 'ignore').decode('utf-8')
    
    # Substitui espaços por hífens
    texto_sem_espacos = re.sub(r'\s', '-', texto_sem_acentos)
    
    # Remove todos os caracteres que não são letras, dígitos ou hífen
    texto_limpo = re.sub(r'[^a-zA-Z0-9-]', '', texto_sem_espacos)
    
    # Converte o texto para minúsculas
    texto_final = texto_limpo.lower()
    
    return texto_final

def lambda_handler(event, context):
    # Supondo que o evento contém o nome do arquivo CSV no S3
    csv_file = 'cities.csv'
   
    date_time = datetime.datetime.fromtimestamp(int(event['date']))
    date = date_time.strftime('%Y-%m-%d')
    hour = date_time.strftime('%H')
    minute = date_time.strftime('%M')
    print(date)
    print(hour)
    print(minute)
    
    if not csv_file:
        logger.error('Nome do arquivo CSV não fornecido no evento')
        return

    try:
        # Lendo o arquivo CSV do S3
        csv_obj = s3.get_object(Bucket='how-desafio-3', Key=csv_file)
        csv_data = csv_obj['Body'].read().decode('utf-8')
        csv_reader = csv.reader(csv_data.splitlines())

    except (BotoCoreError, ClientError) as err:
        logger.error(f'Erro ao ler o arquivo CSV do S3: {err}')
        return

    # Ignorando o cabeçalho
    next(csv_reader)

    for index, row in enumerate(csv_reader):
        lat = row[1]  # Supondo que a latitude está na primeira coluna
        long = row[2]  # Supondo que a longitude está na segunda coluna
        city = normalizar_texto(row[0])

        #api_url = f'https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={long}&appid=5645791e289ea974155c5ea490e8db3f'
        cnt = 1
        start = event['date']
        api_url = f'https://history.openweathermap.org/data/2.5/history/city?lat={lat}&lon={long}&type=hour&start={start}&cnt={cnt}&appid=5645791e289ea974155c5ea490e8db3f'

        # Tentativas de chamada da API
        for attempt in range(3):
            try:
                # Fazendo a chamada da API
                response = requests.get(api_url, timeout=5)
                data = response.json()
                #print(data)
                #logger.info(data)
                # Verificando se a chamada da API foi bem-sucedida
                response.raise_for_status()

                # Se a chamada da API foi bem-sucedida, sair do loop
                break

            except requests.exceptions.RequestException as err:
                logger.error(f'Erro ao fazer a chamada da API: {err}')
                # Se esta foi a última tentativa, retornar
                if attempt == 2:
                    return
                # Caso contrário, esperar um pouco antes de tentar novamente
                time.sleep(2 ** attempt)

        data = response.json()
        data['city'] = city
        data['latitude'] = lat
        data['longitude'] = long

        # Convertendo os dados para o formato JSON
        json_data = json.dumps(data)

        try:
            # Salvando os dados no S3 com a data no nome do arquivo
            s3.put_object(
                Body=json_data,
                Bucket='how-desafio-3',
                Key=f'raw/{date}/{hour}/{minute}/{city}.json'
            )
        except (BotoCoreError, ClientError) as err:
            logger.error(f'Erro ao salvar os dados no S3: {err}')
            return

        logger.info('Dados salvos com sucesso no S3')