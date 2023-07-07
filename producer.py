from kafka import KafkaProducer
from time import sleep
import requests
import json
import os
import csv
from datetime import datetime
import requests # pour utiliser le protocole https pour récupéréer les données Alpha Vantage
from datetime import datetime # pour manipuler le type date
# import pytz # time zones
from pyspark.sql.types import StructField, StructType, StringType, LongType, DoubleType, DateType, TimestampType
import pytz

# Serveurs Kafka bootstrap
bootstrap_servers = ['localhost:9092']

# Topic Kafka pour produire des données
topic = 'data-stream'

# Création d'un producer Kafka
producer = KafkaProducer(bootstrap_servers=bootstrap_servers, api_version=(0, 10, 1))
print(os.getcwd())
directory = os.path.join(os.getcwd(), "data/alpha/STOX")

# Créer le répertoire s'il n'existe pas
os.makedirs(directory, exist_ok=True)
topics = ["MSFT", "AMZN", "GOOGL"]
while True:
    # Make a request to the Alpha Vantage API
    for topic in topics:
        req = ("https://www.alphavantage.co/query?"+
            "function=TIME_SERIES_INTRADAY"+
            "&symbol=" + topic + 
            "&interval=1min"+
            "&datatype=csv"+
            "&apikey=ZK5M5JBX3PEQ25D0")
        
        f = requests.get(req)
        content = f.text
        prefix = topic + ','

        contentWithStockName = prefix.join(content.splitlines(True))
        contentWithoutHeader = contentWithStockName.split("\n", 1)[1];
        
        # cdate et temps actuels
        timestamp = datetime.now(pytz.timezone('US/Eastern')).strftime("%Y%m%d_%H%M%S")

        print(contentWithoutHeader)

        producer.send(topic, value=bytes(contentWithoutHeader, 'utf-8'))
    print('Waiting for next call ...')
    sleep(60)
    