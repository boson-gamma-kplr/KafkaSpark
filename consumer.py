from kafka import KafkaConsumer
import json

# Kafka bootstrap servers
bootstrap_servers = ['localhost:9092']

# topic kafka
topic = 'data-stream2'

# Creation d'un consumer kafka
consumer = KafkaConsumer(topic,
                         bootstrap_servers=bootstrap_servers,
                         value_deserializer=lambda m: json.loads(m.decode('utf-8')))

for message in consumer:
    price_data = message.value
    
    # Extraire les valeurs requises
    timestamp = price_data['timestamp']
    open_price = price_data['open']
    high_price = price_data['high']
    low_price = price_data['low']
    close_price = price_data['close']
    volume = price_data['volume']
    
    print('Timestamp: ' + timestamp)
    print('Open Price: ' + open_price)
    print('High Price: ' + high_price)
    print('Low Price: ' + low_price)
    print('Close Price: ' + close_price)
    print('Volume: ' + volume)
    print('---------------------')