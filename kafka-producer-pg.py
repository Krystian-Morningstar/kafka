from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def on_send_success(record_metadata):
    print(record_metadata.topic)
    print(record_metadata.partition)
    print(record_metadata.offset)

def on_send_error(excp):
    print('I am an errback', exc_info=excp)

## Modificar la URL 
url='https://raw.githubusercontent.com/Krystian-Morningstar/spark-labs/refs/heads/main/results/video_game_sales/part-00000-77d17a9b-8220-48ed-b92e-996b362786ae-c000.json'

import pandas as pd

df = pd.read_json(url, orient='records', lines=True)

for index, value in df.head(100).iterrows():
    dict_data = dict(value)
    producer.send('games', value=dict_data)
    print(dict_data)

producer.close()
