from kafka import KafkaProducer
import json
import pandas as pd

# Configuración de Kafka Producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def on_send_success(record_metadata):
    print(f"Sent to {record_metadata.topic} | Partition {record_metadata.partition} | Offset {record_metadata.offset}")

def on_send_error(excp):
    print('Error sending message', exc_info=excp)

# URL del dataset
url = 'https://raw.githubusercontent.com/Krystian-Morningstar/spark-labs/refs/heads/main/results/video_game_sales/part-00000-77d17a9b-8220-48ed-b92e-996b362786ae-c000.json'

# Cargar dataset
df = pd.read_json(url, lines=True)  # ✅ Importante: Agregar `lines=True` porque los datos están en formato JSONL

for _, row in df.head(100).iterrows():
    dict_data = row.to_dict()  # ✅ Convertir fila en diccionario
    producer.send('games', value=dict_data).add_callback(on_send_success).add_errback(on_send_error)
    print(f"Sent: {dict_data}")

producer.close()
