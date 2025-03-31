from kafka import KafkaConsumer
from pymongo import MongoClient
from pymongo.server_api import ServerApi
import json

# URI de conexión a MongoDB
uri = "mongodb+srv://rodrigo:040103@test.uhvwuww.mongodb.net/?retryWrites=true&w=majority&appName=test"

# Conectar a MongoDB
try:
    client = MongoClient(uri, server_api=ServerApi('1'))
    client.admin.command('ping')
    print("✅ Conexión exitosa a MongoDB!")

    db = client.games  # Base de datos
    collection = db.games  # Colección
    print("✅ Colección seleccionada correctamente!")
except Exception as e:
    print(f"❌ No se pudo conectar a MongoDB: {e}")

# Configuración de Kafka Consumer
consumer = KafkaConsumer('games', bootstrap_servers=['localhost:9092'], value_deserializer=lambda m: json.loads(m.decode('utf-8')))

# Leer mensajes de Kafka y guardarlos en MongoDB
for msg in consumer:
    record = msg.value  # ✅ Ya es un diccionario, no hace falta json.loads()
    print(f"📥 Recibido: {record}")

    try:
        record_id = collection.insert_one(record).inserted_id  # ✅ Insertar en la colección correcta
        print(f"✅ Insertado en MongoDB con ID: {record_id}")
    except Exception as e:
        print(f"❌ No se pudo insertar en MongoDB: {e}")
