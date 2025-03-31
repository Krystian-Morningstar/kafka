from kafka import KafkaConsumer
import json
import psycopg2

print('connecting pg ...')

try:
    conn = psycopg2.connect(database = "defaultdb", 
                        user = "avnadmin", 
                        host= 'pg-1aa56d3f-rodrigo413.h.aivencloud.com',
                        password = "AVNS_SeUblJcKtl3naJLVcKs",
                        port = 15385)
    cur = conn.cursor()
    print("PosgreSql Connected successfully!")
except:
    print("Could not connect to PosgreSql")

consumer = KafkaConsumer('games',bootstrap_servers=['localhost:9092'])

for msg in consumer:
    record = json.loads(msg.value.decode('utf-8')) 
    title = record["title"]  
    console = record["console"]
    
    try:
        sql = "INSERT INTO games (title, console) VALUES (%s, %s)"  
        cur.execute(sql, (title, console))  
        conn.commit()
        print(f"Inserted: {title} - {console}")
    except Exception as e:
        print("Could not insert into PostgreSQL:", e)
conn.close()
