import streamlit as st
import requests
import pandas  as pd
import json
import pymongo

##Metodo para inicializar la conexion a MongoDB
@st.cache_resource
def init_connection():
    return pymongo.MongoClient(**st.secrets["mongo"])

client = init_connection()

@st.cache_data(ttl=600)
def get_data():
    db = client.games
    items = db.games.find()
    items = list(items) 
    return items

conn = st.connection("postgresql", type="sql")

##Metodo para inicializar la conexion a Github para el Workflow
def post_spark_job(user, repo, job, token, codeurl, dataseturl):
    url = 'https://api.github.com/repos/' + user + '/' + repo + '/dispatches'
    payload = {
        "event_type": job,
        "client_payload": {
        "codeurl": codeurl,
        "dataseturl": dataseturl
        }

    }

    headers = {
        'Authorization': 'Bearer ' + token,
        'Accept': 'application/vnd.github.v3+json',
        'Content-type': 'application/json'
    }

    st.write(url)
    st.write(payload)
    st.write(headers)

    response = requests.post(url, json=payload, headers=headers)

    st.write(response)    

##Sidebar de la aplicacion
st.sidebar.image('identificacion.png') 
st.sidebar.text("Christian Rodrigo Porfirio Castro")
st.sidebar.text("zS21004519")
st.sidebar.text("Base de Datos No Convencionales")
st.sidebar.markdown("______")

##Contenido de la aplicacion
st.title("Spark & Streamlit con Github Actions, MongoDB y PostgreSQL")

github_user  =  st.text_input('Usuario de Github', value='Krystian-Morningstar')
github_repo  =  st.text_input('Repositorio de Github', value='spark-labs')
spark_job    =  st.text_input('Trabajo de Spark', value='spark')
github_token =  st.text_input('Token de Github', value='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')
code_url     =  st.text_input('URL de Codigo', value='https://raw.githubusercontent.com/Krystian-Morningstar/spark-labs/refs/heads/main/games.py')
dataset_url  =  st.text_input('URL de Dataset', value='https://raw.githubusercontent.com/Krystian-Morningstar/spark-labs/refs/heads/main/dataset.csv')

if st.button("POST Spark Submit"):
    post_spark_job(github_user, github_repo, spark_job, github_token, code_url, dataset_url)
    
st.markdown("____")

def get_spark_results(url_results):
    response = requests.get(url_results)
    st.write(response)

    if  (response.status_code ==  200):
        try:
            json_lines = response.text.strip().split("\n") 
            json_data = [json.loads(line) for line in json_lines]
            
            st.json(json_data)
            
        except json.JSONDecodeError as e:
            st.write("Error al decodificar la respuesta JSON:", e)
            st.write(response.text) 
            
st.header("Spark-Submit Results")

url_results=  st.text_input('URL results', value='')

if st.button("GET Spark Results"):
    get_spark_results(url_results)

st.markdown("____")

st.sidebar.header("Query MongoDB & PostgreSQL")

## Método para consultar MongoDB
if st.sidebar.button("Query Mongodb Collection"):
    items = get_data()
    st.header("Resultados de la consulta a MongoDB")
    
    if items:
        for item in items:
            title = item.get("title", "Título desconocido") 
            console = item.get("console", "Consola desconocida")  
            st.write(f"🎮 Titulo:  {title} | 🕹️ Consola: {console}")  
    else:
        st.write("⚠️ No se encontraron datos en MongoDB")
    
    st.markdown("____")

## Método para consultar PostgreSQL
if st.sidebar.button("Query Postgresql table"):
    st.header("Resultados de la consulta a PostgreSQL")
    df = conn.query('SELECT * FROM games;', ttl="10m") 
    for row in df.itertuples():
        st.write(row)
    st.markdown("____")

st.sidebar.markdown("____")