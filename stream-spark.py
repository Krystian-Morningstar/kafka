import streamlit as st
import requests
import pandas  as pd
import json
import pymongo

# Initialize connection.
# Uses st.cache_resource to only run once.
@st.cache_resource
def init_connection():
    return pymongo.MongoClient(**st.secrets["mongo"])

client = init_connection()

# Pull data from the collection.
# Uses st.cache_data to only rerun when the query changes or after 10 min.
@st.cache_data(ttl=600)
def get_data():
    db = client.people
    items = db.people.find()
    items = list(items)  # make hashable for st.cache_data
    return items

# Initialize connection.
conn = st.connection("postgresql", type="sql")

def post_spark_job(user, repo, job, token, codeurl, dataseturl):
    # Define the API endpoint
    url = 'https://api.github.com/repos/' + user + '/' + repo + '/dispatches'
    # Define the data to be sent in the POST request
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

    # Make the POST request
    response = requests.post(url, json=payload, headers=headers)

    # Display the response in the app
    st.write(response)

st.title("Spark & streamlit")

st.header("spark-submit Job")

github_user  =  st.text_input('Github user', value='Krystian-Morningstar')
github_repo  =  st.text_input('Github repo', value='spark-labs')
spark_job    =  st.text_input('Spark job', value='spark')
github_token =  st.text_input('Github token', value='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')
code_url     =  st.text_input('Code URL', value='')
dataset_url  =  st.text_input('Dataset URL', value='')

if st.button("POST spark submit"):
    post_spark_job(github_user, github_repo, spark_job, github_token, code_url, dataset_url)

def get_spark_results(url_results):
    response = requests.get(url_results)
    st.write(response)

    if  (response.status_code ==  200):
        st.write(response.json())
st.header("spark-submit results")

url_results=  st.text_input('URL results', value='https://raw.githubusercontent.com/Krystian-Morningstar/spark-labs/refs/heads/main/results/part-00000-dcb8f532-3af9-463e-bfbb-86b833a68214-c000.json')

if st.button("GET spark results"):
    get_spark_results(url_results)
    

if st.button("Query mongodb collection"):
    items = get_data()

    # Print results.
    for item in items:
        # st.write(f"{item["name"]} : {item['birth']}:")
        try:
            data_dict = json.loads(item["data"])  # Convertir string JSON a diccionario
            name = data_dict.get("name", "Desconocido")
            birth = data_dict.get("birth", "Fecha desconocida")
            st.write(f"{name} : {birth}")
        except json.JSONDecodeError:
            st.write("Error al decodificar los datos")

if st.button("Query Postgresql table"):
    # Perform query.
    df = conn.query('SELECT * FROM people;', ttl="10m")
    # Print results.
    for row in df.itertuples():
        st.write(row)