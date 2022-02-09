from sqlalchemy import create_engine
import pandas as pd
import pymongo
import requests
from etl_tools import clean_text, length_text, positivity_score
from etl_tools import negativity_score, neutral_score, compound_score, label_sentiment
import logging
from args import parser

# 5 login
logging.basicConfig(level=logging.INFO)

#information sur la connexion
HOST_POSTGRES = 'postgresql'
PORT_POSTGRES = '5432'
DBNAME_POSTGRES = 'twitter_database'
USERNAME='admin'
PASSWORD='root'

HOST_MONGO = 'mongodb'
PORT_MONGO = '27017'
CLIENT = pymongo.MongoClient('mongodb')
DB = CLIENT.twitter_db

def extract_data(id_index):
    """Read data from Mongodb"""
    if id_index == 0:
        cursor = DB.tweets.find({})
        current_size = DB.tweets.find({}).count()
    else:
        cursor = DB.tweets.find({}).skip(id_index)
        current_size = DB.tweets.find({}).count()

    # DataFrame
    df =  pd.DataFrame(list(cursor))
    logging.info("Mongo data loaded")
    logging.info(str(df.shape))   # logging
    print(df.columns.values)
    return df, current_size

def transform_data(df):
    """Update data for Postgres"""
    # Création d'étiquettes
    df['hour'] = pd.DatetimeIndex(df['time']).hour
    df['minute'] = pd.DatetimeIndex(df['time']).minute
    df['day'] = pd.DatetimeIndex(df['time']).day
    df['month'] = pd.DatetimeIndex(df['time']).month
    df['year'] = pd.DatetimeIndex(df['time']).year
    #Nettoyer le texte du tweet
    df['clean text'] = df['text'].apply(clean_text)
    #Ajouter une colonne de longueur du tweet
    df['length text'] = df['clean text'].apply(length_text)
    #colonne de score de positivité du tweet
    df['positivity'] = df['clean text'].apply(positivity_score)
    #colonne de score de négativité du tweet
    df['negativity'] = df['clean text'].apply(negativity_score)
    #colonne de score neutre du tweet
    df['neutral'] = df['clean text'].apply(neutral_score)
    #colonne score composé du tweet
    df['compound'] = df['clean text'].apply(compound_score)
    #Ajouter une étiquette de colonne sentiment du tweet
    df['sentiment'] = df['compound'].apply(label_sentiment)
    #Sauvegarder dans Postgres
    df = df[['time', 'followers','user_favorites_count', 'username', 'hour', 'minute', 'day',
           'month', 'year', 'clean text', 'length text', 'positivity',
           'negativity', 'neutral', 'compound', 'sentiment']]
    return df

def load_data(df, job_number):
    """Load data into Postgres"""
    connection_string = 'postgres://admin:root@172.17.0.1:5432/twitter_database?sslmode=disable'
    engine = create_engine(connection_string)

    if job_number == 1:
        df.to_sql('tweets_data', engine, if_exists='replace')
    else:
        df.to_sql('tweets_data', engine, if_exists='append')
    logging.info(f"Postgres data loaded for job number {job_number}")
    logging.info(str(df.shape))

# Charger les paramètres du programmateur
args = parser.parse_args()

index_etl = args.index_mongo
job_number = args.job_number

#Extraction des données et sauvegarde de l'index actuel des données Mongo.
df, id_index = extract_data(index_etl)
logging.info(f"id_index is now {id_index} for Mongo")
logging.info(f"job number is now {job_number} for ETL")
d={'index_saved':id_index,'job_nb':job_number}
save_index = pd.DataFrame(d, index=[0])

if job_number == 1 :
    save_index.to_csv('save_index.csv', index=None)
else:
    save_index.to_csv('save_index.csv', index=None, mode='a', header=False)

#Transformer les données pour Postgres :
df = transform_data(df)
#Sauvegarder les données dans Postgres
load_data(df, job_number)
