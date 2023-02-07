from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from datetime import datetime, timedelta


default_args = {
    'owner': 'thomas',
    'start_date': datetime(2023, 2, 3),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'dagProcessusMetier',
    default_args=default_args,
    description='dag Processus Metier',
    schedule_interval=timedelta(days=1),
)

def load_data_to_mongodb():
    from pymongo.errors import BulkWriteError
    import json
    from pymongo import MongoClient

    print("Début fonction!")
    uri = "mongodb+srv://user:user@cluster0.rs5oaul.mongodb.net/dw"
    client = MongoClient(uri)
    db = client.dw

    with open('/home/tr/airflow/dags/file/versailles_tweets_100.json', encoding='utf8') as f:
        file_json = json.load(f)
    print("Fin lecture fichier json")
    try:
        result = db.tweets.insert_many(file_json)
        print("Fin instruction envoi fichiers à mongodb")
        if result.inserted_ids:
            print(f"{len(result.inserted_ids)} documents ont été insérés avec succès.")
        else:
            print("Aucun document n'a été inséré.")

    except BulkWriteError as exc:
        print(exc.details)
    print("Fin fonction!")

# Function to connect to ElephantSQL and create tables
def create_tables(ds, **kwargs):
    import psycopg2
    conn = psycopg2.connect(
        host="kandula.db.elephantsql.com",
        database="chvayfrg",
        user="chvayfrg",
        password="YEuFsFQY7kmv-ULXW09-VIG-dBlzfoYr"
    )
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS tweet_analyse (
        id_twt    varchar(255) NOT NULL,
        auteur    varchar(255) NOT NULL,
        sentiment varchar(255) NOT NULL,
        PRIMARY KEY (id_twt)
    );


    CREATE TABLE IF NOT EXISTS tweet_hashtag (
        id_hashtag bigint ,
        id_twt     varchar(255) NOT NULL,
        hashtag    varchar(255) NOT NULL,
        PRIMARY KEY (id_hashtag),
        FOREIGN KEY (id_twt) REFERENCES tweet_analyse (id_twt)
    );

    CREATE TABLE IF NOT EXISTS tweet_topic (
        id_topic bigint,
        id_twt   varchar(255) NOT NULL,
        topic    varchar(255) NOT NULL,
        PRIMARY KEY (id_topic),
        FOREIGN KEY (id_twt) REFERENCES tweet_analyse (id_twt)
    );
    """)
    conn.commit()
    conn.close()



def get_author(**kwargs):
    from pymongo import MongoClient
    client = MongoClient("mongodb+srv://user:user@cluster0.rs5oaul.mongodb.net/dw")
    dbname = client.dw
    collection_name = dbname.tweets
    twt_id = kwargs['twt_id']
    doc = collection_name.find_one({"id" : twt_id},{"author_id"})
    return doc["author_id"]

def get_all_authors(**kwargs):
    from pymongo import MongoClient
    client = MongoClient("mongodb+srv://user:user@cluster0.rs5oaul.mongodb.net/dw")
    dbname = client.dw
    collection_name = dbname.tweets
    all_docs = collection_name.find({}, {"id", "author_id"})
    all_authors = [get_author(twt_id=doc['id']) for doc in all_docs]
    print('#########',all_authors)
    return all_authors

def value(**kwargs):     #Renvoyer les balises pour le hashtag
    list = []
    dict_list = kwargs['dict']
    for item in dict_list:
        list.append(str(item.get('tag')))
    return list

def get_hashtags(**kwargs):
    from pymongo import MongoClient
    twt_id = kwargs['twt_id']
    client = MongoClient("mongodb+srv://user:user@cluster0.rs5oaul.mongodb.net/dw")
    dbname = client.dw
    collection_name = dbname.tweets
    doc = collection_name.find_one({"id" : twt_id},{"entities.hashtags.tag"})
    h = {}
    h[f"id-{doc['_id']}"] = None
    if("entities" in doc):
        for item in doc["entities"]:
            if("hashtags" in item):
                for elt in doc["entities"].values():
                    h[f"id-{doc['_id']}"] = value(dict=elt)
    else:
        h[f"id-{doc['_id']}"] = None
    return h 

def get_all_hashtags(**kwargs):
    from pymongo import MongoClient
    client = MongoClient("mongodb+srv://user:user@cluster0.rs5oaul.mongodb.net/dw")
    dbname = client.dw
    collection_name = dbname.tweets
    all_docs = collection_name.find({})
    all_hashtags = [get_hashtags(twt_id=doc['id']) for doc in all_docs]
    print("########### ",all_hashtags)
    return all_hashtags

def get_all_tweet_ids(**kwargs):
    from pymongo import MongoClient
    client = MongoClient("mongodb+srv://user:user@cluster0.rs5oaul.mongodb.net/dw")
    # Sélectionnez la base de données et la collection à utiliser
    db = client["dw"]
    collection = db["tweets"]

    # Récupérer les documents
    docs = collection.find()

    # Créer une liste pour stocker les ID
    ids = []

    # Boucle sur les documents et ajouter les ID à la liste
    for doc in docs:
        ids.append(doc["_id"])

    print("#### ", ids)
    return ids

def get_topics(**kwargs):
    import numpy as np
    from pymongo import MongoClient
    twt_id = kwargs['twt_id']
    client = MongoClient("mongodb+srv://user:user@cluster0.rs5oaul.mongodb.net/dw")
    dbname = client.dw
    collection_name = dbname.tweets
    doc = collection_name.find_one({"id" : twt_id},{"context_annotations.domain.name"})
    t = {}
    list_top = []
    if('context_annotations' in doc):
        for item in doc['context_annotations']:
            if('domain' in item):
                for elt in item["domain"].values():
                    list_top.append(str(elt))
            t[f"id-{doc['_id']}"] = list(np.unique(list_top))
    else:
        t[f"id-{doc['_id']}"] = None
    return t

def get_all_topics(**kwargs):
    from pymongo import MongoClient
    client = MongoClient("mongodb+srv://user:user@cluster0.rs5oaul.mongodb.net/dw")
    dbname = client.dw
    collection_name = dbname.tweets
    all_docs = collection_name.find({})
    all_topics = [get_topics(twt_id=doc['id']) for doc in all_docs]
    print("########### ",all_topics)
    return all_topics

def get_sentiment(**kwargs):
    from pymongo import MongoClient
    from textblob import TextBlob
    twt_id = kwargs['twt_id']
    client = MongoClient("mongodb+srv://user:user@cluster0.rs5oaul.mongodb.net/dw")
    dbname = client.dw
    collection_name = dbname.tweets
    doc = collection_name.find_one({"id" : twt_id},{"text"})
    id = doc["_id"]
    twt_txt = TextBlob(doc["text"])
    if twt_txt.sentiment.polarity < 0:
        return 'Negatif'
    elif twt_txt.sentiment.polarity == 0:
        return 'Neutre'
    else:
        return 'Positif'
    
def get_all_sentiment(**kwargs):
    from pymongo import MongoClient
    client = MongoClient("mongodb+srv://user:user@cluster0.rs5oaul.mongodb.net/dw")
    dbname = client.dw
    collection_name = dbname.tweets
    all_docs = collection_name.find({})
    all_sentiment = [get_sentiment(twt_id=doc['id']) for doc in all_docs]
    print("########### ",all_sentiment)
    return all_sentiment

def insert_to_datalake(**kwargs):
    import psycopg2
    from urllib.parse import urlparse
    ti = kwargs['ti']
    url = urlparse("postgres://chvayfrg:YEuFsFQY7kmv-ULXW09-VIG-dBlzfoYr@kandula.db.elephantsql.com/chvayfrg")
    conn = psycopg2.connect(
        database=url.path[1:],
        user=url.username,
        password=url.password,
        host=url.hostname,
        port=url.port
    )
    conn.autocommit = True
    cursor = conn.cursor()

    author_task_instance = ti.xcom_pull(task_ids='get_all_authors')
    sentiment_task_instance = ti.xcom_pull(task_ids='get_all_sentiment')

    h = ti.xcom_pull(task_ids='get_all_hashtags')
    h_list = [ha.values() for ha in h]
    hashtags_task_instance = [item for sublist in h_list for item in sublist]
    
    t = ti.xcom_pull(task_ids='get_all_topics')
    t_list = [to.values() for to in t]
    topics_task_instance = [item for sublist in t_list for item in sublist]

    id_tweet = ti.xcom_pull(task_ids='get_all_tweet_ids')
    print("#####ID TWEET ",id_tweet)
    print("#####TOPICS ",topics_task_instance)
    print("#####HASHTAGS ",hashtags_task_instance)
    print("#####SENTIMENT ",sentiment_task_instance)
    print("#####AUTEUR    ", author_task_instance)
    cpt = 1
    cpt2 = 1 
    for i in range(len(author_task_instance)):
        auteur = author_task_instance[i]
        sentiment = sentiment_task_instance[i]
        id = id_tweet[i]
        cursor.execute('INSERT INTO tweet_analyse(id_twt, auteur, sentiment) VALUES (%s,%s, %s) ON CONFLICT DO NOTHING',(id, auteur, sentiment))
        topic = topics_task_instance[i]
        for elt in topic:
            cursor.execute('INSERT INTO tweet_topic(id_topic, id_twt, topic) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING',(cpt, id,  elt))
            cpt += 1
        hashtag = hashtags_task_instance[i]
        for elt in hashtag:
            cursor.execute('INSERT INTO tweet_hashtag(id_hashtag, id_twt, hashtag) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING',(cpt2, id, elt))
            cpt2 += 1
    



    conn.close()
    print('####FINIIII')


def top_hashtags(**kwargs):
    import psycopg2
    from urllib.parse import urlparse
    K = kwargs['params']['K']
    url = urlparse("postgres://chvayfrg:YEuFsFQY7kmv-ULXW09-VIG-dBlzfoYr@kandula.db.elephantsql.com/chvayfrg")
    conn = psycopg2.connect(
        database=url.path[1:],
        user=url.username,
        password=url.password,
        host=url.hostname,
        port=url.port
    )
    conn.autocommit = True
    cursor = conn.cursor()
    cursor.execute(f'SELECT hashtag, COUNT(id_twt) FROM tweet_hashtag GROUP BY hashtag ORDER BY COUNT(id_twt) DESC LIMIT {K}')
    result = cursor.fetchall()
    # Sauvegarde du résultat dans un fichier
    with open('top_f"{K}"_hashtags.txt', 'w') as f:
        for hashtags, count in result:
            f.write(f"{hashtags}: {count}\n")
    return result


def top_topics(**kwargs):
    import psycopg2
    from urllib.parse import urlparse
    K = kwargs['params']['K']
    url = urlparse("postgres://chvayfrg:YEuFsFQY7kmv-ULXW09-VIG-dBlzfoYr@kandula.db.elephantsql.com/chvayfrg")
    conn = psycopg2.connect(
        database=url.path[1:],
        user=url.username,
        password=url.password,
        host=url.hostname,
        port=url.port
    )
    conn.autocommit = True
    cursor = conn.cursor()
    # Exécution de la requête
    cursor.execute(f"SELECT topic, COUNT(id_twt) FROM tweet_topic GROUP BY topic ORDER BY COUNT(id_twt) DESC LIMIT {K}")
    result = cursor.fetchall()

    # Sauvegarde du résultat dans un fichier
    with open('top_f"{K}"_topics.txt', 'w') as f:
        for topic, count in result:
            f.write(f"{topic}: {count}\n")


def top_users(**kwargs):
    import psycopg2
    from urllib.parse import urlparse
    K = kwargs['params']['K']
    url = urlparse("postgres://chvayfrg:YEuFsFQY7kmv-ULXW09-VIG-dBlzfoYr@kandula.db.elephantsql.com/chvayfrg")
    conn = psycopg2.connect(
        database=url.path[1:],
        user=url.username,
        password=url.password,
        host=url.hostname,
        port=url.port)
    conn.autocommit = True
    cursor = conn.cursor()
    # Exécution de la requête
    cursor.execute(f"SELECT auteur, COUNT(id_twt) FROM tweet_analyse GROUP BY auteur ORDER BY COUNT(id_twt) DESC LIMIT{K}")
    result = cursor.fetchall()
    # Sauvegarde du résultat dans un fichier
    with open('top_f"{K}"_users.txt', 'w') as f:
        for users, count in result:
            f.write(f"{users}: {count}\n")



def top_hashtags_positive_sentiment(**kwargs):
    import psycopg2
    from urllib.parse import urlparse
    K = kwargs['params']['K']
    url = urlparse("postgres://chvayfrg:YEuFsFQY7kmv-ULXW09-VIG-dBlzfoYr@kandula.db.elephantsql.com/chvayfrg")
    conn = psycopg2.connect(
        database=url.path[1:],
        user=url.username,
        password=url.password,
        host=url.hostname,
        port=url.port
    )
    conn.autocommit = True
    result = cursor.fetchall()
    cursor = conn.cursor()
    cursor.execute(f"SELECT hashtag, COUNT(id_twt) FROM tweet_hashtag JOIN tweet_analyse ON tweet_hashtag.id_twt = tweet_analyse.id_twt WHERE sentiment = 'positif' GROUP BY hashtag ORDER BY COUNT(id_twt) DESC LIMIT {K}")
    # Save the result to a file
    with open('hashtags_top_f"{K}"_positive_sentiment.txt', "w") as f:
        for row in result:
            f.write("{}: {}\n".format(row[0], row[1]))
    conn.commit()
    cursor.close()
    conn.close()

def top_hashtags_negative_sentiment(**kwargs):
    import psycopg2
    from urllib.parse import urlparse
    K = kwargs['params']['K']
    url = urlparse("postgres://chvayfrg:YEuFsFQY7kmv-ULXW09-VIG-dBlzfoYr@kandula.db.elephantsql.com/chvayfrg")
    conn = psycopg2.connect(
        database=url.path[1:],
        user=url.username,
        password=url.password,
        host=url.hostname,
        port=url.port
    )
    conn.autocommit = True
    result = cursor.fetchall()
    cursor = conn.cursor()
    cursor.execute(f"SELECT hashtag, COUNT(id_twt) FROM tweet_hashtag JOIN tweet_analyse ON tweet_hashtag.id_twt = tweet_analyse.id_twt WHERE sentiment = 'negative' GROUP BY hashtagORDER BY COUNT(id_twt) DESC LIMIT {K}")
    # Save the result to a file
    with open('hashtags_top_f"{K}"_negative_sentiment.txt', "w") as f:
        for row in result:
            f.write("{}: {}\n".format(row[0], row[1]))
    conn.commit()
    cursor.close()
    conn.close()


# Retrieve top K here positive sentiment hashtag
top_hashtags_positive_task = PythonOperator(
    task_id='top_hashtags_positive_sentiment',
    python_callable=top_hashtags_positive_sentiment,
    params={'K': 10},
    provide_context=True,
    dag=dag
)

# Retrieve top K here positive sentiment hashtag
top_hashtags_negative_task = PythonOperator(
    task_id='top_hashtags_negative_sentiment',
    python_callable=top_hashtags_negative_sentiment,
    params={'K': 10},
    provide_context=True,
    dag=dag
)

# Operator for retrieving top K topics
top_users_task = PythonOperator(
    task_id='top_users',
    python_callable=top_users,
    params={'K': 10},
    provide_context=True,
    dag=dag,
)

# Operator for retrieving top K topics
top_topics_task = PythonOperator(
    task_id='top_topics',
    python_callable=top_topics,
    params={'K': 10},
    provide_context=True,
    dag=dag,
)


# Operator for retrieving top K hashtags
top_hashtags_task = PythonOperator(
    task_id='top_hashtags',
    python_callable=top_hashtags,
    provide_context=True,
    params={'K': 10},
    dag=dag
)



# Operator for retrieving all ids tweets
get_all_tweet_ids_task = PythonOperator(
    task_id='get_all_tweet_ids',
    python_callable=get_all_tweet_ids,
    provide_context=True,
    dag=dag
)
    

# Operator for retrieving data from all previous tasks
insert_to_datalake_task = BranchPythonOperator(
    task_id='insert_to_datalake',
    python_callable=insert_to_datalake,
    provide_context=True,
    dag=dag
)

# Create a task to return all tags
get_all_sentiment_task = PythonOperator(
    task_id='get_all_sentiment',
    python_callable=get_all_sentiment,
    provide_context=True,
    dag=dag
)

# Create a task to return sentiment
get_sentiment_task = PythonOperator(
    task_id='get_sentiment',
    python_callable=get_sentiment,
    provide_context=True,
    dag=dag
)

# Create a task to return all tags
get_value_task = PythonOperator(
    task_id='value',
    python_callable=value,
    provide_context=True,
    dag=dag
)


# Create a task to return all topics
get_all_topics_task = PythonOperator(
    task_id='get_all_topics',
    python_callable=get_all_topics,
    provide_context=True,
    dag=dag
)

# Create a task to return topics
get_topics_task = PythonOperator(
    task_id='get_topics',
    python_callable=get_topics,
    provide_context=True,
    dag=dag
)

# Create a task to return all hashtags
get_all_hashtags_task = PythonOperator(
    task_id='get_all_hashtags',
    python_callable=get_all_hashtags,
    provide_context=True,
    dag=dag
)


# Create a task to return hashtags
get_hashtags_task = PythonOperator(
    task_id='get_hashtags',
    python_callable=get_hashtags,
    provide_context=True,
    dag=dag
)


# Create a task to return all author name
get_all_authors_task = PythonOperator(task_id='get_all_authors',
                      python_callable=get_all_authors,
                      provide_context=True,
                      dag=dag) 


# Create a task to return author name
get_author_task = PythonOperator(task_id='get_author',
                      python_callable=get_author,
                      provide_context=True,
                      dag=dag)



# Create a task to create tables
create_tables_task = PythonOperator(task_id='create_tables',
                                     python_callable=create_tables,
                                     provide_context=True,
                                     dag=dag)

# Create a task to load data to mongodb
load_data_to_mongodb_task = PythonOperator(
    task_id='load_data_to_mongodb',
    python_callable=load_data_to_mongodb,
    dag=dag
)


load_data_to_mongodb_task >> create_tables_task >> get_all_authors_task >> get_all_hashtags_task >> get_all_topics_task >> get_all_sentiment_task >> insert_to_datalake_task >> top_hashtags_task >> top_topics_task >> top_users_task >> top_hashtags_negative_task >> top_hashtags_positive_task

