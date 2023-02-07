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
    'dagPMLocal',
    default_args=default_args,
    description='dag PM',
    schedule_interval=timedelta(days=1),
)





def create_dw(ds, **kwargs):
    import psycopg2
    conn = psycopg2.connect(database="dw_inpoda", user="postgres", password="user")
    cur = conn.cursor()
    # création des tables
    cur.execute("""CREATE TABLE IF NOT EXISTS tweets(
                    id_tweet   INT PRIMARY KEY,
                    id_author BIGINT NOT NULL,
                    sentiment VARCHAR(8) NOT NULL
                    )""")
    cur.execute("""CREATE TABLE IF NOT EXISTS tweets_topics(
                    id_tweet INT,
                    topic VARCHAR(150),
                    PRIMARY KEY(id_tweet, topic),
                    CONSTRAINT fk_tweet
                        FOREIGN KEY(id_tweet)
                            REFERENCES tweets(id_tweet)
                    )""")
    cur.execute("""CREATE TABLE IF NOT EXISTS tweets_hashtags(
                    id_tweet INT,
                    hashtag VARCHAR(50),
                    PRIMARY KEY(id_tweet, hashtag),
                    CONSTRAINT fk_tweet
                        FOREIGN KEY(id_tweet)
                            REFERENCES tweets(id_tweet)
                    )""")
    print("Table created successfully")


def init_dl(ds, **kwargs):
        import psycopg2
        from psycopg2.extras import Json
        import json
        conn = psycopg2.connect(database="datalake", user="postgres", password="user")
        cur = conn.cursor()
        # creation of the table in the database if it does not exist
        cur.execute("""CREATE TABLE IF NOT EXISTS t_json(
                    id   INT    GENERATED BY DEFAULT AS IDENTITY,
                    c_json   json)""")
        cur.execute("""CREATE UNIQUE INDEX IF NOT EXISTS on_id_tweets ON t_json( (c_json->>'id') ) ;""")
        # chargement du fichier json
        my_file = open("file/versailles_tweets_100.json", encoding="utf-8")
        data = json.load(my_file)
        # insertion de tous les tuples avec clés générées automatiquement
        for index in range(len(data)):
            d = data[index]
            cur.execute("""INSERT INTO t_json (c_json) VALUES (%s) ON CONFLICT DO NOTHING""", [Json(d)])
        conn.commit()
        conn.close()    

def author_id(**kwargs):
        import psycopg2
        conn = psycopg2.connect(database="datalake", user="postgres", password="user")
        cur = conn.cursor()
        key = kwargs['key']
        cur.execute("""SELECT c_json
                            FROM t_json
                            WHERE id = (%s)""", [key])
        row = cur.fetchone()
        conn.commit()
        conn.close()
        dic = row[0]
        return dic['author_id']

def hashtag_extraction(**kwargs):
        import psycopg2
        conn = psycopg2.connect(database="datalake", user="postgres", password="user")
        cur = conn.cursor()
        key = kwargs['key']
        cur.execute("""SELECT c_json
                                FROM t_json
                                WHERE id = (%s)""", [key])
        row = cur.fetchone()
        conn.commit()
        conn.close()
        dic = row[0]
        list_hashtags = []
        try:
            d = dic['entities']['hashtags']
            for index in range(len(d)):
                hashtag = d[index]['tag']
                list_hashtags.append(hashtag)
        except KeyError:
            pass
        return list_hashtags

def sentiment_analysis(**kwargs):
        import psycopg2
        from textblob import TextBlob
        conn = psycopg2.connect(database="datalake", user="postgres", password="user")
        cur = conn.cursor()
        key = kwargs['key']
        cur.execute("""SELECT c_json
                                        FROM t_json
                                        WHERE id = (%s)""", [key])
        row = cur.fetchone()
        conn.commit()
        conn.close()
        dic = row[0]
        tweet = TextBlob(dic['text'])
        if tweet.sentiment.polarity >= 0:
            return "positive"
        return "negative"

def topic_id(**kwargs):
        import psycopg2
        conn = psycopg2.connect(database="datalake", user="postgres", password="user")
        cur = conn.cursor()
        key = kwargs['key']
        cur.execute("""SELECT c_json
                                        FROM t_json
                                        WHERE id = (%s)""", [key])
        row = cur.fetchone()
        conn.commit()
        conn.close()
        dic = row[0]
        list_topics = []
        try:
            d = dic['context_annotations']
            for index in range(len(d)):
                topic = d[index]['entity']['name']
                list_topics.append(topic)
        except KeyError:
            pass
        return list_topics

def insert_dw(**kwargs):
        import psycopg2
        conn = psycopg2.connect(database="dw_inpoda", user="postgres", password="user")
        cur = conn.cursor()
        

    # insertion des tuples et/ou actualisation de la base de données


        conn2 =  psycopg2.connect(database="datalake", user="postgres", password="user")
        cur2 = conn2.cursor
        cur2.execute("""SELECT * FROM t_json""")
        result = cur2.fetchall()
        ti = kwargs['ti']
        for row in result:
            id_tweet = row[0]
            tweet_author = author_id(key=id_tweet)
            tweet_sentiment = sentiment_analysis(key=id_tweet)
            cur.execute("""INSERT INTO tweets(id_tweet, id_author, sentiment)
                            VALUES (%s, %s, %s)
                            ON CONFLICT DO NOTHING""", (id_tweet, tweet_author, tweet_sentiment))
            print("Tweet "+str(id_tweet) + " inserted successfully or already inserted")
            hashtags = hashtag_extraction(key=id_tweet)
            if hashtags:
                for index in range(len(hashtags[0])):
                    cur.execute("""INSERT INTO tweets_hashtags(id_tweet, hashtag) 
                                    VALUES (%s, %s)
                                    ON CONFLICT DO NOTHING
                                """, (id_tweet, hashtags[0][index]))
                    print("Couple tweet-hashtag Tweet" + str(id_tweet) + "---" + hashtags[0][index] + "inserted successfully or already inserted")
            topics = topic_id(key=id_tweet)
            if topics:
                for index in range(len(topics[0])):
                    cur.execute("""INSERT INTO tweets_topics(id_tweet, topic) 
                                        VALUES (%s, %s)
                                        ON CONFLICT DO NOTHING
                                    """, (id_tweet, topics[0][index]))
                    print("Couple tweet-topic Tweet" + str(id_tweet) + "---" + topics[0][
                        index] + "inserted successfully or already inserted")



def publication_by_user(**kwargs):
        import psycopg2
        conn = psycopg2.connect(database="dw_inpoda", user="postgres", password="user")
        cur = conn.cursor()
        cur.execute("""SELECT id_author, COUNT(*) AS nb_p
                        FROM tweets
                        GROUP BY id_author
                        """)
        result = cur.fetchall()
        conn.commit()
        conn.close()
        dic_users = {}
        try:
            for row in result:
                users = str(row[0])
                nb_p = row[1]
                dic_users["U" + users] = nb_p
                print(dic_users)
        except KeyError:
            pass
        return dic_users

def publication_by_hashtag(**kwargs):
        import psycopg2
        conn = psycopg2.connect(database="dw_inpoda", user="postgres", password="user")
        cur = conn.cursor()
        cur.execute("""SELECT hashtag, COUNT(*) AS nb_p
                        FROM tweets_hashtags
                        GROUP BY hashtag
                        """)
        result = cur.fetchall()
        conn.commit()
        conn.close()
        dic_hashtags = {}
        try:
            for row in result:
                hashtags = row[0]
                nb_p = row[1]
                dic_hashtags[hashtags] = nb_p
                print(dic_hashtags)
        except KeyError:
            pass
        return dic_hashtags

def publication_by_topic(**kwargs):
        import psycopg2
        conn = psycopg2.connect(database="dw_inpoda", user="postgres", password="user")
        cur = conn.cursor()
        cur.execute("""SELECT topic, COUNT(*) AS nb_p
                        FROM tweets_topics
                        GROUP BY topic
                        """)
        result = cur.fetchall()
        conn.commit()
        conn.close()
        dic_topics = {}
        try:
            for row in result:
                topics = row[0]
                topics = topics.replace(" ", "_")
                topics = topics.replace("&", "and")
                nb_p = row[1]
                dic_topics[topics] = nb_p
                print(dic_topics)
        except KeyError:
            pass
        return dic_topics

init_dl_task = PythonOperator(
    task_id='init_dl',
    python_callable=init_dl,
    provide_context=True,
    dag=dag
)

create_dw_task = PythonOperator(
    task_id='create_dw',
    python_callable=create_dw,
    provide_context=True,
    dag=dag
)


author_id_task = PythonOperator(
    task_id='author_id',
    python_callable=author_id,
    provide_context=True,
    dag=dag
)

hashtag_extraction_task = PythonOperator(
    task_id='hashtag_extraction',
    python_callable=hashtag_extraction,
    provide_context=True,
    dag=dag 
)

sentiment_analysis_task = PythonOperator(
    task_id='sentiment_analysis',
    python_callable=sentiment_analysis,
    provide_context=True,
    dag=dag 
)

topic_id_task = PythonOperator(
    task_id='topic_id',
    python_callable=topic_id,
    provide_context=True,
    dag=dag 
)

insert_dw_task = BranchPythonOperator(
    task_id='insert_dw',
    python_callable=insert_dw,
    provide_context=True,
    dag=dag
)



publication_by_user_task = PythonOperator(
    task_id='publication_by_user_task',
    python_callable=publication_by_user,
    provide_context=True,
    dag=dag, 
)

publication_by_hashtag_task = PythonOperator(
    task_id='publication_by_hashtag',
    python_callable=publication_by_hashtag,
    provide_context=True,
    dag=dag, 
)

publication_by_topic_task = PythonOperator(
    task_id='publication_by_topic',
    python_callable=publication_by_topic,
    provide_context=True,
    dag=dag, 
)

init_dl_task >> create_dw_task 
create_dw_task >> author_id_task >> insert_dw_task
create_dw_task >> sentiment_analysis_task >> insert_dw_task
create_dw_task >> hashtag_extraction_task >> insert_dw_task
create_dw_task >> topic_id_task >> insert_dw_task
insert_dw_task >>  publication_by_user_task

insert_dw_task >> publication_by_hashtag_task

insert_dw_task >> publication_by_topic_task
