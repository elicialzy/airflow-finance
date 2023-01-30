from DBHelper import DBHelper
from LDA_retrain import lda_dataprep
import boto3
from airflow.models import Variable
from airflow.operators.email_operator import EmailOperator
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from gensim.parsing.preprocessing import remove_stopwords
from gensim.models.coherencemodel import CoherenceModel
from gensim.models.ldamulticore import LdaMulticore
from gensim.models import CoherenceModel
import tqdm
import pandas as pd
import numpy as np
import gensim.corpora as corpora
import gensim
from textwrap import dedent
import snscrape.modules.twitter as sntwitter
import datetime as dt
import re
import string
import pickle
import nltk
nltk.download('wordnet')

############################################# SOCIAL MEDIA ###########################################


def twitter_scrape():
    # users
    users = ['CNBC', 'FinancialTimes', 'FinancialNews', 'markets', 'Stocktwits', 'nytimesbusiness',
             'WSJMarkets', 'IBDinvestors', 'bespokeinvest', 'MarketWatch', 'BusinessTimes', 'SGX', 'EDBsg',
             'jimcramer', 'mcuban', 'CathieDWood', 'elonmusk', 'chamath']

    # Write tweets into list
    lst = []
    for user in users:
        for i, tweet in enumerate(sntwitter.TwitterUserScraper(f'{user} lang:en since:{dt.datetime.today().strftime("%Y-%m-%d")} -filter:links -filter:replies').get_items()):
            lst.append({'tweet_id': tweet.id, 'date': tweet.date,
                       'content': tweet.content})

    db = DBHelper()

    # stored with historical data for LDA model training in mongodb
    db.insert_many_for_collection(
        'industry-qualitative-analysis', 'sm-raw-twitter-historical', lst)
    # stored for generating sentiments in current pipeline
    db.insert_many_for_collection(
        'industry-qualitative-analysis', 'sm-raw-twitter-current', lst)

############################################# LDA ###########################################


def lda_filtering():
    # get model
    bucketname = 'is3107-models'  # replace with your bucket name
    filename = 'ldav1.pkl'

    s3 = boto3.resource(
        service_name='s3',
        region_name='ap-southeast-1',
        aws_access_key_id='AKIAXL3EKMZMGCYQJBOJ',
        aws_secret_access_key='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'
    )
    obj = s3.Object(bucketname, filename).get()
    lda = pickle.load(obj['Body'])

    # get data
    db = DBHelper()
    collection = db.get_documents_for_collection(
        'industry-qualitative-analysis', 'sm-raw-twitter-current')
    maindf = pd.DataFrame(list(collection))

    # data formatting
    df = lda_dataprep(maindf)

    # get corpus
    text_data = df.content.apply(lambda x: [w for w in x.split()])
    dictionary = corpora.Dictionary(text_data)
    corpus = [dictionary.doc2bow(text) for text in text_data]

    # filter for topics
    train_vecs = []
    for i in range(len(df)):
        top_topics = (lda.get_document_topics(
            corpus[i], minimum_probability=0.0))
        topic_vec = [top_topics[i][1] for i in range(10)]
        train_vecs.append(topic_vec)

    for i in range(len(lda.show_topics())):
        label = str(i)
        df[label] = [train_vecs[x][i] for x in range(len(train_vecs))]

    df['topic'] = df.iloc[:, 3:].idxmax(axis=1)

    topics = ["0", "1", "2", "4", "6", "7"]
    # topics = [x.strip() for x in topics]
    df = df[df['topic'] in topics]
    maindf = maindf[maindf["tweet_id"] in df["tweet_id"].tolist()]

    # replace collection with filtered data
    db.delete_documents_for_collection(
        'industry-qualitative-analysis', 'sm-raw-twitter-current',)
    db.insert_many_for_collection(
        'industry-qualitative-analysis', 'sm-raw-twitter-current', maindf.to_dict('records'))

############################################# BERT ###########################################
# def get_sentiments()


############################################# DAG ###########################################
default_args = {
    'owner': 'wenqi',
    'depends_on_past': False,
    'email': ['lwenqi.wql@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': dt.timedelta(minutes=5)
}
industry_qualitative_analysis_dag = DAG(
    'industry_qualitative_analysis',
    description='Scrape qualitative industry data',
    schedule_interval=dt.timedelta(days=1),
    start_date=dt.datetime(2021, 1, 1),
    catchup=False,
    tags=['industry', 'qualitative'],
)

t1 = PythonOperator(
    task_id='twitter_scrape',
    python_callable=twitter_scrape,
    dag=industry_qualitative_analysis_dag
)

t2 = PythonOperator(
    task_id='lda_filtering',
    python_callable=lda_filtering,
    dag=industry_qualitative_analysis_dag
)

# t3 = PythonOperator(
#     task_id='get_sentiments',
#     python_callable=get_sentiments,
# dag = industry_qualitative_analysis_dag
# )

t1 >> t2  # >> t3
