from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.bash import BashOperator
from numpy import False_

import requests
import random
import json
import codecs
from DBHelper import DBHelper
import snscrape.modules.twitter as sntwitter
import snscrape.modules.telegram as snTele
from nltk.sentiment import SentimentIntensityAnalyzer
import pytz
import random
import json
from pytrends.request import TrendReq
import codecs
from nltk.sentiment import SentimentIntensityAnalyzer
import social_media_spam_classifier as SocialMediaSpamClassifier


########################################### DB HELPER ###############################################

DATABASE_NAME = "qualitative-analysis"
SM_RAW_TWITTER="sm-raw-twitter"
SM_RAW_TELEGRAM="sm-raw-telegram"
SM_FILTERED_TWITTER="sm-filtered-twitter"
SM_FILTERED_TELEGRAM="sm-filtered-telegram"
SM_COMBINED_PROCESSED="sm-combined-processed"
NEWS_API='news-newsapi'
MEDIASTACK='news-mediastack'
NEWS_COMBINED='news-combined'
NEWS_SENTIMENTS='news-sentiments'
SOCIAL_MEDIA_SENTIMENTS='sm-sentiments'
COMPANY_TRENDS = 'company-trends'
COMBINED_SENTIMENTS = 'combined-sentiments'

########################################### TARGET COMPANY ###############################################

def random_select_companies(**kwargs):
    companies_json = json.load(codecs.open('/home/airflow/3107-pipeline/static/companies.json', 'r', 'utf-8-sig'))
    companies_arr = companies_json['companies']

    selected_companies = []

    n = 10
    for i in range(n):
        index = int(random.random() * (len(companies_arr) - 1))
        if (companies_arr[index]['tradingName'] in selected_companies):
            n += 1
        else:
            # standardized all company names to lowercase
            selected_companies.append(companies_arr[index]['tradingName'].lower())

    print(selected_companies)
    kwargs['ti'].xcom_push(key='company_names', value=selected_companies)

########################################### NEWS ###############################################

def get_newsapi_api(**kwargs):
    selected_companies=kwargs['ti'].xcom_pull(key='company_names', task_ids='random_select_companies')
    company_news = []
    newsapi_apikey = 'd315c85053c24c79a7f09bf6a694b125'
    selected_companies = [selected_companies[0]] #just to reduce api calls for testing, delete when done
    for company in selected_companies:
        response = requests.get(f'https://newsapi.org/v2/everything?q={company}&language=en&apiKey={newsapi_apikey}')
        
        if response.status_code == 200:
            data = response.json()
            data['company'] = company
            company_news.append(data)
        else:
            handle_error(response)
    
    db = DBHelper()
    #push to temp storage
    db.insert_many_for_collection(DATABASE_NAME, NEWS_API, company_news)


def get_mediastack_api(**kwargs):
    selected_companies=kwargs['ti'].xcom_pull(key='company_names', task_ids='random_select_companies')
    company_news = []
    mediastack_apikey ='a2e70c5f7d0de01c5f73af203d2d70e5'
    selected_companies = [selected_companies[0]] #just to reduce api calls for testing, delete when done
    for company in selected_companies:
        response = requests.get(f'http://api.mediastack.com/v1/news?access_key={mediastack_apikey}&languages=en&keywords={company}')

        if response.status_code == 200:
            data = response.json()
            data['company'] = company
            company_news.append(data)
        else:
            handle_error(response)

    if len(company_news) > 0:
        db = DBHelper()
        db.insert_many_for_collection(DATABASE_NAME, MEDIASTACK, company_news)


def handle_error(response):
    #do some error handling here?
    print('error!')


def standardise_news_data():
    db = DBHelper()

    newsapi_documents = db.get_documents_for_collection(DATABASE_NAME, NEWS_API)
    mediastack_documents = db.get_documents_for_collection(DATABASE_NAME, MEDIASTACK)

    all_company_news = []

    for i in range(len(newsapi_documents)):
        newsapi_company_news=newsapi_documents[i]
        mediastack_company_news=mediastack_documents[i]
        consolidated_articles = []
        for article in newsapi_company_news["articles"]:
            standardised_details = {
                "author": article["author"],
                "title": article["title"],
                "description": article["description"],
                "source": article["source"]["name"]
            }
            consolidated_articles.append(standardised_details)

        for article in mediastack_company_news["data"]:
            standardised_details = {
                "author": article["author"],
                "title": article["title"],
                "description": article["description"],
                "source": article["source"]
            }
            consolidated_articles.append(standardised_details)

        all_company_news.append({
            "company": newsapi_company_news["company"],
            "news": consolidated_articles
        })

    if len(all_company_news) > 0:
        db.insert_many_for_collection(DATABASE_NAME, NEWS_COMBINED, all_company_news)


def news_sentiment_analysis():
    sia = SentimentIntensityAnalyzer()
    db = DBHelper()

    news_combined = db.get_documents_for_collection(DATABASE_NAME, NEWS_COMBINED)

    company_sentiments = []

    for company in news_combined:
        del company["_id"]
        neg = 0
        neu = 0
        pos = 0
        compound = 0
        for news in company["news"]:
            if news["description"] is not None:
                score = sia.polarity_scores(news["description"])
                neg = neg + score["neg"]
                neu = neu + score["neu"]
                pos = pos + score["pos"]
                compound = compound + score["compound"]
        
        number_of_news = len(company["news"])
        if (number_of_news > 0):
            company["neg_news_sentiments"] = neg / number_of_news
            company["pos_news_sentiments"] = pos / number_of_news
            company["neu_news_sentiments"] = neu / number_of_news
            company["compound_news_sentiments"] = compound / number_of_news
            company['number_relevant_news'] = number_of_news
        else:
            company["neg_news_sentiments"] = 0
            company["pos_news_sentiments"] = 0
            company["neu_news_sentiments"] = 0
            company["compound_news_sentiments"] = 0
            company['number_relevant_news'] = number_of_news
        company_sentiments.append(company)

    if len(company_sentiments) > 0:
        db.insert_many_for_collection(DATABASE_NAME, NEWS_SENTIMENTS, company_sentiments)

def social_media_sentiments():
    sia = SentimentIntensityAnalyzer()
    db = DBHelper()
    twitter = db.get_documents_for_collection(DATABASE_NAME, SM_FILTERED_TWITTER)
    telegram = db.get_documents_for_collection(DATABASE_NAME, SM_FILTERED_TELEGRAM)

    social_media = twitter + telegram

    social_media_sentiment = []

    for document in social_media:
        if document['content'] is not None:
            score = sia.polarity_scores(document["content"])
            del document['_id']
            document['neg_news_sentiments'] = score['neg']
            document['pos_news_sentiments'] = score['pos']
            document['neu_news_sentiments'] = score['neu']
            document['compound_news_sentiments'] = score['compound']
            document['number_relevant_tweets'] = len(twitter)
            document['number_telegram_messages'] = len(telegram)
            social_media_sentiment.append(document)
    
    if len(social_media_sentiment) > 0:
        db.insert_many_for_collection(DATABASE_NAME, SOCIAL_MEDIA_SENTIMENTS, social_media_sentiment)

############################################# PYTRENDS ###########################################

def retrieve_trends(**kwargs):
    selected_companies=kwargs['ti'].xcom_pull(key='company_names', task_ids='random_select_companies')

    pytrends = TrendReq(hl='en-US', tz=480)

    trends = []
    for i in range(len(selected_companies)):
        pytrends.build_payload([selected_companies[i]], cat=16, timeframe='now 1-d', geo='SG')
        interests = pytrends.interest_over_time() #returns a dataframe

        if (len(interests) > 0):
            interests_list =  interests.iloc[:,0].to_numpy().tolist()
            average = sum(interests_list) / len(interests_list)
        else:
            interests_list = []
            average = 0

        trends.append({
            "company": selected_companies[i],
            "daily_interests": interests_list,
            "average": average
        })
    
    if (len(trends) > 0):
        db = DBHelper()
        db.insert_many_for_collection(DATABASE_NAME, COMPANY_TRENDS, trends)
    
############################################# SOCIAL MEDIA ###########################################

def get_tweets(**kwargs):
    companies=kwargs['ti'].xcom_pull(key='company_names', task_ids='random_select_companies')
    # companies = ['alibaba', 'paypal', 'tencent']

    search_string = companies[0]
    for i, name in enumerate(companies):
        if i == 0:
            pass
        else:
            search_string = search_string + " OR " + name

    tweets = []
    for i,tweet in enumerate(sntwitter.TwitterSearchScraper(f"{search_string} since:{datetime.today().strftime('%Y-%m-%d')} lang:en").get_items()):
        # for testing, only store 50
        if i>50:
            break
        
        for company in companies:
            if company in tweet['content'].lower():
                tweet_dict = {}
                tweet_dict['date'] = tweet.date
                tweet_dict['content'] = tweet.content.lower()
                tweet_dict['company'] = company
                tweets.append(tweet_dict)
    
    if len(tweets) > 0:
        dbHelper = DBHelper()
        dbHelper.insert_many_for_collection(DATABASE_NAME, SM_RAW_TWITTER, tweets)

def get_telegram_messages(**kwargs):
    companies=kwargs['ti'].xcom_pull(key='company_names', task_ids='random_select_companies')
    # companies = ['alibaba', 'paypal', 'tencent']
    telegram_groups = ['Bloomberg', 'SGX Invest', 'SG Market Updates', 'Business & Finance News USA Edition', 'The Real Rayner Teo', 'Seedly Personal Finance SG']

    def scrape_telegram_for_channel(channel):
        messages = []
        for i, message in enumerate(snTele.TelegramChannelScraper(channel).get_items()):
            if i > 50:
                break
            if message.date < datetime.now(tz=pytz.UTC):
                break
            if not message.content:
                continue

            # filter messages that do not mention the company name
            for company in companies:
                if company in message.content.lower():
                    message_dict = {}
                    message_dict['date'] = message.date
                    message_dict['content'] = message.content.lower()
                    message_dict['company'] = company
                    messages.append(message_dict)
                    break

        return messages

    combined_messages = []
    for group in telegram_groups:
        combined_messages = combined_messages + scrape_telegram_for_channel(group)

    if len(combined_messages) > 0:
        dbHelper = DBHelper()
        dbHelper.insert_many_for_collection(DATABASE_NAME, SM_RAW_TELEGRAM, combined_messages)


def filter_twitter_spams():
    dbHelper = DBHelper()

    twitter_documents = dbHelper.get_documents_for_collection(DATABASE_NAME, SM_RAW_TWITTER)
    
    for document in twitter_documents:
        del document["_id"]
    
    # filter out spams
    filter_sm_documents = [post for post in twitter_documents if SocialMediaSpamClassifier.classify_spam(post['content']) == 1]

    if len(filter_sm_documents) > 0:
        dbHelper.insert_many_for_collection(DATABASE_NAME, SM_FILTERED_TWITTER, filter_sm_documents)


def filter_telegram_spams():
    dbHelper = DBHelper()

    telegram_documents = dbHelper.get_documents_for_collection(DATABASE_NAME, SM_RAW_TELEGRAM)
    
    for document in telegram_documents:
        del document["_id"]
    
    # filter out spams
    filter_sm_documents = [post for post in telegram_documents if SocialMediaSpamClassifier.classify_spam(post['content']) == 1]

    if len(filter_sm_documents) > 0:
        dbHelper.insert_many_for_collection(DATABASE_NAME, SM_FILTERED_TELEGRAM, filter_sm_documents)


################################### COMBINE SENTIMENTS #################################

def combine_sentiments():
    db = DBHelper()
    sm_sentiments = db.get_documents_for_collection(DATABASE_NAME, SOCIAL_MEDIA_SENTIMENTS)
    news_sentiments = db.get_documents_for_collection(DATABASE_NAME, NEWS_SENTIMENTS)
    company_trends = db.get_documents_for_collection(DATABASE_NAME, COMPANY_TRENDS)

    combined_sentiments = []

    for news_company in news_sentiments:
        match=False
        for sm_company in sm_sentiments:
            if sm_company['company'] == news_company['company']:
                match=True
                combined_sentiments.append({
                    'company': sm_company['company'],
                    'pos_combined_sentiments': (sm_company['pos_news_sentiments'] + news_company['pos_news_sentiments']) / 2,
                    'neu_combined_sentiments': (sm_company['neu_news_sentiments'] + news_company['neu_news_sentiments']) / 2,
                    'neg_combined_sentiments': (sm_company['neg_news_sentiments'] + news_company['neg_news_sentiments']) / 2,
                    'compound_combined_sentiments': (sm_company['compound_news_sentiments'] + news_company['compound_news_sentiments']) / 2,
                })
        if match==False:
            combined_sentiments.append({
                'company': news_company['company'],
                'pos_combined_sentiments': news_company['pos_news_sentiments'],
                'neu_combined_sentiments': news_company['neu_news_sentiments'],
                'neg_combined_sentiments': news_company['neg_news_sentiments'],
                'compound_combined_sentiments': news_company['compound_news_sentiments'],
            })

    for company in combined_sentiments:
        match=False
        for company_trend in company_trends:
            if company['company'] == company_trend['company']:
                match=True
                company['weighted_sentiments'] = company['compound_combined_sentiments'] * company_trend['average']

        if match==False:
            company['weighted_sentiments'] = 0

    if len(combine_sentiments) > 0:
        db.insert_many_for_collection(DATABASE_NAME, COMBINED_SENTIMENTS, combined_sentiments)
        
########################################### LOAD TO BIGQUERY ###########################################

# extract and transform required collections to store under tmp/{DATABASE_NAME}
def extract_collections_to_json():
    db = DBHelper()
    #TODO: enter the list of collections to extract
    collections_to_extract = []
    
    for collection in collections_to_extract:
        db.export_collection_to_json(DATABASE_NAME, collection)
    
# upload all the extract json files to GCS
load_to_gcs = BashOperator(
    task_id='load_to_gcs', 
    bash_command="/home/airflow/3107-pipeline/tmp/load_gcs.sh "
)

# load json files from GCS to BigQuery and create the tables
load_to_bq = BashOperator(
    task_id='load_to_bq', 
    bash_command="/home/airflow/3107-pipeline/tmp/load_bq.sh "
)

############################################# DAG ###########################################

default_args = {
    'owner': 'kiyong',
    'depends_on_past': False,
    'email': ['angkiyong@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

qualitative_analysis_dag = DAG(
    'qualitative_analysis',
    default_args=default_args,
    description='news',
    # schedule_interval=timedelta(minutes=1),
    schedule_interval="@daily",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['news']
)

select_random_companies = PythonOperator(
    task_id='random_select_companies',
    python_callable=random_select_companies,
    dag=qualitative_analysis_dag
)

retrieve_newsapi_api_2 = PythonOperator(
    task_id='retrieve_newsapi_api_2',
    python_callable=get_newsapi_api,
    dag=qualitative_analysis_dag
)

retrieve_mediastack_api_2 = PythonOperator(
    task_id='retrieve_mediastack_api_2',
    python_callable=get_mediastack_api,
    dag=qualitative_analysis_dag
)

retrieve_tweets = PythonOperator(
    task_id='retrieve_tweets',
    python_callable=get_tweets,
    dag=qualitative_analysis_dag
)

retrieve_telegram_messages = PythonOperator(
    task_id='retrieve_telegram_messages',
    python_callable=get_telegram_messages,
    dag=qualitative_analysis_dag
)

standardise_news = PythonOperator(
    task_id='standardise_news',
    python_callable=standardise_news_data,
    dag=qualitative_analysis_dag
)

calculate_news_sentiments = PythonOperator(
    task_id='calculate_news_sentiments',
    python_callable=news_sentiment_analysis,
    dag=qualitative_analysis_dag
)

retrieve_company_trends = PythonOperator(
    task_id='retrieve_company_trends',
    python_callable=retrieve_trends,
    dag=qualitative_analysis_dag
)

filter_raw_twitter_spams = PythonOperator(
    task_id='filter_raw_twitter_spams',
    python_callable=filter_twitter_spams,
    dag=qualitative_analysis_dag
)

filter_raw_telegram_spams = PythonOperator(
    task_id='filter_raw_telegram_spams',
    python_callable=filter_telegram_spams,
    dag=qualitative_analysis_dag
)

retrieve_social_media_sentiments = PythonOperator(
    task_id='retrieve_social_media_sentiments',
    python_callable=social_media_sentiments,
    dag=qualitative_analysis_dag
)

combine_all_sentiments = PythonOperator(
    task_id='combine_all_sentiments',
    python_callable=combine_sentiments,
    dag=qualitative_analysis_dag
)

select_random_companies >> [retrieve_mediastack_api_2, retrieve_newsapi_api_2] >> standardise_news >> calculate_news_sentiments
retrieve_company_trends
select_random_companies >> [retrieve_tweets, retrieve_telegram_messages] >>  [filter_raw_telegram_spams, filter_raw_twitter_spams] >> retrieve_social_media_sentiments
[calculate_news_sentiments, retrieve_social_media_sentiments] >> combine_all_sentiments