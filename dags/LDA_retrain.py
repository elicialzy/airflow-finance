from DBHelper import DBHelper
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


def lda_dataprep(df):
    df = df.copy()
    # remove non-ascii characters
    df['content'] = df.content.apply(lambda x: x.encode('utf8'))
    df['content'] = df.content.apply(
        lambda x: re.sub(rb'[^\x00-\x7f]', rb' ', x))

    # lower case
    df['content'] = df['content'].astype(str)
    df['content'] = df.content.apply(lambda x: x.lower())

    # remove punctuation
    punct = set(string.punctuation)
    df['content'] = df.content.apply(lambda x: x.translate(
        str.maketrans(string.punctuation, ' '*len(string.punctuation))))

    # tokenize
    df['content'] = df.content.apply(lambda x: x.split())

    # remove integers
    df['content'] = df.content.apply(
        lambda x: [w for w in x if not w.isnumeric()])

    # remove stopwords
    df['content'] = df.content.apply(
        lambda x: [remove_stopwords(w) for w in x])

    # lemmatize
    wordnet_lemmatizer = nltk.WordNetLemmatizer()
    df['content'] = df.content.apply(
        lambda x: wordnet_lemmatizer.lemmatize(' '.join(x)))

    # remove single letter
    lst = []
    for content in df['content']:
        newstr = ''
        contentsplit = content.split(' ')
        for word in contentsplit:
            if len(word) > 1:
                newstr += ' ' + word
        lst.append(newstr)
    df['content'] = lst

    return df


def retrain_lda(ti):
    # get data
    db = DBHelper()
    collection = db.get_documents_for_collection(
        'industry-qualitative-analysis', 'sm-raw-twitter-historical')
    df = pd.DataFrame(list(collection))

    # data formatting
    df = lda_dataprep(df)

    # get corpus
    text_data = df.content.apply(lambda x: [w for w in x.split()])
    dictionary = corpora.Dictionary(text_data)
    corpus = [dictionary.doc2bow(text) for text in text_data]

    perp_values = []
    coh_values = []
    model_list = {}
    output = ''
    for num_topics in range(5, 30, 5):
        model = LdaMulticore(corpus, id2word=dictionary,
                             num_topics=num_topics, workers=5)
        perplexity = model.log_perplexity(corpus)
        model_list['topic_' + num_topics] = model
        perp_values.append(perplexity)
        cm = CoherenceModel(model=model, corpus=corpus, coherence='u_mass')
        coh_values.append(cm.get_coherence())
        output += '\nTopic: {}, Perplexity: {}\n'.format(
            num_topics, perplexity)
        output += 'Topic: {}, Coherence: {}\n'.format(
            num_topics, cm.get_coherence())
        output += model.print_topics()

    # need to push models to s3
    pickle_byte_obj = pickle.dumps(model_list)
    s3 = boto3.resource(
        service_name='s3',
        region_name='ap-southeast-1',
        aws_access_key_id='AKIAXL3EKMZMGCYQJBOJ',
        aws_secret_access_key='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'
    )
    s3.meta.client.upload_file(pickle_byte_obj, 'is3107-models', 'lda_models_' +
                               dt.datetime.today().strftime("%Y-%m-%d"))

    # push output to xcoms
    ti.xcom_push(key='lda_train_output_' +
                 dt.datetime.today().strftime("%Y-%m-%d"), value=output)
