from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator

import pandas as pd
import yfinance as yf
from urllib.request import Request, urlopen
import requests
import lxml
from functools import reduce
import json
import pandas as pd
import numpy as np
import random
import codecs
from DBHelper import DBHelper
import yahoo_fin.stock_info as si
import talib as ta


########################################### DB HELPER ###############################################

DATABASE_NAME = "quantitative-analysis"
COMPANY_PRICES = "company-prices"
COMPANY_RATIOS = "company-ratios"
COMPANY_TECH_INDICATORS = 'company-tech-indicators'
SECTOR_ETF_PRICES = "sector-etf-prices"
SECTOR_PERF_DATA = "sector-perf-data"
SECTOR_PERC_CHANGE = "sector-perc-change"

########################################### TARGET COMPANY ###############################################

def random_select_companies(**kwargs):
    companies_json = json.load(codecs.open('/home/airflow/3107-pipeline/static/companies.json', 'r', 'utf-8-sig'))
    companies_arr = companies_json['companies']

    selected_companies = []
    selected_tickers = []

    n = 10
    for i in range(n):
        index = int(random.random() * (len(companies_arr) - 1))
        if (companies_arr[index]['tradingName'] in selected_companies):
            n += 1
        else:
            # standardized all company names to lowercase
            selected_companies.append(companies_arr[index]['tradingName'].lower())
            # addded 'SI' to trading codes to access price data on yfinance
            selected_tickers.append(companies_arr[index]['tradingCode'] + ".SI")

    print(selected_companies)
    print(selected_tickers)

    kwargs['ti'].xcom_push(key='company_tickers', value=selected_tickers)

########################################### COMPANY STOCK PRICES ###############################################

def get_company_prices(**kwargs):
    company_prices = pd.DataFrame()
    all_company_prices = []

    selected_tickers=kwargs['ti'].xcom_pull(key='company_tickers', task_ids='random_select_companies')
    
    for ticker in selected_tickers:
        data = yf.download(ticker, period='1d').reset_index()
        data['Stock'] = ticker  # add this column because the dataframe doesn't contain a column with the ticker
        company_prices = company_prices.append(data)

    all_company_prices = company_prices.to_json(orient='records')
    all_company_prices = json.loads(all_company_prices)
    json.dumps(all_company_prices)

    print(all_company_prices)

    db = DBHelper()
    #push to temp storage
    db.insert_many_for_collection(DATABASE_NAME, COMPANY_PRICES, all_company_prices)

########################################### COMPANY FINANCIAL RATIOS ###############################################

def get_company_ratios(**kwargs):
    selected_tickers=kwargs['ti'].xcom_pull(key='company_tickers', task_ids='random_select_companies')

    # extracted relevant metrics for get_stats output
    metrics = ['Beta (5Y Monthly)', '52 Week High 3', 'Payout Ratio 4', 'Profit Margin', 'Return on Assets (ttm)', 
            'Return on Equity (ttm)', 'Gross Profit (ttm)', 'Gross Profit (ttm)', 'EBITDA',
            'Diluted EPS (ttm)', 'Quarterly Earnings Growth (yoy)', 'Total Debt/Equity (mrq)',
            'Current Ratio (mrq)', 'Book Value Per Share (mrq)', 'Operating Cash Flow (ttm)']
    all_ratios = pd.DataFrame()

    for ticker in selected_tickers:
        try:
            stats_val = si.get_stats_valuation(ticker)
            stats_val.columns = ["Metric", "Value"]
            stats_val['Stock'] = ticker
            
            all_ratios = all_ratios.append(stats_val, ignore_index=True)
            
        except Exception as ex:
            print('[Exception]', ex)
            print('Company Ratios Valuation: skipping this ticker')


        try:
            stats = si.get_stats(ticker)
            stats.columns = ["Metric", "Value"]
            stats = stats[stats['Metric'].isin(metrics)]
            stats['Stock'] = ticker
            
            all_ratios = all_ratios.append(stats, ignore_index=True)
        
        except Exception as ex:
            print('[Exception]', ex)
            print('Company Ratios Stats: skipping this ticker')

    all_ratios = all_ratios.reset_index()
    all_ratios = all_ratios.drop(columns=['index'])
    
    all_ratios = all_ratios.to_json(orient='records')
    all_ratios = json.loads(all_ratios)
    json.dumps(all_ratios)

    db = DBHelper()
    #push to temp storage
    db.insert_many_for_collection(DATABASE_NAME, COMPANY_RATIOS, all_ratios)


########################################### STOCKS TECHNICAL INDICATORS ###############################################

def get_company_tech_indicators(**kwargs): 
    selected_tickers=kwargs['ti'].xcom_pull(key='company_tickers', task_ids='random_select_companies')

    # get 1-year historical prices
    hist_company_prices = pd.DataFrame()
    
    for ticker in selected_tickers:
        data = yf.download(ticker, period='1y').reset_index()
        data['Stock'] = ticker  # add this column because the dataframe doesn't contain a column with the ticker
        hist_company_prices = hist_company_prices.append(data)

    # use 1-year historical prices to calculate technical indicators
    all_tech_indicators = pd.DataFrame()

    for ticker in selected_tickers:
        ticker_price_df = hist_company_prices[hist_company_prices['Stock'] == ticker]

        # overlap studies indicators
        ticker_price_df['50_Day_SMA'] = ta.SMA(ticker_price_df['Close'], timeperiod=50)
        ticker_price_df['50_Day_EMA'] = ta.EMA(ticker_price_df['Close'], timeperiod=50)
        ticker_price_df['50_Day_WMA'] = ta.WMA(ticker_price_df['Close'], timeperiod=50)
        ticker_price_df['upper_band'], ticker_price_df['middle_band'], ticker_price_df['lower_band'] = ta.BBANDS(ticker_price_df['Close'], timeperiod =50)
        ticker_price_df['SAR'] = ta.SAR(ticker_price_df['High'], ticker_price_df['Low'])

        # momentum indicators
        ticker_price_df['MACD'], ticker_price_df['MACD_signal'], ticker_price_df['MACD_hist']= ta.MACD(ticker_price_df['Close'], 
                                                                                                fastperiod=12, slowperiod=26, signalperiod=9)
        ticker_price_df['RSI'] = ta.RSI(ticker_price_df['Close'])
        ticker_price_df['AROON_down'], ticker_price_df['AROON_up'] = ta.AROON(ticker_price_df['High'], ticker_price_df['Low'])
        ticker_price_df['PPO'] = ta.PPO(ticker_price_df['Close'])
        ticker_price_df['ADX'] = ta.ADX(ticker_price_df['High'], ticker_price_df['Low'], ticker_price_df['Close'])
        ticker_price_df['CCI'] = ta.CCI(ticker_price_df['High'], ticker_price_df['Low'], ticker_price_df['Close'])

        # volume indicators 
        ticker_price_df['OBV'] = ta.OBV(ticker_price_df['Close'], ticker_price_df['Volume'])

        # volatility indicators
        ticker_price_df['ATR'] = ta.ATR(ticker_price_df['High'], ticker_price_df['Low'], ticker_price_df['Close'])

        all_tech_indicators = all_tech_indicators.append(ticker_price_df, ignore_index=True)
        # drop stock price and volume columns
        all_tech_indicators = all_tech_indicators.drop(columns=['Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume'])

    # extract the row with maximum dates for all stocks (i.e. today's technical indicators)        
    tech_indicators_today = all_tech_indicators[all_tech_indicators.groupby('Stock')['Date'].transform('max') == all_tech_indicators['Date']]

    tech_indicators_today = tech_indicators_today.to_json(orient='records')
    tech_indicators_today = json.loads(tech_indicators_today)
    json.dumps(tech_indicators_today)

    db = DBHelper()
    #push to temp storage
    db.insert_many_for_collection(DATABASE_NAME, COMPANY_TECH_INDICATORS, tech_indicators_today)

########################################### SECTOR ETF PRICES ###############################################

# define sector + ETF ticker
etf_sectors = {'Energy': 'XLE', 
                'Basic Materials': 'XLB',
                'Industrial Goods': 'XLI',
                'Consumer Cyclical': 'XLY',
                'Consumer Defensive': 'XLP',
                'Healthcare': 'XLV',
                'Financial': 'XLF',
                'Technology': 'SMH',
                'Communication Services': 'XTL',
                'Utilities': 'XLU',
                'Real Estate': 'VNQ'}

# convert sector + ETF ticker dictionary to dataframe
sector_df = pd.DataFrame.from_dict(etf_sectors, orient='index').reset_index()
sector_df.columns = ['sector', 'ETF']

def get_etf_prices(**kwargs):
    etf_prices = pd.DataFrame()
    all_etf_prices = []
    
    for ticker in etf_sectors.values():
        data = yf.download(ticker, period='1d').reset_index()
        data['ETF'] = ticker  # add this column because the dataframe doesn't contain a column with the ticker
        etf_prices = etf_prices.append(data)

    all_etf_prices = etf_prices.to_json(orient='records')
    all_etf_prices = json.loads(all_etf_prices)
    json.dumps(all_etf_prices)

    print(all_etf_prices)

    db = DBHelper()
    #push to temp storage
    db.insert_many_for_collection(DATABASE_NAME, SECTOR_ETF_PRICES, all_etf_prices)


########################################### DAILY SECTOR PERFORMANCE DATA (UP, DOWN, CONSTANT) ###############################################

def get_sector_perf_data(**kwargs): 
    clickapi_API_KEY = '6263c6e9033f2ff246ba69d4e08b2aa3'
    sectorperf_df = pd.DataFrame()

    #sectorperf_url = "https://www.clickapis.com/api/v1/sectors_momentum?date=" + datetime.today().strftime('%Y-%m-%d') + "&api_key=" + clickapi_API_KEY 
    sectorperf_url = "https://www.clickapis.com/api/v1/sectors_momentum?date=2022-04-06&api_key=6263c6e9033f2ff246ba69d4e08b2aa3" 
    sectorperf_data = requests.get(sectorperf_url).json()

    for i in range(0, len(sectorperf_data['results'])):
        sectorperf_dict = {'sector': sectorperf_data['results'][i]['sector'],
                            'advancing': sectorperf_data['results'][i]['advancing'],
                            'declining': sectorperf_data['results'][i]['declining'],     
                            'unchanged': sectorperf_data['results'][i]['unchanged'],     
        }
        sectorperf_df = sectorperf_df.append(sectorperf_dict, ignore_index=True)
    
    # only include sectors defined in sector_df 
    sectorperf_df = sectorperf_df[sectorperf_df['sector'].isin(list(etf_sectors.keys()))]

    sectorperf_df = sectorperf_df.to_json(orient='records')
    sectorperf_df = json.loads(sectorperf_df)
    json.dumps(sectorperf_df)

    print(sectorperf_df)

    db = DBHelper()
    # push to temp storage
    db.insert_many_for_collection(DATABASE_NAME, SECTOR_PERF_DATA, sectorperf_df)

########################################### SECTOR PERFORMANCE PERCENTAGE CHANGE DATA ###############################################

# i used up my free API, will create a new account again cuz there's a limit as to how many API calls you can make for sectors performance
# def get_sector_perc_change(**kwargs):
#     fmp_API_KEY = '54be86c7c28fa7d80203ecb46a6a2c44'
#     url = 'https://financialmodelingprep.com/api/v3/stock/sectors-performance?apikey=' + fmp_API_KEY
#     sector_per_request = requests.get(url)
#     sector_per_response = sector_per_request.json()

#     # update sector names to be the same as previous functions
#     sector_perc_change = pd.DataFrame(sector_per_response['sectorPerformance'])
#     sector_perc_change.loc[len(sector_perc_change)] = ['Industrial Goods',
#                         sector_perc_change.loc[sector_perc_change.sector == 'Industrials', 'changesPercentage'].values[0]]
#     sector_perc_change = sector_perc_change.drop(sector_perc_change[sector_perc_change.sector == 'Industrials'].index)

#     sector_perc_change.loc[len(sector_perc_change)] = ['Financial',
#                         sector_perc_change.loc[sector_perc_change.sector == 'Financial Services', 'changesPercentage'].values[0]]
#     sector_perc_change = sector_perc_change.drop(sector_perc_change[sector_perc_change.sector == 'Financial Services'].index)
    
#     sector_perc_change = sector_perc_change.to_json(orient='records')
#     sector_perc_change = json.loads(sector_perc_change)
#     json.dumps(sector_perc_change)

#     print(sector_perc_change)

#     db = DBHelper()
#     #push to temp storage
#     db.insert_many_for_collection(DATABASE_NAME, SECTOR_PERC_CHANGE, sector_perc_change)

########################################### DAG ###############################################

default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'email': ['elicialow99@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'provide_context': True
}

quantitative_analysis_dag = DAG(
    'quantitative_analysis',
    default_args=default_args,
    description='company quantitative analysis',
    # schedule_interval=timedelta(minutes=1),
    schedule_interval="@daily",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['company_quant']
)

select_random_companies = PythonOperator(
    task_id='random_select_companies',
    python_callable=random_select_companies,
    dag=quantitative_analysis_dag
)

get_company_prices = PythonOperator(
    task_id='get_company_prices',
    python_callable=get_company_prices,
    dag=quantitative_analysis_dag
)

get_etf_prices = PythonOperator(
    task_id='get_etf_prices',
    python_callable=get_etf_prices,
    dag=quantitative_analysis_dag
)

get_sector_perf_data = PythonOperator(
    task_id='get_sector_perf_data',
    python_callable=get_sector_perf_data,
    dag=quantitative_analysis_dag
)

# get_sector_perc_change = PythonOperator(
#     task_id='get_sector_perc_change',
#     python_callable=get_sector_perc_change,
#     dag=quantitative_analysis_dag
# )

get_company_ratios = PythonOperator(
    task_id='get_company_ratios',
    python_callable=get_company_ratios,
    dag=quantitative_analysis_dag
)

get_company_tech_indicators = PythonOperator(
    task_id='get_company_tech_indicators',
    python_callable=get_company_tech_indicators,
    dag=quantitative_analysis_dag
)


select_random_companies >> get_company_prices
select_random_companies >> get_company_ratios
select_random_companies >> get_company_tech_indicators

get_etf_prices
get_sector_perf_data