#!/usr/bin/env python
# coding: utf-8

# In[18]:


import requests
import pandas as pd
import bs4
import urllib.parse
import pymongo
import json
import lxml.html
from alpha_vantage.timeseries import TimeSeries
import matplotlib.pyplot as plt
from sqlalchemy import create_engine


###### EXTRACT #######

#web-scrape list of stock tickers from nasdaq100
url = 'https://www.nasdaq.com/market-activity/quotes/Nasdaq-100-Index-Components'
response = requests.get(url)
soup = bs4.BeautifulSoup(response.text,'html.parser')
stocks = []
for x in range(100):
    stock = soup.find('td',attrs={'class':'row_{} col_1'.format(x+1)}).a.get('href')
    ticker = stock.replace('/market-activity/stocks/','')
    stocks.append(ticker)
print(stocks)

#get latest stock price from Alpha Vantage API
ts = TimeSeries(key='IPVCAANNDGK60RBH', output_format='pandas')
stockprice=[]
for x in range(5): #due to free API key's limit of 5 calls per minute, we can't loop through all tickers at once
    data, meta_data = ts.get_intraday(symbol=stocks[x],interval='60min', outputsize='full')
    stockprice.append(data.iloc[0][3])
print(stockprice)

#use NewsAPI to get stock news based on ticker found above
stocks_df = pd.DataFrame(stocks)
stocks_df.head()
base_url = 'https://newsapi.org/v2/everything?'
api_key = "apiKey=17454b732308403bac81dfc6f8f2bf48"
mydict = {}
for index, row in stocks_df.head().iterrows():  #create a dictionary
   s_word = (row[0])
   search ='&q=' + str(s_word)
   d_word = '2019-11-11'
   date = '&from' +d_word
   r_json = requests.get("{}{}{}{}".format(base_url,api_key,search,date)).json()
   mydict[s_word] = pd.read_json(json.dumps(r_json['articles'], indent=4))

    
###### TRANSFORM #######

#combine ticker with their corresponding price into a single DF
shorterstocks=stocks[0:5]
tickerprice={'Ticker':shorterstocks,'Price':stockprice}
stockdf = pd.DataFrame(tickerprice)
stockdf
    
AAPL = pd.DataFrame(mydict['AAPL']) #create DF for each stock ticker
AAPL.insert(0, "Ticker", "AAPL")  #insert 'ticker' as the first column
print(AAPL) #sample DF

MSFT = pd.DataFrame(mydict['MSFT'])
MSFT.insert(0, "Ticker", "MSFT")

AMZN = pd.DataFrame(mydict['AMZN'])
AMZN.insert(0, "Ticker", "AMZN")

FB = pd.DataFrame(mydict['FB'])
FB.insert(0, "Ticker", "FB")

GOOG = pd.DataFrame(mydict['GOOG'])
GOOG.insert(0, "Ticker", "GOOG")

final = pd.concat([AAPL, MSFT, AMZN, FB, GOOG]) #Merge all individual stock DF into a single DF
load = pd.merge(stockdf,final,on="Ticker") #Merge stock price with News Info


###### LOAD #######

#CSV version since I fail to install psycopg2 to export to postgres
exportcsv = load.to_csv('etlproject.csv',index=None) 
#if psycopg2 was successfully installed: 
#engine = create_engine('postgresql://postgres:postgres@localhost:5433/etlproject')
#df.to_sql(etlproject, engine)


# In[ ]:




