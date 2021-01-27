# -*- coding: utf-8 -*-
"""
Created on Tue Jan 26 21:08:52 2021

@author: James Huang
"""
import pandas as pd 
from datetime import * 
import os
import shutil
import yfinance as yf

# Create a working directory
today = datetime.today()
year = today.isocalendar()[0]
wk = today.isocalendar()[1]

path = 'C:\\Stock_Options\\Data_Download\\'
folderName = str(year) + '_wk' + str(wk)
dataFolderName = path + folderName


if os.path.exists(dataFolderName):
    #os.rmdir(dataFolderName)
    shutil.rmtree(dataFolderName)

os.system('mkdir ' + dataFolderName)

#Download weekly historical price from yahoo finance
#Data download parameters
dataPeriod = '4d'

## Current list
#ticker_list = [ '^ixic','^dji', 'tsm','tsla','aapl','amzn','nflx','nvda','fb','baba','nio','qqq','spy', 'arkk','eem', 'gld','snow','sq', \
#                'crm','amd', 'msft', 'uber', 'zm', 'slv', 'arkg', 'xle','xlf', 'xlv', 'xlk', 'lmnd', 'mrna', 'bynd', 'pltr', \
#                'bidu','v','abnb','li', 'ba', 'iwm', 'dash', 'arkq', 'gme','bb']

## Add new list 
ticker_list = ['gme', 'bb']

for ticker in ticker_list:
    # Download stock data then export as CSV for each ticker
    #########################################################
    #--------------------------------------------------------
    ## Prior week
    ##data_df = yf.download(ticker, period= dataPeriod) #5 day historical price
    
    ## Date Range
    data_df = yf.download(ticker, start="2000-01-01", end = date.today()) #end="2020-12-31") # Date Range
    #--------------------------------------------------------
    #########################################################
    
    data_csv_fname = dataFolderName + '\\' + ticker + '.csv'
    data_df.to_csv(data_csv_fname)
    
    #insert ticker to first column
    data = pd.read_csv(data_csv_fname)  
    idx = 0  
    data.insert(loc=idx, column='Ticker', value=ticker.lower())
    data.to_csv(data_csv_fname,  index=False) 
    
#Merge downloaded csv files to 1 csv and load into panda dataframe:
import glob

path = dataFolderName + '\\' # use your path
outFile = path + 'Final.csv'
if os.path.exists(outFile):
    os.remove(outFile)

all_files = glob.glob(path + "*.csv")

li = []

for filename in all_files:
    df = pd.read_csv(filename, index_col=None, header=0)
    li.append(df)

frame = pd.concat(li, axis=0, ignore_index=True)
frame.to_csv(outFile ,  index=False )

#Upload to PostgreSQL
# Imports
from sqlalchemy import create_engine

# This CSV doesn't have a header so pass
# column names as an argument
columns = [
    "ticker",
    "tradedate",
    "openprice",
    "high",
    "low",
    "closeprice",
    "adjclose",
    "volume" 
]

# Instantiate sqlachemy.create_engine object
engine = create_engine('postgresql://database:123456@localhost:32/cstu')

# Create an iterable that will read "chunksize=50000" rows
# at a time from the CSV file
try:
    # code
    for df in pd.read_csv(outFile,names=columns,chunksize=50000):    # add columns name from stockprice table
        df = df.drop(df.index[0])  # Drop csv header row
        df.to_sql(
        'stockprice', 
        engine,
        index=False,
        chunksize=50000,
        method='multi',
        if_exists='append' # if the table already exists, append this data
        )
except exc.DBAPIError as err:
    print (err)
except exc.SQLAlchemyError as e:
    print (f'SQLAlchemy Connection Error occured! {e} {self.uri}')
    raise RuntimeError(f'SQLAlchemy Connection Error occured! {e} {self.uri}')
except Exception as ex:
    print('Connection Error occured!', ex)
else:
    print('Historical Storck Price loaded successfully!')


