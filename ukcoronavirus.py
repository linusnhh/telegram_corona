from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
import tweepy
import json
import datetime
import pandas as pd
import numpy as np
import csv
import re
from textblob import TextBlob
import schedule
import string
import preprocessor as p
import os
import time
import nltk
import matplotlib
from twitter_scrape import TweetMiner
from datetime import date
import warnings
import requests

warnings.filterwarnings('ignore')

miner = TweetMiner(result_limit = 999) #Mine tweet. 
mined_tweets = miner.mine_user_tweets(user='DHSCgovUK', max_pages=10)
mined_tweets_df= pd.DataFrame(mined_tweets)

virus_df = mined_tweets_df[mined_tweets_df['text'].str.contains("died")] # Only include coronavirus update  
virus_df = virus_df[['screen_name', 'created_at', 'text']].reset_index(drop=True) # Keep relevant columns. 
virus_df['date'] = virus_df.created_at.dt.strftime('%Y-%m-%d') # Get date. 
virus_df['time'] = virus_df.created_at.dt.strftime('%H:%M:%S') # Get time. 

update_df = virus_df.loc[(virus_df['date'] > '2020-04-05')] # Work on the latest format.
update_df['test_cum'] = update_df.text.str.findall('\d+(?:,\d+)?').str[2] # Toral number 
update_df['test_daily'] = update_df.text.str.findall('\d+(?:,\d+)?').str[3]
update_df['ppl_test_cum'] = update_df.text.str.findall('\d+(?:,\d+)?').str[5]
update_df['ppl_confirmed_cum'] = update_df.text.str.findall('\d+(?:,\d+)?').str[6]
update_df['death_cum'] = update_df.text.str.findall('\d+(?:,\d+)?').str[9]

vis_df = update_df.drop(columns=['text', 'created_at', 'time'])
vis_df= vis_df.stack().str.replace(',', '').unstack()
vis_df = vis_df.apply(pd.to_numeric, errors='ignore')
vis_df['ppl_tested_daily'] = vis_df.ppl_test_cum.diff(periods = -1).fillna(0).astype(np.int64)
vis_df['death_daily'] = vis_df.death_cum.diff(periods=-1).fillna(0).astype(np.int64)
vis_df['ppl_confirmed_daily'] = vis_df.ppl_confirmed_cum.diff(periods = -1).fillna(0).astype(np.int64)
vis_df['ppl_percentage_daily'] = vis_df['ppl_confirmed_daily']/vis_df['ppl_tested_daily']
vis_df['death_rate'] = vis_df['death_cum']/vis_df['ppl_confirmed_cum']
vis_df['ppl_confirmed_change'] = vis_df.ppl_confirmed_daily.pct_change(periods=-1)
vis_df['death_change'] = vis_df.death_daily.pct_change(periods=-1)
vis_df['ppl_tested_change'] = vis_df.ppl_tested_daily.pct_change(periods=-1)

latest_date = vis_df.date.iloc[0]
yesterday = vis_df.date.iloc[1]
ppl_test_cum = format(vis_df.ppl_test_cum.iloc[0], ',')
ppl_confirmed_daily = format(vis_df.ppl_confirmed_daily.iloc[0], ',')
ppl_tested_daily = format(vis_df.ppl_tested_daily.iloc[0], ',')
yesterday_cases = format(vis_df.ppl_confirmed_daily.iloc[1], ',')
ppl_confirmed_change = round(vis_df.ppl_confirmed_change.iloc[0]*100, 1)
ppl_tested_change = round(vis_df.ppl_tested_change.iloc[0]*100, 1)
death_daily = format(vis_df.death_daily.iloc[0], ',')
death_rate = round(vis_df.death_rate.iloc[0]*100, 1)
death_cum = format(vis_df.death_cum.iloc[0], ',')
death_change = round(vis_df.death_change.iloc[0]*100, 1)

a = 'On {0}, {1} people are tested, a {2}% change.'.format(latest_date, ppl_tested_daily, ppl_tested_change)
b = ' So far, {0} people have been tested of which {1} tested positive today.'.format(ppl_test_cum, ppl_confirmed_daily)
c = ' There is a {0}% change in confirmed cases, comparing to {1} cases yesterday.'.format(ppl_confirmed_change, yesterday_cases)
d = ' Death toll increased by {0} to {1}, representing a {2}% change. The death rate is {3}%.'.format(death_daily, death_cum, death_change,death_rate)
e = a+b+c+d
print (e)

def telegram_bot_sendtext(bot_message):
        bot_token = '1157266597:AAEAEJN67IFaAHuCpl43dqCg_GlBq2xKPyo'
        bot_chatID = '@ukcoronavirusupdate'
        send_text = 'https://api.telegram.org/bot' + bot_token + '/sendMessage?chat_id=' + bot_chatID + '&parse_mode=Markdown&text=' + bot_message
        response = requests.get(send_text)

output_df = pd.read_csv('/Users/linusnhh/Desktop/local/Python/twitter/project_tweet/output/uk_coronavirus_stats.csv')
if (date.today().strftime('%Y-%m-%d') == latest_date) == False:
    print ('Official data has not been updated yet')
if (date.today().strftime('%Y-%m-%d') != output_df.date.iloc[0]) == True: 
    print ('Latest data has already been written.')
if date.today().strftime('%Y-%m-%d') == latest_date and date.today().strftime('%Y-%m-%d') != output_df.date.iloc[0]:
    print('Sending message...')
    telegram_bot_sendtext(e)
    vis_df.to_csv('/Users/linusnhh/Desktop/local/Python/twitter/project_tweet/output/uk_coronavirus_stats.csv', index = False)
    print('Data has been updated.')