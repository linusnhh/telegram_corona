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

target_channel = '@globalcoronavirusupdates'
miner = TweetMiner(result_limit = 500) #Mine tweet. 
uk_tweets = miner.mine_user_tweets(user='DHSCgovUK', max_pages=10)
number_regex = '\d+(?:,\d+)*'
url_regex = '(?:(?:https?|ftp):\/\/)?[\w/\-?=%.]+\.[\w/\-?=%.]+'

def get_tweets_dataframe(mined_tweets):
    """Take mined tweets as args, return a tweets dataframe."""
    tweets_df= pd.DataFrame(mined_tweets)
    tweets_df = tweets_df[tweets_df['text'].str.contains("died")] # Only include coronavirus update  
    tweets_df = tweets_df[['screen_name', 'created_at', 'text']].reset_index(drop=True) # Keep relevant columns 
    tweets_df['date'] = tweets_df.created_at.dt.strftime('%Y-%m-%d') 
    tweets_df['time'] = tweets_df.created_at.dt.strftime('%H:%M:%S')
    tweets_df = tweets_df.drop(columns=['created_at'])
    return tweets_df

def get_info_dataframe(tweets_df, number_regex, url_regex): 
    """Take tweets dataframe as args, return a dataframe with extracted info."""
    info_df = tweets_df.loc[(tweets_df['date'] > '2020-04-05')] # Only include tweets with latest info
    info_df['test_cum'] = info_df.text.str.findall(number_regex).str[2] # Cumulative test 
    info_df['test_daily'] = info_df.text.str.findall(number_regex).str[3] # Daily test
    info_df['ppl_test_cum'] = info_df.text.str.findall(number_regex).str[5] # Cumulative people
    info_df['ppl_confirmed_cum'] = info_df.text.str.findall(number_regex).str[6] # Cumnulative confirmed
    info_df['death_cum'] = info_df.text.str.findall(number_regex).str[9] # Cumulative death
    info_df['url'] = info_df.text.str.findall(url_regex).apply(''.join) # Source url
    info_df.dropna(inplace = True) # Filter out tweets that are not the updates
    return info_df

def get_numeric_dataframe(df): 
    """Take dataframe as args, return a dataframe in numeric format."""
    df = df.stack().str.replace(',', '').unstack() # Convert numbers in number format
    df = df.apply(pd.to_numeric, errors='ignore') 
    return df

def get_daily_df(info_df):
    """Take extracted info dataframe as args, return a dataframe with daily stats."""
    info_df['ppl_tested_daily'] = info_df.ppl_test_cum.diff(periods = -1).fillna(0).astype(np.int64)
    info_df['death_daily'] = info_df.death_cum.diff(periods=-1).fillna(0).astype(np.int64)
    info_df['ppl_confirmed_daily'] = info_df.ppl_confirmed_cum.diff(periods = -1).fillna(0).astype(np.int64)
    info_df['ppl_percentage_daily'] = info_df['ppl_confirmed_daily']/info_df['ppl_tested_daily']
    info_df['ppl_confirmed_case_change'] = info_df.ppl_confirmed_daily.diff(periods = -1).fillna(0).astype(np.int64)
    info_df['ppl_death_change_number'] = info_df.death_daily.diff(periods = -1).fillna(0).astype(np.int64)
    info_df['ppl_confirmed_change'] = info_df.ppl_confirmed_daily.pct_change(periods=-1)
    info_df['death_change'] = info_df.death_daily.pct_change(periods=-1)
    info_df['ppl_tested_change'] = info_df.ppl_tested_daily.pct_change(periods=-1)
    info_df['ppl_confirmed_rate'] = info_df['ppl_confirmed_daily']/info_df['ppl_tested_daily']
    daily_df = info_df.drop(columns=['test_cum', 'ppl_test_cum', 'ppl_confirmed_cum', 'death_cum'])
    return daily_df

def get_cum_df(info_df):
    """Take extracted info dataframe as args, return a dataframe with cumulative stats."""
    info_df['death_rate'] = info_df['death_cum']/info_df['ppl_confirmed_cum']
    cum_df = info_df[['test_cum', 'ppl_test_cum', 'ppl_confirmed_cum','death_cum', 'death_rate','url']]
    return cum_df

def telegram_bot_sendtext(bot_message, target_channel):
        bot_token = '1157266597:AAEAEJN67IFaAHuCpl43dqCg_GlBq2xKPyo'
        bot_chatID = target_channel
        send_text = 'https://api.telegram.org/bot' + bot_token + '/sendMessage?chat_id=' + bot_chatID + '&parse_mode=Markdown&text=' + bot_message
        response = requests.get(send_text)

uk_df = get_tweets_dataframe(uk_tweets)
info_df = get_info_dataframe(uk_df, number_regex, url_regex)
info_df = get_numeric_dataframe(info_df)
daily_df = get_daily_df(info_df)
cum_df = get_cum_df(info_df)

# Varaibles for daily mesage
latest_date = daily_df.date.iloc[0] 
ppl_tested_today = format(daily_df.ppl_tested_daily.iloc[0], ',')
ppl_tested_change = round(daily_df.ppl_tested_change.iloc[0]*100, 1)
ppl_confirmed_case_change = format(daily_df.ppl_confirmed_case_change.iloc[0], ',')
ppl_confirmed_change = round(daily_df.ppl_confirmed_change.iloc[0]*100, 1)
ppl_confirmed_today = format(daily_df.ppl_confirmed_daily.iloc[0], ',')
ppl_confirmed_yesterday = format(daily_df.ppl_confirmed_daily.iloc[1], ',')
ppl_confirmed_rate = round(daily_df.ppl_confirmed_rate.iloc[0]*100, 1)
death_case_change = format(daily_df.ppl_death_change_number.iloc[0], ',')
death_change = round(daily_df.death_change.iloc[0]*100, 1)
death_today =format(daily_df.death_daily.iloc[0], ',')
death_yesterday =format(daily_df.death_daily.iloc[1], ',')

# Varaibles for cumulative message
ppl_test_cum = format(cum_df.ppl_test_cum.iloc[0], ',')
ppl_confirmed_cum = format(cum_df.ppl_confirmed_cum.iloc[0], ',')
death_cum = format(cum_df.death_cum.iloc[0], ',')
death_rate = round(cum_df.death_rate.iloc[0]*100, 1)
url = cum_df.url.iloc[0]

daily_testing = 'On {0}, {1} people are tested, representing a {2}% change. '.format(latest_date, \
                ppl_tested_today, ppl_tested_change)
daily_confirmed = 'People who have been tested positive today changed by {0} ({1}%) to {2} (yesterday: {3}). The positive rate is {4}%. '.format(ppl_confirmed_case_change, \
                ppl_confirmed_change, ppl_confirmed_today, ppl_confirmed_yesterday, ppl_confirmed_rate)
daily_today = 'Death toll today changed by {0} ({1}%) to {2} (yesterday: {3}).'.format(death_case_change,
                death_change, death_today, death_yesterday)
cum_confirmed = 'Cumulatively, {0} people are tested, of which {1} are tested positive. '.format(ppl_test_cum, ppl_confirmed_cum)
cum_death = 'The death toll is {0} and the death rate is {1}%. {2}'.format(death_cum, death_rate, url)
daily_msg = daily_testing + daily_confirmed + daily_today
cum_msg = cum_confirmed + cum_death
update_msg = daily_msg + cum_msg
print (update_msg)

output_df = pd.read_csv('/Users/linusnhh/Desktop/local/Python/twitter/project_tweet/output/uk_coronavirus_stats.csv')
if (date.today().strftime('%Y-%m-%d') == latest_date) == False:
    print ('Official data has not been updated yet')
if (date.today().strftime('%Y-%m-%d') != output_df.date.iloc[0]) == True: 
    print ('Latest data has already been written.')
if date.today().strftime('%Y-%m-%d') == latest_date and date.today().strftime('%Y-%m-%d') != output_df.date.iloc[0]:
    print('Sending message...')
    telegram_bot_sendtext(daily_msg, target_channel)
    telegram_bot_sendtext(cum_msg, target_channel)
    daily_df.to_csv('/Users/linusnhh/Desktop/local/Python/twitter/project_tweet/output/uk_coronavirus_stats.csv', index = False)
    print('Data has been updated.')