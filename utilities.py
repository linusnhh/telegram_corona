import pandas as pd
import numpy as np
import requests 

def get_tweets_dataframe(mined_tweets):
    """Take mined tweets as args, return a tweets dataframe."""
    tweets_df= pd.DataFrame(mined_tweets)
    tweets_df = tweets_df[['screen_name', 'created_at','text']].reset_index(drop=True) # Keep relevant columns 
    tweets_df['date'] = tweets_df.created_at.dt.strftime('%Y-%m-%d') 
    tweets_df['time'] = tweets_df.created_at.dt.strftime('%H:%M:%S')
    tweets_df = tweets_df.drop(columns=['created_at'])
    return tweets_df

def get_info_dataframe(tweets_df, number_regex, url_regex): 
    """Take tweets dataframe as args, return a dataframe with extracted info."""
    tweets_df = tweets_df[tweets_df['text'].str.contains("died")] # Only include coronavirus update  
    info_df = tweets_df.loc[(tweets_df['date'] > '2020-04-05')]# Only include tweets with latest info
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

def telegram_bot_sendtext(bot_token, bot_chatID, bot_message):
        send_text = 'https://api.telegram.org/bot' + bot_token + '/sendMessage?chat_id=' + bot_chatID + '&parse_mode=Markdown&text=' + bot_message
        response = requests.get(send_text)


