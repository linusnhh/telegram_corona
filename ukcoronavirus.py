import pandas as pd
from datetime import date

from twitter_scrape import TweetMiner
from config import number_regex, url_regex, twitter_id
from utilities import get_tweets_dataframe, get_info_dataframe, get_daily_df, get_numeric_dataframe, get_cum_df, telegram_bot_sendtext
from auth import bot_chatID, bot_token

import warnings
warnings.filterwarnings('ignore')

miner = TweetMiner(result_limit = 500) #Mine tweet. 
uk_tweets = miner.mine_user_tweets(user=twitter_id, max_pages=10)
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
    telegram_bot_sendtext(bot_token, bot_chatID, daily_msg)
    telegram_bot_sendtext(bot_token, bot_chatID, cum_msg)
    daily_df.to_csv('/Users/linusnhh/Desktop/local/Python/twitter/project_tweet/output/uk_coronavirus_stats.csv', index = False)
    print('Data has been updated.')
    