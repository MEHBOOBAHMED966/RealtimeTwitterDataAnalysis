import tweepy
import pandas as pd 
import json
from datetime import datetime
import s3fs 

def run_twitter_etl():

    
    access_key = "2F0EOaF6VBT9CO8vHOPqtDUrA" 
    access_secret = "crZI7l24ggMKnVCseYOAbkRo6G3WXbsX9KJP7YsNvnLvMKZ3ay" 
    consumer_key = "768882384032890880-gNZtdwYOhSj0sWuKbTgu7bOzzGzXfLR"
    consumer_secret = "xvee0wdAkYhjdAXwT7Lp1gCKaQ08sM4YppqVHnwQLaOM9"


        # Twitter authentication
    auth = tweepy.OAuthHandler(access_key, access_secret)   
    auth.set_access_token(consumer_key, consumer_secret) 
    # # # Creating an API object 
    tweets = api.user_timeline(screen_name='@elonmusk', 
                                # 200 is the maximum allowed count
                                count=200,
                                include_rts = False,
                                # Necessary to keep full_text 
                                # otherwise only the first 140 words are extracted
                                tweet_mode = 'extended'
                                )

    list = []
    for tweet in tweets:
        text = tweet._json["full_text"]

        refined_tweet = {"user": tweet.user.screen_name,
                            'text' : text,
                            'favorite_count' : tweet.favorite_count,
                            'retweet_count' : tweet.retweet_count,
                            'created_at' : tweet.created_at}
            
        list.append(refined_tweet)

        df = pd.DataFrame(list)
        df.to_csv('s3://twitterbucket/refined_tweets.csv')