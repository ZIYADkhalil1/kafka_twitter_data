import tweepy as tw
from tweepy import OAuthHandler
import data_collection_and_preprocessing as dcp
import pandas as pd
from textblob import TextBlob 

class TwitterClient(object):
    '''
    Generic Twitter Class for sentiment analysis.
    '''
    def __init__(self):
        '''
        Class constructor or initialization method.
        '''
        # keys and tokens from the Twitter Dev Console
        consumer_key = ''
        consumer_secret = ''
        access_token = ''
        access_token_secret = ''
  
        # attempt authentication
        try:
            # create OAuthHandler object
            self.auth = OAuthHandler(consumer_key, consumer_secret)
            # set access token and secret
            self.auth.set_access_token(access_token, access_token_secret)
            # create tweepy API object to fetch tweets
            self.api = tw.API(self.auth)
        except:
            print("Error: Authentication Failed")
  
    def clean_tweet(self, tweet):
        '''
        Utility function to clean tweet text by removing links, special characters
        using simple regex statements.
        '''
        tweet = dcp.clean_tweets(tweet)
        #tweet = ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)", " ", tweet).split())
        return tweet
  
    def get_tweet_sentiment(self, tweet):
        '''
        Utility function to classify sentiment of passed tweet
        using textblob's sentiment method
        '''
        # create TextBlob object of passed tweet text
        analysis = TextBlob(tweet)
        # set sentiment
        if analysis.sentiment.polarity > 0.3:
            return 'positive'
        elif analysis.sentiment.polarity > -0.3:
            return 'neutral'
        else:
            return 'negative'
  
    def get_tweets(self, query, count = 100):
        '''
        Main function to fetch tweets and parse them.
        '''
        # empty list to store parsed tweets
        tweets = []
  
        try:
            # call twitter api to fetch tweets
            fetched_tweets = self.api.search_tweets(q = query, count = count)
            # parsing tweets one by one
            for tweet in fetched_tweets:

                # empty dictionary to store required params of a tweet
                parsed_tweet = {}

                # saving text of tweet
                cleaned_tweet = self.clean_tweet(tweet.text)
                parsed_tweet['text'] = cleaned_tweet

                # saving sentiment of tweet
                parsed_tweet['sentiment'] = self.get_tweet_sentiment(cleaned_tweet)
  
                # appending parsed tweet to tweets list
                if tweet.retweet_count > 0:

                    # if tweet has retweets, ensure that it is appended only once
                    if parsed_tweet not in tweets:
                        tweets.append(parsed_tweet)
                else:
                    tweets.append(parsed_tweet)
  
            # return parsed tweets
            return tweets
  
        except tw.errors.TweepyException as e:
            # print error (if any)
            print("Error : " + str(e))

    def tweet_labels_stat(self,tweets):

        # picking positive tweets from tweets
        self.ptweets = [tweet for tweet in tweets if tweet['sentiment'] == 'positive']

        # percentage of positive tweets
        print("Positive tweets percentage: {} %".format(100*len(self.ptweets)/len(tweets)))
    
        # picking negative tweets from tweets
        self.ntweets = [tweet for tweet in tweets if tweet['sentiment'] == 'negative']
    
        # percentage of negative tweets
        print("Negative tweets percentage: {} %".format(100*len(self.ntweets)/len(tweets)))

        # picking neutral tweets from tweets
        self.neutral_tweets = [tweet for tweet in tweets if tweet['sentiment'] == 'neutral']
    
        # percentage of negative tweets
        print("Neutral tweets percentage: {} %".format(100*len(self.neutral_tweets)/len(tweets)))

        self.data = pd.DataFrame(tweets)
  
