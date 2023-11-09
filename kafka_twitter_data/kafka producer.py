import twitter as tw
from kafka import KafkaProducer
import json
import time

def json_serializer(data):
    return json.dumps(data.to_dict(orient='records'))


producer = KafkaProducer(bootstrap_servers= ['localhost:9092'],value_serializer = json_serializer) 

# creating object of TwitterClient Class
twitter = tw.TwitterClient()


while 1 == 1:

    # Prompt the user to enter a query topic
    query = input("Enter a topic to query from Twitter: ")

    # calling function to get tweets
    tweets = twitter.get_tweets(query = [query], count = 20)

    #Store the data in dataframe
    twitter_data = twitter.data

    #Get twitter data statistics
    twitter.tweet_labels_stat(tweets)

    producer.send("sentiment_data",[twitter_data,query])
    
    time.sleep(15)