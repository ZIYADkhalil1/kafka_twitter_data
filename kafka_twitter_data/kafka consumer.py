from kafka import KafkaConsumer
import json
import pandas as pd
import plotly.express as px
import plotly.graph_objs as go
from plotly.subplots import make_subplots
from collections import Counter


consumer = KafkaConsumer(
        "sentiment_data",
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        group_id="consumer-group-a")


print("starting the consumer....")

for msg in consumer:
    reterived_json = json.loads(msg.value)
    twitter_data = pd.DataFrame(reterived_json[0])
    
    query = reterived_json[1]
    # Count the sentiment values
    sentiment_count = twitter_data['sentiment'].value_counts().reset_index()

    # Sort the data by sentiment count in descending order
    sentiment_count = sentiment_count.sort_values('sentiment', ascending=False)
    sentiment_count.rename(columns={'index':'Sentiment','sentiment':'count'},inplace = True)

    # Extract all words from the tweets
    all_words = [word for tweet in twitter_data['text'] for word in tweet.split()]

    # Count the number of times each word appears
    word_counts = Counter(all_words)

    # Select the top 5 most common words
    top_words = word_counts.most_common(5)

    # Unzip the list of tuples into two lists
    words, counts = zip(*top_words)

    # Set the color palette
    colors = px.colors.sequential.RdBu

    title = "Sentiment Analysis Dashboard for {} tweets".format(query)

    # Create a dashboard with both charts
    fig = make_subplots(rows=1, cols=2, subplot_titles=('Text analysis','Sentimet analysis'),
                    specs=[[{'type': 'xy'},{'type': 'pie'}]])

    fig.add_trace(go.Pie(labels=sentiment_count['Sentiment'], values=sentiment_count['count'], textinfo='percent+label', 
                                 marker=dict(colors=colors)), row=1, col=2)

    fig.add_trace(go.Bar(x=words, y=counts, marker_color='Darkred' , name=''), row=1, col=1)

    fig.update_traces(showlegend=False, selector=dict(name=''))

    fig.update_layout(height=500, title_text = title, title_font=dict(size=30), 
                  plot_bgcolor='whitesmoke', paper_bgcolor='whitesmoke')

    # Show the dashboard
    fig.show()