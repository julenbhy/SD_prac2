
###     imports     ###
import tweepy
import json
import csv
import pandas as pd
from textblob import TextBlob
from lithops import Storage
from lithops.multiprocessing import Pool


###   constant declarations   ###
CONSUMER_KEY = "cUTbEOCImSDyFH4GAFxNuHjhx"
CONSUMER_SECRET = "RxSQhoUyOkcXV1oJEpJofk6TLhoiLooJsYRfEjg2dP3mU1ul0N"
ACCESS_TOKEN = "566149666-y9lr86gtUGUlVYkqGp8zFVgWonfniPqcdv9XMYI2"
ACCESS_TOKEN_SECRET = "Ma7DFP6XSXR0ocw0S0JhJJk1F2uwS5FUM1kdQxYGtaAiV"

BUCKET = "prac2-bucket"


# select the numbert of tweets to analize of each issue
NUM_TWEETS = 1000

# the list of issues to analyze
ISSUES=[("palestine",),
        ("gaza",),
        ("israel",),
        ("#EURO2020",),
        ("covid",),
        ("G7",),
        ("china",)]




def counting_words(tweet_list):
    tweet_list = json.loads(tweet_list)
    for tweet in tweet_list:
        tweet["Length"] = len(tweet.get("Text").split())
    return json.dumps(tweet_list)



def sentiment_analyzer(tweet_list, issue):

    for tweet in tweet_list:
        analysis = TextBlob(tweet.get("Text"))
        tweet["Sentiment"] = analysis.sentiment.polarity

    csvfile="./dataset/"+issue+".csv"
    with open(csvfile, 'w') as f:
        w = csv.DictWriter(f, tweet_list[0].keys())
        w.writeheader()
        for tweets in tweet_list:
            w.writerow(tweets) 

    



#######################################################
###              Stage 1:  data crawler             ###
#######################################################

# gets num of tweets containin issue and makes a dictionary with their content
def get_tweets (issue):

    global NUM_TWEETS
    i=0

    #authentication on twitter
    auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
    #creation of the twitter api
    api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

    #the query to search
    query = '"' + issue + '"' + " lang:en -filter:retweets"
    tweet_list=[]
    #obtain the result of the query and iterate the tweets for extracting information
    for tweet in tweepy.Cursor(api.search, q=query, tweet_mode="extended").items(NUM_TWEETS):
        text = tweet.full_text
        author = str(tweet.user.screen_name)
        date = tweet.created_at.strftime("%m/%d/%Y %H:%M:%S")
        location = str(tweet.user.location)
        url = "https://twitter.com/twitter/statuses/" + str(tweet.id)

        #print ("\ntext:" + text + "\nauthor: @" + author + "date: " + date + "location: " + location + "url: " + url)

        #create a dictionary to sum the tweet info we have collected
        tweet_info = {
            "Text": text,
            "Author": author,
            "Date": date,
            "Location": location,
            "URL": url
        }
        tweet_list.append(tweet_info)
        i += 1          


    # Upload to COS
    namefile=issue+".txt"
    storage = Storage()
    storage.put_object(bucket=BUCKET, key=namefile, body=json.dumps(tweet_list))





############################################################
###              Stage 2:  data preprocesing             ###
############################################################

def analize_tweets(issue):
    #read tweet_list from cloud
    namefile=issue+".txt"
    storage = Storage()
    json_list=storage.get_object(bucket=BUCKET, key=namefile)
    tweet_list = json.loads(json_list)

    #tweet_list = counting_words(tweet_list)
    tweet_list = json.loads(counting_words(json.dumps(tweet_list)))
    sentiment_analyzer(tweet_list, issue)





############################################################
###                Stage 3: Python notebook              ###
############################################################

def average_sentiment(issue):
    
    with open("./dataset/"+str(issue)+".csv", 'r') as file:
        pd_file = pd.read_csv(file)

    sentiment_avg =  pd_file["Sentiment"].mean()
    length_avg = pd_file["Length"].mean()

    return issue, sentiment_avg, length_avg






############################################################
###                         MAIN                         ###
############################################################


if __name__ == '__main__':
    with Pool() as p:
        p.starmap(get_tweets, ISSUES)
        p.starmap(analize_tweets, ISSUES)

