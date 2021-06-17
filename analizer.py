
###     imports     ###
import tweepy
import json
import csv
from csv import writer
from csv import reader
from textblob import TextBlob
import lithops
from lithops import Storage
from lithops.multiprocessing import Pool

###   constant declarations   ###
CONSUMER_KEY = "cUTbEOCImSDyFH4GAFxNuHjhx"
CONSUMER_SECRET = "RxSQhoUyOkcXV1oJEpJofk6TLhoiLooJsYRfEjg2dP3mU1ul0N"
ACCESS_TOKEN = "566149666-y9lr86gtUGUlVYkqGp8zFVgWonfniPqcdv9XMYI2"
ACCESS_TOKEN_SECRET = "Ma7DFP6XSXR0ocw0S0JhJJk1F2uwS5FUM1kdQxYGtaAiV"

BUCKET = "prac2-bucket"

ISSUES=[("seleccion española",100,),
        ("perro sanchez",100,),
        ("covid",100,)]


#######################################################
###              Stage 1:  data crawler             ###
#######################################################

# gets num of tweets containin issue and makes a dictionary with their content
def get_tweets (issue, num):

    #tweet_list =  {}
    i=0

    #authentication on twitter
    auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
    #creation of the twitter api
    api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

    #the query to search
    query = '"' + issue + '"' + " lang:es -filter:retweets"
    tweet_list=[]
    #obtain the result of the query and iterate the tweets for extracting information
    for tweet in tweepy.Cursor(api.search, q=query, tweet_mode="extended").items(num):
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

    namefile="./dataset/"+issue+".txt"
    #namefile=issue+".txt"
    textfile=open(namefile, 'w')
    for element in tweet_list:
        textfile.write(str(element)+"\n")
    textfile.close()

    # Upload to COS
    #storage = Storage()
    #storage.put_object(bucket=BUCKET, key=namefile, body=json.dumps(tweet_list))


def counting_words(issue):
    csvfileinput="./dataset/"+issue+"1.csv"
    csvfileoutput="./dataset/"+issue+"2.csv"
    #csvfileinput=issue+"1.csv"
    #csvfileoutput=issue+"2.csv"
    with open(csvfileinput, 'r') as read_obj, \
        open(csvfileoutput, 'w', newline='') as write_obj:
        csv_reader= reader(read_obj)
        csv_writer = writer(write_obj)
        for row in csv_reader:
            row.append(len(row[0].split()))
            csv_writer.writerow(row)



def sentiment(issue):
    csvfileinput="./dataset/"+issue+"2.csv"
    csvfileoutput="./dataset/"+issue+"3.csv"
    #csvfileinput=issue+"2.csv"
    #csvfileoutput=issue+"3.csv"
    with open(csvfileinput, 'r') as read_obj, \
        open(csvfileoutput, 'w', newline='') as write_obj:
        csv_reader= reader(read_obj)
        csv_writer = writer(write_obj)
        for row in csv_reader:
            analysis = TextBlob(row[0])
            row.append(analysis.sentiment)
            csv_writer.writerow(row)



############################################################
###              Stage 2:  data preprocesing             ###
############################################################

def analize_tweets(issue,words):
    #read tweet_list from cloud
    namefile="./dataset/"+issue+".txt"
    #namefile=issue+".txt"
    #storage = Storage()
    #textfile=storage.get_object(bucket=BUCKET, key=namefile)
    with open(namefile) as f:
        string_list = f.readlines()
    #lines = textfile.read()
    #tweet_list= lines.splitlines
    #namefile.close()
    #print(string_list)
    tweet_list=[]
    for string in string_list:
        tweet_list.append(eval(string))
        

    csvfile="./dataset/"+issue+"1.csv"
    #csvfile=issue+"1.csv"
    with open(csvfile, 'w') as f:
            w = csv.DictWriter(f, tweet_list[0].keys())
            #w.writeheader()
            for tweets in tweet_list:
                w.writerow(tweets)
    #storage.put_object(bucket=BUCKET, key=namefile)
    
    counting_words(issue)
    sentiment(issue)



############################################################
###                Stage 3: Python notebook              ###
############################################################





if __name__ == '__main__':
    #get_tweets("selectividad", 100)
    #analize_tweets("selectividad")
    
    with Pool() as p:
        p.starmap(get_tweets, ISSUES)#,("abascal",100,),("perro sanchez",100)]))
        p.starmap(analize_tweets, ISSUES)