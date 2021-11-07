from google.cloud import storage
import requests
import json,  traceback
from json import JSONDecodeError
from datetime import datetime, timedelta
from time import sleep
from collections import deque
import pandas as pd

def len2(iterable):
    """Calculate length by summing the integer 1 for every element
    O(n) in run time
    O(1) in memory"""
    return sum(1 for e in iterable)

class StockTwitsAPIScraper:
    def __init__(self, symbol, date, bucket , blob_path):
        self.finished=False
        self.symbol = symbol
        self.link = "https://api.stocktwits.com/api/2/streams/symbol/{}.json?".format(symbol)
        self.targetDate = date
        self.tweets = []
        self.reqeustQueue = deque()
        self.client=storage.Client()
        self.bucket= self.client.bucket(bucket)
        self.blob_path = blob_path
               

    def initialize_max_id(self,maxId):
        blobs_iterator = self.client.list_blobs(self.bucket.name, prefix=self.blob_path,delimiter='/')
        
        blobs=[]
        for element in blobs_iterator:
            blobs.append(element.name)
        size=len(blobs)
        if size==1:
            self.maxId=maxId
            print("maxId_1="+str(self.maxId))
        elif size>1:
            print("Elemento="+blobs[1])
            string_max_id=blobs[1].split("/")[-1].split('.')[0]
            self.maxId = int(string_max_id)
            print("maxId_2="+str(self.maxId))


    def inform_run_active(self):
        blob=self.bucket.blob(self.blob_path + "finished_run.txt")
        blob.upload_from_string("0",'txt/csv')

    def inform_run_finished(self):
        blob=self.bucket.blob(self.blob_path + "finished_run.txt")
        blob.upload_from_string("1",'txt/csv')

    def setLimits(self, size, duration):
        self.size = size
        self.duration = duration
        self.requestInterval = duration // size + 1 if duration % size else duration // size

    # write tweets we get and the ID of the last tweet in case system break down
    def writeJson(self):
        if self.tweets:
            self.maxId = self.tweets[-1]["id"]
            tweets_df=pd.json_normalize(self.tweets)
            
            #Convert to Date type a column that is timestamp type
            tweets_df['time']=pd.to_datetime(tweets_df['time'],unit='s')
            filename_csv = "{}.csv".format(self.maxId)
                      
            blob = self.bucket.blob(self.blob_path + filename_csv)
            blob.upload_from_string(tweets_df.to_csv(), 'text/csv')

            
    
    def getCurrentUrl(self):
        return self.link + "max={}".format(self.maxId)

    # request manager
    # can't exceed 200 requests within an hour
    def requestManager(self):
        if len(self.reqeustQueue) == self.size:
            now = datetime.now()
            firstRequest = self.reqeustQueue.popleft()
            if now < firstRequest + timedelta(seconds=self.duration):
                timeDiff = firstRequest - now
                waitTime = timeDiff.total_seconds() + 1 + self.duration                
                print("Holandaaaa 01 Reach request limit, wait for {} seconds.".format(waitTime))
                sleep(waitTime)

    def getMessages(self, url):
        self.requestManager()

        response = requests.get(url)
        self.reqeustQueue.append(datetime.now())
        try:
            data = json.loads(response.text)
        except JSONDecodeError:
            if "Bad Gateway" in response.text:
                print("Just a Bad Gateway, wait for 1 minute.")
                sleep(60)
                return True
            else:
                print(len(self.reqeustQueue))
                print(self.reqeustQueue[0], datetime.now())
                print(url)
                print(response.text)
                print(traceback.format_exc())
                print("Something worong with the response.")
                return False
                
        if data and data["response"]["status"] == 200:
            data["cursor"]["max"]
            for m in data["messages"]:
                record = {}            
                createdAt = datetime.strptime(m["created_at"], "%Y-%m-%dT%H:%M:%SZ")
                if createdAt < self.targetDate:
                    self.finished=True
                    return False
                record["id"] = m["id"]
                record["text"] = m["body"]
                record["time"] = createdAt.timestamp()
                record["sentiment"] = m["entities"]["sentiment"]["basic"] if m["entities"]["sentiment"] else ""
                self.tweets.append(record)
        else:
            print(response.text)        
        return True

    def getTweetsAndWriteToFile(self):        
        if not self.getMessages(self.getCurrentUrl()):
            return False
        self.writeJson()
        print("Scrap {} tweets starting from {}.".format(len(self.tweets), self.maxId))
        self.tweets.clear()
        sleep(self.requestInterval)
        return True

    def scrapTweets(self):        
        try:
            doScrap = True
            while doScrap:
                doScrap = self.getTweetsAndWriteToFile()
        except Exception:
            print(traceback.format_exc())

symbol = input("Enter stock symbol: ")
print("This scraper scraps tweets backward.\n\
The ID you put in belongs the most recent tweet you're goint go scrap.\n\
And the scraper will keep going backward to scrap older tweets.")
maxId = input("Enter the starting tweet ID: ")
targetDate = input("Enter the earlest date (mmddyyyy): ")
print("You can only send 200 requests to StockTwits in an hour.")
requestLimit = input("Enter the limit of number of requests within an hour: ")


def get_stocktwits(request):
    # Stock symbol to collect twits from
    symbol='SPY'
    # The ID belonging to the most recent tweet we're goint go scrap.
    maxId=382693194
    # The earliest date our scrap is going to reach
    targetDate='01012019'
    # Limit of number of requests within an hour. 
    # We can only send 200 requwests to Stocktwits in an hour.
    requestLimit=100000
    #Name of the bucket in Google Cloud Storage where we are going to save our collected data. 
    bucket_name='wid_sigtech'
    #Path to the bucket. This is going to create a folder named StockTwits in our bucket,
    # and another folder inside, with the name of our chosen stock. 
    blob_path="StockTwits/{}/".format(symbol)
        
    scraper = StockTwitsAPIScraper(symbol, datetime.strptime(targetDate, "%m%d%Y"),bucket_name,blob_path)
    scraper.initialize_max_id(int(maxId))
    scraper.setLimits(int(requestLimit), 3600)
    while (scraper.finished==False):
        try:
            scraper.scrapTweets()
        except:
            print("There was some exception or error")    
    return 'End of get_stocktwits'