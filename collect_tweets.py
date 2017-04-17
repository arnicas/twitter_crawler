
from datetime import date
import json
import logging
import sys

from numpy import NINF  # negative infinity
from peewee import MySQLDatabase
import tweepy
from urllib import parse

import database as mytools
from database import Tweet
import credentials as cred  # also includes path to logs

# should move to a credentials file
db = MySQLDatabase(cred.SQLDB, host=cred.SQLHOST, user=cred.SQLUSER,passwd=cred.SQLPASS, charset="utf8")

auth = tweepy.OAuthHandler(cred.CONSUMER_KEY, cred.CONSUMER_SECRET)
auth.set_access_token(cred.ACCESS_TOKEN, cred.ACCESS_SECRET)
api = tweepy.API(auth, wait_on_rate_limit=True)


SEARCHES = ["@SAFRAN", "@Alstom","@airliquidegroup","@TechnipGroup",
            "@SolvayGroup","@Rexel_Group","@VolvoTrucksFR","@orexad_FR",
            "@Capgemini","@PublicisGroupe","@ENGIEgroup","@ArcelorMittal",
            "@Intel","@Cisco","@Forrester","@Adobe","@Salesforce",
            "@Oracle","@MaerskLine","@Generalelectric","@VMware"]

#SEARCHES = ['@Adobe', '@Cisco']

JSON_FILEPATH = cred.PATH + "B2Bfiles/data/"
LOGGERPATH = cred.PATH + "B2Bfiles/logs/"

TODAY = date.today().strftime("%Y-%m-%d")

def get_start_id(SEARCH, date=None):
    # date format is %Y-%m-%d
    db.connect()

    if not date:
        res = Tweet.select(Tweet.id).where(Tweet.searchterm==SEARCH).order_by(Tweet.id.desc()).get()
    else:
        res = Tweet.select(Tweet.id).where((Tweet.searchterm==SEARCH) and (Tweet.date <= date)).order_by(Tweet.date.asc()).get()
    id = res.id
    db.close()
    return id

def get_end_id(SEARCH, date=None):
    db.connect()
    if date:
        res = Tweet.select(Tweet.id).where((Tweet.searchterm==SEARCH) and (Tweet.date <= date)).order_by(Tweet.date.desc()).get()
        id = res.id
    else:
        res = Tweet.select(Tweet.id).where(Tweet.searchterm==SEARCH).order_by(Tweet.id.desc()).get()
    id = res.id
    db.close()
    return id

def get_tweets(SEARCH, sinceId, max_id=None):
    # code from https://www.karambelkar.info/2015/01/how-to-use-twitters-search-rest-api-most-effectively./
    maxTweets = 100000 # just large
    tweetsPerQry = 100
    tweets = []
    tweetCount = 0

    if not max_id:
        max_id = NINF

    while tweetCount < maxTweets:
        try:
            if (max_id <= 0):
                new_tweets = api.search(q=SEARCH, count=tweetsPerQry,
                                            since_id=sinceId)
            else:
                new_tweets = api.search(q=SEARCH, count=tweetsPerQry,
                                            max_id=str(max_id - 1),
                                            since_id=sinceId)
            if not new_tweets:
                break
            tweetCount += len(new_tweets)
            max_id = new_tweets[-1].id
            print("found %s tweets" % tweetCount)
            tweets.extend(new_tweets)
        except tweepy.TweepError as e:
            # Just exit if any error
            print("some error : " + str(e))
            break

    logger.info("Downloaded {0} tweets".format(tweetCount))
    return tweets

def write_file(searchterm, resultsjson, date=None):

    filename = JSON_FILEPATH + "tweets_" + searchterm + "_" + date + ".json"
    with open(filename, "w", encoding="utf8", errors="ignore") as handle:
        for res in resultsjson:
            handle.write(json.dumps(res) + "\n")
    return filename

def add_to_database(tweets, searchterm):

    counter = 0
    for tweet in tweets:
        if tweet:
            t = mytools.create_tweet_from_dict(tweet, searchterm)
            if t:
                counter += 1
            else:
                logger.error("Did not save tweet %s" % tweet["id"])
        else:
            continue
    return counter

def main():
    global logger
    logger = logging.getLogger('collect_tweets')
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    logger.setLevel(logging.INFO)

    max_id = None
    date_start = None
    date_end = None
    
    if len(sys.argv) == 3:
        date_start = sys.argv[1]
        date_end = sys.argv[2]

    for SEARCH in SEARCHES:

        hdlr = logging.FileHandler(LOGGERPATH + 'collect' + SEARCH + '.log')
        hdlr.setFormatter(formatter)
        logger.addHandler(hdlr)

        tweets = {}
        results = None

        if date_end:
            max_id = get_end_id(SEARCH, date=date_end)
        else:
            date_end = TODAY

        logger.info("Max id %s", max_id)

        id = get_start_id(SEARCH, date=date_start)
        results = get_tweets(SEARCH, id, max_id=max_id)  # returns a list
        if results:
            for res in results:
                tweets[res._json["id"]] = res._json

        foundcount = len(tweets)
        logger.info("Unique tweets found for %s is %s" % (SEARCH, foundcount))
        fileout = write_file(SEARCH, tweets.values(), date=date_end)
        logger.info("Wrote out file %s" % fileout)
        savedcount = add_to_database(tweets.values(), SEARCH)

        logger.info("Added %s tweets to the db for term %s" % (savedcount, SEARCH))

        if foundcount != savedcount:
            diff = foundcount - savedcount
            logger.warning("Mismatch of %s in Found vs Saved for %s" % (diff, SEARCH)) 
        logger.removeHandler(hdlr)

if __name__ == "__main__":
    main()
