
from datetime import date
import json
import logging
import sys

from peewee import MySQLDatabase
import tweepy
from urllib import parse

import database as mytools
from database import Tweet
import credentials as cred  # also includes path to logs

# should move to a credentials file
db = MySQLDatabase(cred.SQLDB, host=cred.SQLHOST, user=cred.SQLUSER,passwd=cred.SQLPASS, charset="utf8")
db.connect()

auth = tweepy.OAuthHandler(cred.CONSUMER_KEY, cred.CONSUMER_SECRET)
auth.set_access_token(cred.ACCESS_TOKEN, cred.ACCESS_SECRET)
api = tweepy.API(auth, wait_on_rate_limit=True)



# SEARCHES = ["@SAFRAN", "@Alstom","@airliquidegroup"',"@TechnipGroup"',
#            "@SolvayGroup","@Rexel_Group","@VolvoTrucksFR","@orexad_FR",
#            "@Capgemini","@PublicisGroupe","@ENGIEgroup","@ArcelorMittal",
#            "@Intel","@Cisco","@Forrester","@Adobe","@Salesforce",
#            "@Oracle","@MaerskLine","@Generalelectric","@VMware"]

SEARCHES = ['@orexad_FR', '@airliquidegroup', '@VolvoTrucksFR']

JSON_FILEPATH =  cred.PATH + "B2Bfiles/data/"
LOGGERPATH = cred.PATH + "B2Bfiles/logs/"
TODAY = date.today().strftime("%Y-%m-%d")

def get_tweets(SEARCH):

    res = Tweet.select(Tweet.id).where(Tweet.searchterm==SEARCH).order_by(Tweet.id.desc()).get()
    ID = res.id

    try:
        results = api.search(
            SEARCH, since_id=ID
            )
    except:
        print("error, no results")
    return results

def write_file(searchterm, results):

    dict_to_save = {}
    for row in results:
        data = row._json
        dict_to_save[data['id']] = data

    filename = JSON_FILEPATH + "tweets_" + searchterm + "_" + TODAY + ".json"
    jsondata = json.dumps(dict_to_save)
    with open(filename, "w", encoding="utf8", errors="ignore") as handle:
        handle.write(jsondata)
    return filename


def add_to_database(tweets, searchterm):

    counter = 0
    for tweet in tweets:
        if tweet:
            data = tweet._json
            t = mytools.create_tweet_from_dict(data, searchterm)
            if t:
                counter += 1
            else:
                logging.error("Did not save tweet %s" % data["id"])
        else:
            continue
    return counter

def main():

    logger = logging.getLogger('collect_tweets')
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    logger.setLevel(logging.INFO)

    for SEARCH in SEARCHES:

        hdlr = logging.FileHandler(LOGGERPATH + 'collect' + SEARCH + '.log')
        hdlr.setFormatter(formatter)
        logger.addHandler(hdlr)

        tweets = get_tweets(SEARCH)  # returns a list
        foundcount = len(tweets)
        logger.info("Collected %s tweets for term %s" % (foundcount, SEARCH))

        fileout = write_file(SEARCH, tweets)
        logger.info("Wrote out file %s" % fileout)

        savedcount = add_to_database(tweets, SEARCH)

        logger.info("Added %s tweets to the db for term %s" % (savedcount, SEARCH))

        if foundcount != savedcount:
            diff = foundcount - savedcount
            logger.warning("Mismatch of %s in Found vs Saved for %s" % (diff, SEARCH)) 
        logger.removeHandler(hdlr)

if __name__ == "__main__":
    main()

