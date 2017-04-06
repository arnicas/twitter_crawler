
from datetime import date
import json
import logging
import sys

from peewee import MySQLDatabase
import twitter
from urllib import parse

from database import Tweet
import credentials as cred

# should move to a credentials file
db = MySQLDatabase(cred.SQLDB, host=cred.SQLHOST, user=cred.SQLUSER,passwd=cred.SQLPASS, charset="utf8")
db.connect()
api = twitter.Api(consumer_key=cred.CONSUMER_KEY,
                  consumer_secret=cred.CONSUMER_SECRET,
                  access_token_key=cred.ACCESS_TOKEN,
                  access_token_secret=cred.ACCESS_SECRET,
                  sleep_on_rate_limit=True)

SEARCHES = ["@SAFRAN", "@Alstom","@airliquidegroup"',"@TechnipGroup"',
            "@SolvayGroup","@Rexel_Group","@VolvoTrucksFR","@orexad_FR",
            "@Capgemini","@PublicisGroupe","@ENGIEgroup","@ArcelorMittal",
            "@Intel","@Cisco","@Forrester","@Adobe","@Salesforce",
            "@Oracle","@MaerskLine","@Generalelectric","@VMware"]

JSON_FILEPATH = "B2Bfiles/data/"
LOGGERPATH = "B2Bfiles/logs/"
TODAY = date.today().strftime("%Y-%m-%d")

def get_tweets(SEARCH):

    res = Tweet.select(Tweet.id).where(Tweet.searchterm==SEARCH).order_by(Tweet.id.desc()).get()
    ID = res.id
    params = { 
        "q": SEARCH,
        "until": TODAY,
        "since_id": ID
        }
    try:
        results = api.GetSearch(
            raw_query=parse.urlencode(params)
            )
    except:
        print("error, no results")
    return results

def write_file(searchterm, results):

    dict_to_save = {}
    for row in results:
        data = json.loads(row.AsJsonString())
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
            t = database.create_tweet_from_dict(tweet, SEARCHTERM)
            if t:
                counter += 1
            else:
                logging.error("Did not save tweet %s" % tweet["id"])
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

        #savedcount = db.add_to_database(tweets, SEARCH)


        #logger.info("Added %s tweets to the db for term %s" % (savedcount, SEARCH))

        #if foundcount != savedcount:
        #    diff = foundcount - savedcount
        #    logger.warning("Mismatch of %s in Found vs Saved for %s" % (diff, SEARCH)) 
        logger.removeHandler(hdlr)

if __name__ == "__main__":
    main()

