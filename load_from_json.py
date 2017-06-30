# usage: python load_from_json.py <folder of json> <searchterm>"
# if searchterm is "all" then use all the files in folder and derive
# searchterm from the json filename


import json
import logging
from os import listdir
from os.path import isfile, join
import sys
logger = logging.getLogger('load_json') # in this order because of circular dep
import database

# generic defaults - modified in main loop below

FORMATTER = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
hdlr = logging.FileHandler('tweet_collector.log')  # defaults
hdlr.setFormatter(FORMATTER)
logger.addHandler(hdlr)
logger.setLevel(logging.INFO)


def get_json_filenames(folder):
    # because we want to return full paths, we need to make sure there is
    # a / at the end.
    # If this doesn't work on Windows, change the slash direction.
    if not folder.endswith("/"):
        folder = folder + "/"
    # this will return only the filenames, not folders inside the path
    return [folder + f for f in listdir(folder) if isfile(join(folder, f)) and f != ".DS_Store" and f.endswith(".json")]

def iterate_file(filename, status_frequency=50):
    i = 0
    jsonfilename = filename
    tweet_dict = {}
    with open(jsonfilename) as jfile:
        try:
            rows = json.loads(jfile.read())
        except:
            print("Error with file", jsonfilename)
            logger.error("File issue: %s", jsonfilename)
            yield None
            return
        # the key is the date jean used, not the data
        data = [value for key,value in rows.items()]
        # reduce overlap, use keys to eliminate dupes
        tweet_dict = {d["id"]:d for d in data}
        for line in tweet_dict.values():
            i += 1
            yield line # the yield returns the row
            if i % status_frequency == 0:
                print("Status >>> %s: %d" % (jsonfilename, i))


def load_from_files(files, searchterm):
    # Files is a list of json files, searchterm is the search used
    for file in files:
            print("File ", file)
            logger.info("file %s", file)
            for tweet in iterate_file(file):
                if tweet:
                    database.create_tweet_from_dict(tweet, searchterm)
                else:
                    continue
    return

def main():

    global logger  # because being used in database module and mod here

    #print(len(sys.argv))
    if len(sys.argv) < 3:
        print("Usage: python load_from_json.py <folder of json> <searchterm>")
        return
    PATH = sys.argv[1]
    SEARCHTERM = sys.argv[2]

    files = get_json_filenames(PATH)

    if SEARCHTERM != "all":
    #  Main loop:
        if files:
            load_from_files(files, SEARCHTERM)
        else:
            print("No json files found.")
        return

    if SEARCHTERM == "all":
        hdlr = logging.FileHandler('load_json_ALL.log')
        hdlr.setFormatter(FORMATTER)
        logger.addHandler(hdlr)
        for file in files:
            SEARCHTERM = file.split("_")[1]
            print("search term is ", SEARCHTERM)
            load_from_files([file], SEARCHTERM)

if __name__ == "__main__":
    main()
