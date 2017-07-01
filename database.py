#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Modification of code by
# Copyright (C) 2016 Pascal Jürgens and Andreas Jungherr
# Pascal Jürgens and Andreas Jungherr. 2016. twitterresearch [Computer software]. Retrieved from https://github.com/trifle/twitterresearch
# Mods by Lynn Cherny (2017) to increase model coverage and storage.


from datetime import datetime
from pytz import utc, timezone
import sys

import peewee
from peewee import MySQLDatabase
from playhouse.fields import ManyToManyField

import credentials as cred
from load_from_json import logger


db = MySQLDatabase(cred.SQLDB, host=cred.SQLHOST, user=cred.SQLUSER,passwd=cred.SQLPASS, charset="utf8")
db.connect()

class BaseModel(peewee.Model):

    """
    Base model for setting the database to use
    """
    class Meta:
        database = db


class Hashtag(BaseModel):

    """
    Hashtag model.
    """
    tag = peewee.CharField(unique=True, primary_key=True)


class URL(BaseModel):

    """
    URL model.
    """
    url = peewee.CharField(unique=True, primary_key=True)


class User(BaseModel):

    """
    Twitter user model.
    Stores the user's unique ID as a primary key along with the username.
    """
    id = peewee.BigIntegerField(unique=True, primary_key=True)
    screen_name = peewee.CharField(null=True)
    description = peewee.CharField(null=True)
    followers = peewee.IntegerField(null=True)
    following = peewee.IntegerField(null=True)
    listed = peewee.IntegerField(null=True)
    name = peewee.CharField(null=True)
    url = peewee.CharField(null=True)
    statuses_count = peewee.IntegerField(null=True)
    created_at = peewee.DateTimeField(null=True)
    location = peewee.CharField(null=True)

    def last_tweet(self):
        return Tweet.select().where(Tweet.user == self).order_by(Tweet.id.desc())[0]

    def first_tweet(self):
        return Tweet.select().where(Tweet.user == self).order_by(Tweet.id.asc())[0]

class Place(BaseModel):

    id = peewee.CharField(unique=True, primary_key=True)
    full_name = peewee.CharField(null=True)
    country = peewee.CharField(null=True)
    country_code = peewee.CharField(null=True)
    name = peewee.CharField(null=True)
    type = peewee.CharField(null=True)
    url = peewee.CharField(null=True)


class Media(BaseModel):

    id = peewee.BigIntegerField(unique=True, primary_key=True)
    type = peewee.CharField(null=True)
    url = peewee.CharField(null=True)
    display_url = peewee.CharField(null=True)
    expanded_url = peewee.CharField(null=True)
    source_status_id = peewee.BigIntegerField(null=True)


class Tweet(BaseModel):

    """
    Tweet model.
    Stores the tweet's unique ID as a primary key along with the user, text and date.
    """
    id = peewee.BigIntegerField(unique=True, primary_key=True)
    user = peewee.ForeignKeyField(User, related_name='tweets')
    text = peewee.CharField()
    date = peewee.DateTimeField()
    tags = ManyToManyField(Hashtag)
    urls = ManyToManyField(URL)
    mentions = ManyToManyField(User)
    media = ManyToManyField(Media)
    searchterm = peewee.CharField()
    place = peewee.ForeignKeyField(Place, null=True)  # field is called _id
    reply_to_user = peewee.ForeignKeyField(
        User, null=True, related_name='replies')
    reply_to_tweet = peewee.BigIntegerField(null=True)
    retweet = peewee.ForeignKeyField(
        'self', null=True, related_name='retweets')
    lat = peewee.FloatField(null=True)
    lon = peewee.FloatField(null=True)


def deduplicate_lowercase(l):
    """
    Helper function that performs two things:
    - Converts everything in the list to lower case
    - Deduplicates the list by converting it into a set and back to a list
    """
    valid = list(filter(None, l))
    lowercase = [e.lower() for e in valid]
    if len(valid) != len(lowercase):
        logger.warning("The input file had {0} empty lines, skipping those. Please verify that it is complete and valid.".format(len(lowercase) - len(valid)))
    deduplicated = list(set(valid))
    return deduplicated


def create_user_from_tweet(tweet):
    """
    Function for creating a database entry for
    one user using the information contained within a tweet

    :param tweet:
    :type tweet: dictionary from a parsed tweet
    :returns: database user object
    """
    userdict = tweet['user']
    user = None
    user, created = User.get_or_create(
                id = userdict['id'],
                screen_name = userdict['screen_name']
                )
    if user and len(userdict.keys()) > 2:
        try:
            user.created_at = datetime.strptime(userdict['created_at'],"%a %b %d %H:%M:%S +0000 %Y")
            user.description = userdict['description']
            user.followers = userdict['followers_count']
            user.following = userdict['friends_count']
            user.listed = userdict['listed_count']
            user.name = userdict['name']
            user.url = userdict['url']
            user.statuses_count = userdict['statuses_count']
            user.location = userdict['location']
            user.save()
        except peewee.InternalError as exc:
            logger.error("Issue %s %s" % (exc, userdict))
        except peewee.IntegrityError as exc:
            logger.error("Issue key/integrity %s %s" % (exc, userdict))
        except:
            logger.error("Unexpected issue with user %s for %s" % (sys.exc_info()[0], userdict))
    return user


def create_hashtags_from_entities(entities):
    """
    Attention: Casts tags into lower case!
    Function for creating database entries for
    hashtags using the information contained within entities

    :param entities:
    :type entities: dictionary from a parsed tweet's "entities" key
    :returns: list of database hashtag objects
    """
    tags = [h["text"] for h in entities["hashtags"]]
    # Deduplicate tags since they may be used multiple times per tweet
    tags = deduplicate_lowercase(tags)
    db_tags = []
    for h in tags:
        tag, created = Hashtag.get_or_create(tag=h)
        db_tags.append(tag)
    return db_tags


def create_urls_from_entities(entities):
    """
    Attention: Casts urls into lower case!
    Function for creating database entries for
    urls using the information contained within entities

    :param entities:
    :type entities: dictionary from a parsed tweet's "entities" key
    :returns: list of database url objects
    """
    urls = [u["expanded_url"] for u in entities["urls"]]
    urls = deduplicate_lowercase(urls)
    db_urls = []
    for u in urls:
        url, created = URL.get_or_create(url=u)
        db_urls.append(url)
    return db_urls


def create_users_from_entities(entities):
    """
    Function for creating database entries for
    users using the information contained within entities

    :param entities:
    :type entities: dictionary from a parsed tweet's "entities" key
    :returns: list of database user objects
    """
    users = [(u["id"], u["screen_name"]) for u in entities["user_mentions"]]
    users = list(set(users))
    db_users = []
    for id, name in users:
        user, created = User.get_or_create(
            id = id,
            screen_name=name
        )
        db_users.append(user)
    return db_users

def create_place_from_places(placedict):

    place = None
    try:
        place, created = Place.get_or_create(
            id = placedict['id'],
            full_name = placedict['full_name'],
            country = placedict['country'],
            country_code = placedict['country_code'],
            name = placedict['name'],
            type = placedict['place_type'],
            url = placedict['url']
            )
    except:
        logger.error("error with place %s", sys.exc_info()[0])
        return place
    return place


def create_media_from_entities(medias):
    # Note: does not put urls into url table.

    all_media = []
    for media in medias:
        try:
            if not ("id" in media.keys()) and ("id_str" in media.keys()):
                id = int(media['id_str'])
            else:
                id = media['id']
            medium, created = Media.get_or_create(
                type = media["type"],
                url = media["url"],
                display_url = media["display_url"],
                expanded_url = media["expanded_url"],
                id = id
                )
            if "source_status_id" in media.keys():
                medium.source_status_id = media["source_status_id"]
                medium.save()
            all_media.append(medium)
        except peewee.IntegrityError as exc:
            logger.warning("media key error %s %s" % (exc, media))
            continue
        except:
            logger.error("Error with media save %s", sys.exc_info()[0])
            continue
    return all_media


def create_tweet_from_dict(tweet, searchterm, user=None):
    """
    Function for creating a tweet and all related information as database entries
    from a dictionary (that's the result of parsed json)
    This does not do any deduplication, i.e. there is no check whether the tweet is
    already present in the database. If it is, there will be an UNIQUE CONSTRAINT exception.

    :param tweet:
    :type tweet: dictionary from a parsed tweet
    :returns: bool success
    """
    # If the user isn't stored in the database yet, we
    # need to create it now so that tweets can reference her/him

    # place seems to not exist in many
    place = False
    media = False
    if "place" in tweet and tweet['place']:
        place = create_place_from_places(tweet['place'])

    if "entities" in tweet and "media" in tweet["entities"]:
        media = create_media_from_entities(tweet["entities"]["media"])

    try:
        if not user:
            user = create_user_from_tweet(tweet)
        if "entities" in tweet:
            tags = create_hashtags_from_entities(tweet["entities"])
            urls = create_urls_from_entities(tweet["entities"])
            mentions = create_users_from_entities(tweet["entities"])

        # Create new database entry for this tweet
        t = Tweet.create(
            id=tweet['id'],
            user=user,
            text=tweet['text'],
            searchterm = searchterm,
            date=datetime.strptime(tweet['created_at'], "%a %b %d %H:%M:%S +0000 %Y")
        )
        if tags:
            t.tags = tags
        if place:
            t.place_id = place.id
        if urls:
            t.urls = urls
        if mentions:
            t.mentions = mentions
        if media:
            t.media = media
        if tweet["coordinates"]:  # seems to not exit?
            t.lat = tweet['coordinates']['coordinates'][1]
            t.lon = tweet['coordinates']['coordinates'][0]
        if tweet["in_reply_to_user_id"]:
            # Create a mock user dict so we can re-use create_user_from_tweet
            reply_to_user_dict = {'user':
                                  {'id': tweet['in_reply_to_user_id'],
                                   'screen_name': tweet['in_reply_to_screen_name'],
                                   }}
            reply_to_user = create_user_from_tweet(reply_to_user_dict)
            t.reply_to_user = reply_to_user
            t.reply_to_tweet = tweet['in_reply_to_status_id']
        if 'retweeted_status' in tweet:
            retweet = create_tweet_from_dict(tweet['retweeted_status'], searchterm)
            t.retweet = retweet
        t.save()
        return t
    except peewee.IntegrityError as exc:
        logger.warning("key warning %s %s" % (exc, tweet))
        return False
    except:
        logger.error("unexpected error %s", sys.exc_info()[0])
        return False


def setup():
    # Set up database tables. This needs to run at least once before using the db.
    tables = [Hashtag, URL, User, Tweet, Place, Media, Tweet.tags.get_through_model(), Tweet.urls.get_through_model(), Tweet.mentions.get_through_model(), Tweet.media.get_through_model()]
    try:
        db.drop_tables(tables)
    except:
        print("some tables not there?")
    db.create_tables(tables,safe=True)

    # tag in Hashtag column can't be set to utf8mb4 because of length and unique restraint.
    db.execute_sql("ALTER TABLE tweet MODIFY text CHAR(255) CHARACTER SET utf8mb4")
    db.execute_sql("ALTER TABLE user MODIFY description CHAR(255) CHARACTER SET utf8mb4")
    db.execute_sql("ALTER TABLE user MODIFY screen_name CHAR(255) CHARACTER SET utf8mb4")
    db.execute_sql("ALTER TABLE user MODIFY name CHAR(255) CHARACTER SET utf8mb4")
    db.execute_sql("ALTER TABLE user MODIFY location CHAR(255) CHARACTER SET utf8mb4")

if __name__ == "__main__":
    #setup()
    print("If you run this at the command line, you want to setup. Uncomment it.")
