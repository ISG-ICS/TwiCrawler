import json
import logging
import time
from typing import List

import requests
import rootpath

from paths import TWITTER_API_CONFIG_PATH
from utilities.ini_parser import parse

rootpath.append()

from crawler.crawlerbase import CrawlerBase

logger = logging.getLogger()


class TweetCOVID19APIV2Crawler(CrawlerBase):
    MAX_WAIT_TIME = 128

    def __init__(self, extractor):
        super().__init__()
        self.wait_time = 1

        # this is not balanced, rather just parsing from config directly
        self.api = parse(TWITTER_API_CONFIG_PATH)["twitter-covid-19-API"]

        self.data: List = []
        self.keywords = []
        self.total_crawled_count = 0

        import tweepy

        class V2Client(tweepy.StreamingClient):

            # This will print the Tweet ID and Tweet text for each Tweet
            def on_data(self, data):
                extractor.export([data.decode('utf-8')], file_name="coronavirus")

        # Replace with your own bearer token below
        self.client = V2Client(self.get_bearer_token(), wait_on_rate_limit=True)
        # This is where we set our filter rule
        self.client.add_rules(tweepy.StreamRule("context:123.1220701888179359745"))

    def get_bearer_token(self):
        response = requests.post(
            "https://api.twitter.com/oauth2/token",
            auth=(self.api.get('consumer_key'), self.api.get('consumer_secret')),
            data={'grant_type': 'client_credentials'},
            headers={"User-Agent": "TwitterDevCovid19StreamQuickStartPython"})

        if response.status_code != 200:
            raise Exception(f"Cannot get a Bearer token (HTTP %d): %s" % (response.status_code, response.text))

        body = response.json()
        return body['access_token']

    def crawl(self):
        self.client.filter(
            tweet_fields="attachments,author_id,created_at,entities,geo,id,in_reply_to_user_id,lang,possibly_sensitive,public_metrics,referenced_tweets,source,text,withheld,context_annotations,conversation_id,reply_settings".split(
                ","),
            user_fields="id,created_at,entities,username,name,location,url,verified,profile_image_url,public_metrics,pinned_tweet_id,description,protected".split(
                ","),
            expansions="author_id,entities.mentions.username,in_reply_to_user_id,referenced_tweets.id,referenced_tweets.id.author_id,attachments.media_keys,geo.place_id".split(
                ","),
            place_fields="contained_within,country,country_code,full_name,geo,id,name,place_type".split(","),
            media_fields="duration_ms,height,media_key,preview_image_url,public_metrics,type,url,width,alt_text,variants".split(
                ",")
        )


if __name__ == '__main__':
    logger.setLevel(logging.INFO)
    logger.addHandler(logging.StreamHandler())
