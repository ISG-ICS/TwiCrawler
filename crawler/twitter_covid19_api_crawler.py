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


class TweetCOVID19APICrawler(CrawlerBase):
    MAX_WAIT_TIME = 128

    def __init__(self):
        super().__init__()
        self.wait_time = 1

        # this is not balanced, rather just parsing from config directly
        self.api = parse(TWITTER_API_CONFIG_PATH)["twitter-covid-19-API"]

        self.data: List = []
        self.keywords = []
        self.total_crawled_count = 0

    def get_bearer_token(self):
        response = requests.post(
            "https://api.twitter.com/oauth2/token",
            auth=(self.api.get('consumer_key'), self.api.get('consumer_secret')),
            data={'grant_type': 'client_credentials'},
            headers={"User-Agent": "TwitterDevCovid19StreamQuickStartPython"})

        if response.status_code is not 200:
            raise Exception(f"Cannot get a Bearer token (HTTP %d): %s" % (response.status_code, response.text))

        body = response.json()
        return body['access_token']

    def crawl(self, partition):
        re_attempts = 7
        while True:
            try:
                logger.info("Attempting to connect to stream...")
                response = requests.get(f"https://api.twitter.com/labs/1/tweets/stream/covid19?partition={partition}",
                                        headers={"User-Agent": "Some agent",
                                                 "Authorization": f"Bearer {self.get_bearer_token()}"},
                                        stream=True)
                for response_line in response.iter_lines():
                    if response_line:
                        if response_line == b'Rate limit exceeded':
                            self.wait()
                            continue
                        self.reset_wait_time()
                        data = json.loads(response_line)
                        try:
                            assert isinstance(data, dict), "returned is not dict"
                            assert 'text' in data and 'id' in data, "no data"
                            yield data
                            re_attempts = 7
                        except Exception as err:
                            logger.error(f'{data} - {err}')
                            raise err
            except Exception as err:
                if re_attempts:
                    logger.error(err)
                    self.wait()
                    re_attempts -= 1
                else:
                    raise err

    def reset_wait_time(self) -> None:
        """resets the wait time"""
        self.wait_time = 1

    def wait(self) -> None:
        """Incrementally waits when the request rate hits limitation."""
        logger.info(f"waiting {self.wait_time}")
        time.sleep(self.wait_time)
        if self.wait_time < self.MAX_WAIT_TIME:  # set a wait time limit no longer than 128s
            self.wait_time *= 2  # an exponential back-off pattern in subsequent reconnection attempts


if __name__ == '__main__':
    logger.setLevel(logging.INFO)
    logger.addHandler(logging.StreamHandler())

    tweet_covid_19_api_crawler = TweetCOVID19APICrawler()
    for i in tweet_covid_19_api_crawler.crawl(1):
        print(i)
