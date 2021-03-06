import logging
import time
import traceback
from typing import List

import rootpath

rootpath.append()

from crawler.crawlerbase import CrawlerBase
from utilities.cacheset import CacheSet
from utilities.twitter_api_load_balancer import TwitterAPILoadBalancer

logger = logging.getLogger()


class TweetFilterAPICrawler(CrawlerBase):
    MAX_WAIT_TIME = 64

    def __init__(self):
        super().__init__()
        self.wait_time = 1
        self.api = TwitterAPILoadBalancer().get()
        self.data: List = []
        self.keywords = []
        self.total_crawled_count = 0
        self.cache: CacheSet[int] = CacheSet()

    def crawl(self, keywords: List, batch_number: int = 100) -> List[int]:
        """
        Crawling Tweet ID with the given keyword lists, using Twitter Filter API

        Twitter Filter API only provides compatibility mode, thus no `full_text` is returned by the API. Have to crawl
        for IDs and then fetch full_text with GetStatus, which will be in other thread.

        Args:

            keywords (List[str]): keywords that to be used for filtering tweet text, hash-tag, etc. Exact behavior
                is defined by python-twitter.

            batch_number (int): a number that limits the returned list length. using 100 as default since Twitter API
                limitation is set to 100 IDs per request.

        Returns:
             (List[int]): a list of Tweet IDs

        """
        self.keywords = list(map(str.lower, keywords + ["#" + keyword for keyword in keywords]))
        logger.info(f'Filter Mode crawler Started')
        self.data = []
        count = 0
        while len(self.data) < batch_number:
            logger.info(f'Filter Mode sending a Request to Twitter Filter API')
            try:

                for tweet in self.api.GetStreamFilter(track=self.keywords):
                    self.reset_wait_time()
                    if tweet.get('text') is None:
                        continue

                    # if the original tweet has keywords, add its id to cache and data
                    if tweet.get('retweeted_status') and self._has_keywords(tweet['retweeted_status']):
                        self._add_to_batch(tweet['retweeted_status']['id'])

                    # if the tweet contains keywords, add its id to cache and data (for return)
                    elif self._has_keywords(tweet):
                        self._add_to_batch(tweet['id'])
                    else:
                        continue

                    # print Crawling info every one tenth of the batch number
                    if len(self.data) > count:
                        count = len(self.data)
                        if batch_number >= 10:
                            if count % (batch_number // 10) == 0:
                                logger.info(f"Filter Mode crawled ID count in this batch: {count}")

                    if count >= batch_number:
                        logger.info(f"Filter Mode crawled ID count in this batch: {len(self.data)}")
                        break

            except:
                # in this case the collected twitter id will be recorded and tried again next time
                logger.error(f'Error: {traceback.format_exc()}')
                self.wait()

        count = len(self.data)
        logger.info(f'Outputting {count} Tweet IDs')
        self.total_crawled_count += count
        logger.info(f'Total crawled count {self.total_crawled_count}')
        return self.data

    def _has_keywords(self, tweet):
        try:
            return any([keyword in tweet['text'].lower() for keyword in set(self.keywords)])
        except:
            print(tweet)

    def _add_to_batch(self, tweet_id: int) -> None:
        if tweet_id not in self.cache:
            self.data.append(tweet_id)
            self.cache.add(tweet_id)

    def reset_wait_time(self) -> None:
        """resets the wait time"""
        self.wait_time = 1

    def wait(self) -> None:
        """Incrementally waits when the request rate hits limitation."""
        time.sleep(self.wait_time)
        if self.wait_time < self.MAX_WAIT_TIME:  # set a wait time limit no longer than 64s
            self.wait_time *= 2  # an exponential back-off pattern in subsequent reconnection attempts


if __name__ == '__main__':
    logger.setLevel(logging.INFO)
    logger.addHandler(logging.StreamHandler())

    tweet_filter_api_crawler = TweetFilterAPICrawler()
    for _ in range(3):
        raw_tweets = tweet_filter_api_crawler.crawl(['武汉', '新冠', '新冠肺炎', '冠状', '冠状病毒', '方舱医院', ''], batch_number=3)
        print(raw_tweets)
