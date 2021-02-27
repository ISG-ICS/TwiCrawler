import logging
import logging.config
import os
import pickle
import sys
import time
from multiprocessing import Process, Lock

from crawler.twitter_covid19_api_crawler import TweetCOVID19APICrawler
from crawler.twitter_filter_api_crawler import TweetFilterAPICrawler
from crawler.twitter_id_mode_crawler import TweetIDModeCrawler
from crawler.twitter_search_api_crawler import TweetSearchAPICrawler
from dumper.twitter_dumper import TweetDumper
from extractor.twitter_extractor import TweetExtractor
from paths import TWITTER_TEXT_CACHE, LOG_DIR, BACKUP_DIR, CACHE_DIR
from utilities.cacheset import CacheSet
from utilities.connection import Connection

try:
    cache: CacheSet[int] = pickle.load(open(TWITTER_TEXT_CACHE, 'rb+'))
except:
    cache = CacheSet()


def _fetch_id_from_db():
    """a generator which generates 100 id list at a time"""
    result = list()
    for id, in Connection.sql_execute(
            f"SELECT id FROM records WHERE create_at IS NULL and deleted IS NOT TRUE ORDER BY id DESC"):
        if id not in cache:
            cache.add(id)
            result.append(id)
        if len(result) == 100:
            yield result
            result.clear()
    pickle.dump(cache, open(TWITTER_TEXT_CACHE, 'wb+'))
    yield result


def start(mode):
    tweet_dumper = TweetDumper()
    tweet_extractor = TweetExtractor()
    if mode == "filter_mode":
        tweet_filter_api_crawler = TweetFilterAPICrawler()
        while True:
            keywords = read_keywords()
            ids = tweet_filter_api_crawler.crawl(keywords, batch_number=100)
            tweet_dumper.insert(ids, id_mode=True)
            time.sleep(10)
    elif mode == "search_mode":
        tweet_search_api_crawler = TweetSearchAPICrawler()
        while True:
            keywords = read_keywords()
            ids = tweet_search_api_crawler.crawl(keywords, batch_number=100)
            tweet_dumper.insert(ids, id_mode=True)
    elif mode == "id_mode":
        tweet_id_mode_crawler = TweetIDModeCrawler()
        while True:
            try:
                for ids in _fetch_id_from_db():
                    status = tweet_id_mode_crawler.crawl(ids)
                    logging.info(ids)
                    tweets = tweet_extractor.extract(status)
                    ids_with_text = {t['id'] for t in tweets}
                    ids_no_text = set(ids) - ids_with_text
                    logging.info(ids_no_text)
                    tweet_extractor.export(status, file_name="coronavirus")
                    tweet_dumper.insert(tweets)
                    tweet_dumper.delete(ids_no_text)
                    time.sleep(5)
            except:
                pass
            finally:
                time.sleep(5)

    elif mode == 'covid19_mode':

        threads = list()
        lock = Lock()

        # for mode in ['id_mode', 'search_mode', 'filter_mode']:
        def thread_function(partition):
            try:
                tweets = list()
                for tweet in TweetCOVID19APICrawler().crawl(partition):
                    tweets.append(tweet)
                    if len(tweets) == 100:
                        lock.acquire()
                        tweet_extractor.export(tweets, file_name="coronavirus")
                        lock.release()
                        tweets.clear()
            except:
                lock.acquire()
                exit(1)

        for i in range(1, 5):
            thread = Process(target=thread_function, args=(i,))
            threads.append(thread)
            thread.start()

        for index, thread in enumerate(threads):
            thread.join()
            logging.info("Main    : thread %d done", index)


def read_keywords():
    keywords = set()
    with open("keywords.txt", 'r') as file:
        for line in file:
            keyword = line.strip().lower()
            if keyword:
                keywords.add(keyword)
    logging.info(f"LOADING keywords={keywords}")
    return list(keywords)

if __name__ == "__main__":
    format = '[%(asctime)s] [%(levelname)s] [%(threadName)s] [%(module)s] [%(funcName)s]: %(message)s'
    handler_name = 'main.log'
    current_time = time.strftime('%m%d%Y_%H-%M-%S_', time.localtime(time.time()))
    logging.basicConfig(format=format, level=logging.ERROR, filename=os.path.join(LOG_DIR, current_time + handler_name),
                        datefmt="%H:%M:%S")
    logging.getLogger().addHandler(logging.StreamHandler())

    if not os.path.exists(CACHE_DIR):
        os.makedirs(CACHE_DIR)
    if not os.path.exists(BACKUP_DIR):
        os.makedirs(BACKUP_DIR)

    logging.info('Crawler Starting...')
    # for mode in ['id_mode', 'search_mode', 'filter_mode']:

    start(sys.argv[1])
