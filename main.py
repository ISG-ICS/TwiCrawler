import json
import logging
import logging.config
import os
import pickle
import threading
import time

from crawler.twitter_filter_api_crawler import TweetFilterAPICrawler
from crawler.twitter_id_mode_crawler import TweetIDModeCrawler
from crawler.twitter_search_api_crawler import TweetSearchAPICrawler
from dumper.twitter_dumper import TweetDumper
from extractor.twitter_extractor import TweetExtractor
from paths import TWITTER_TEXT_CACHE, LOG_CONFIG_PATH, LOG_DIR
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
            f"SELECT id FROM records WHERE user_id IS NULL or text is null ORDER BY create_at DESC"):
        if id not in cache:
            cache.add(id)
            result.append(id)
        if len(result) == 100:
            yield result
            result.clear()
    pickle.dump(cache, open(TWITTER_TEXT_CACHE, 'wb+'))
    yield result


def thread_function(mode, keywords):
    tweet_dumper = TweetDumper()
    tweet_extractor = TweetExtractor()
    if mode == "filter_mode":
        tweet_filter_api_crawler = TweetFilterAPICrawler()
        while True:
            ids = tweet_filter_api_crawler.crawl(keywords, batch_number=100)
            tweet_dumper.insert(ids, id_mode=True)
            time.sleep(10)
    elif mode == "search_mode":
        tweet_search_api_crawler = TweetSearchAPICrawler()
        while True:
            ids = tweet_search_api_crawler.crawl(keywords, batch_number=100)
            tweet_dumper.insert(ids, id_mode=True)
    elif mode == "id_mode":
        tweet_id_mode_crawler = TweetIDModeCrawler()
        while True:
            try:
                for ids in _fetch_id_from_db():
                    status = tweet_id_mode_crawler.crawl(ids)
                    tweets = tweet_extractor.extract(status)
                    tweet_dumper.insert(tweets)
                    time.sleep(5)
            except:
                pass
            finally:
                time.sleep(5)


def read_keywords():
    keywords = set()
    with open("keywords.txt", 'r') as file:
        for line in file:
            keyword = line.strip().lower()
            if keyword:
                keywords.add(keyword)
    return list(keywords)


def initialize_logger() -> logging.Logger:
    """
    Initializes a logger
    :return: initialized logger
    """
    with open(LOG_CONFIG_PATH, 'r') as file:
        # create path to save logs
        if not os.path.exists(LOG_DIR):
            os.makedirs(LOG_DIR)
        config = json.load(file)
        # use json file to config the logger
        logging.config.dictConfig(config)
        logger = logging.getLogger('1')
        info_format = '[%(asctime)s] [%(levelname)s] [%(threadName)s] [%(module)s] [%(funcName)s]: %(message)s'
        date_format = '%m/%d/%Y-%H:%M:%S'
        formatter = logging.Formatter(fmt=info_format, datefmt=date_format)
        handler_names = ['info.log', 'error.log']
        current_time = time.strftime('%m%d%Y_%H-%M-%S_', time.localtime(time.time()))
        for handler_name in handler_names:
            file_name = os.path.join(LOG_DIR, current_time + handler_name)
            # create log file in advance
            if not os.path.exists(file_name):
                # `touch` only works on *nix systems, not cross-platform. using open()
                with open(file_name, 'w'):
                    pass
            file_handler = logging.FileHandler(file_name, mode='a', encoding=None, delay=False)
            file_handler.setLevel(
                logging.DEBUG if 'info' in handler_name else logging.ERROR)
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)

    return logger


if __name__ == "__main__":
    format = '[%(asctime)s] [%(levelname)s] [%(threadName)s] [%(module)s] [%(funcName)s]: %(message)s'
    handler_name = 'main.log'
    current_time = time.strftime('%m%d%Y_%H-%M-%S_', time.localtime(time.time()))
    logging.basicConfig(format=format, level=logging.INFO, filename=os.path.join(LOG_DIR, current_time + handler_name),
                        datefmt="%H:%M:%S")
    logging.getLogger().addHandler(logging.StreamHandler())

    logging.info('Crawler Starting...')

    keywords = read_keywords()

    logging.info(f"keywords={keywords}")
    threads = list()
    for mode in ['id_mode', 'search_mode', 'filter_mode']:
        thread = threading.Thread(target=thread_function, args=(mode, keywords))
        threads.append(thread)
        thread.start()

    for index, thread in enumerate(threads):
        thread.join()
        logging.info("Main    : thread %d done", index)
