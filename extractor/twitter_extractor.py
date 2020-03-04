import datetime
import gzip
import json
import os
from datetime import datetime
from typing import List, Optional, Dict

import rootpath

rootpath.append()

from crawler.twitter_filter_api_crawler import TweetFilterAPICrawler
from crawler.twitter_id_mode_crawler import TweetIDModeCrawler
from paths import BACKUP_DIR

from extractor.extractorbase import ExtractorBase


class TweetExtractor(ExtractorBase):
    def __init__(self):
        super().__init__()
        self.crawler_data: Optional[List] = None
        self.data: list = []
        self.id: int

    def extract(self, data_from_crawler: List[Dict]) -> List:
        """extracts useful information after being provided with original tweet data (similar to a filter)"""
        collected_ids = set()
        self.data.clear()
        for tweet_json_string in data_from_crawler:
            # tweet_json_string is a list of dictionary
            tweet: dict = json.loads(str(tweet_json_string))
            id = tweet.get('id')
            if not id:
                continue

            if id not in collected_ids:
                collected_ids.add(id)

                # extracts (filters) the useful information
                date_time: datetime = datetime.strptime(tweet["created_at"], '%a %b %d %H:%M:%S %z %Y')
                full_text: str = tweet.get("full_text")
                hashtags: List[str] = [tag['text'] for tag in tweet["hashtags"]]
                profile_pic: str = tweet.get('user').get('profile_image_url')
                screen_name: str = tweet.get('user').get('screen_name')
                user_name: str = tweet.get('user').get('name')
                created_date_time: datetime = datetime.strptime(tweet['user']["created_at"], '%a %b %d %H:%M:%S %z %Y')
                followers_count: int = tweet.get('user').get('followers_count')
                favourites_count: int = tweet.get('user').get('favourites_count')
                friends_count: int = tweet.get('user').get('friends_count')
                user_id: int = tweet.get('user').get('id')
                if tweet.get('user').get('geo_enabled') is True:
                    user_location: str = tweet.get('user').get('location')
                else:
                    user_location: str = 'None'
                statuses_count: int = tweet.get('user').get('statuses_count')
                if tweet.get('place') is not None:
                    top_left, _, bottom_right, _ = tweet["place"]['bounding_box']['coordinates'][0]

                else:
                    top_left = bottom_right = None
                    # where the geolocation does not exist

                self.data.append(
                    {'id': id, 'date_time': date_time, 'full_text': full_text, 'hashtags': hashtags,
                     'top_left': top_left,
                     'bottom_right': bottom_right, 'profile_pic': profile_pic, 'screen_name': screen_name,
                     'user_name': user_name, 'created_date_time': created_date_time, 'followers_count': followers_count,
                     'favourites_count': favourites_count, 'friends_count': friends_count, 'user_id': user_id,
                     'user_location': user_location, 'statuses_count': statuses_count})

        return self.data
        # stores self.data and returns a reference of it

    def export(self, data, file_type="gz", file_name="", dir=BACKUP_DIR) -> None:
        """exports data with specified file type"""

        if not os.path.exists(BACKUP_DIR):
            os.makedirs(BACKUP_DIR)
        if file_type == 'gz':

            file_name += f"_{datetime.now().strftime('%m-%d-%Y')}.{file_type}"
            with gzip.open(os.path.join(dir, file_name), 'a+') as file:
                for one in data:
                    file.write(bytes(str(one) + '\n', encoding='utf8'))
        else:
            raise TypeError(f"not supported export file type {file_type}")


if __name__ == '__main__':

    tweet_filter_api_crawler = TweetFilterAPICrawler()

    tweet_id_mode_crawler = TweetIDModeCrawler()
    tweet_extractor = TweetExtractor()
    for _ in range(1):
        raw_ids = tweet_filter_api_crawler.crawl(['coronavirus'], batch_number=1)
        status = tweet_id_mode_crawler.crawl(raw_ids)
        print(status)
        print(tweet_extractor.extract(status))
        tweet_extractor.export(status, file_name="coronavirus")
    with gzip.open("../backup/coronavirus_03-04-2020.gz", 'r') as file:
        status = file.readlines()
        status = [one.decode('utf-8') for one in status]
        print(status)
        print(tweet_extractor.extract(status))
