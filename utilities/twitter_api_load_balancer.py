import threading
from threading import Lock

import twitter

from paths import TWITTER_API_CONFIG_PATH
from utilities.ini_parser import parse


class TwitterAPILoadBalancer:
    iter_index = 0
    apis = [twitter.Api(**config, sleep_on_rate_limit=True) for config in
            parse(TWITTER_API_CONFIG_PATH).values()]
    lock = Lock()

    @staticmethod
    def get():
        TwitterAPILoadBalancer.lock.acquire()
        TwitterAPILoadBalancer.iter_index += 1
        if TwitterAPILoadBalancer.iter_index == len(TwitterAPILoadBalancer.apis):
            TwitterAPILoadBalancer.iter_index = 0
        api = TwitterAPILoadBalancer.apis[TwitterAPILoadBalancer.iter_index]
        TwitterAPILoadBalancer.lock.release()
        return api


if __name__ == '__main__':

    class DummyThread(threading.Thread):
        def run(self) -> None:
            print(TwitterAPILoadBalancer().get())


    threads = [DummyThread() for i in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
