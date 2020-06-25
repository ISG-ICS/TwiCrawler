import json
import logging.config
import random
import time
from collections import defaultdict
from enum import Enum
from typing import Tuple, Optional, Dict

import pygeoj
from numpy import mean
from shapely.geometry import Polygon, Point
from shapely.strtree import STRtree
import shelve

from paths import GENERAL_LOG_CONFIG_PATH


class RandomMode(Enum):
    GEO_CENTER = 1
    UNIFORM_DISTRIBUTION_RANDOM = 2
    NORMAL_DISTRIBUTION_RANDOM = 3


class TwitterJSONTagger:

    def __init__(self, random_mode=RandomMode.UNIFORM_DISTRIBUTION_RANDOM):
        # by default we set UNIFORM_DISTRIBUTION_RANDOM as Point in Polygon.
        self._random_mode = random_mode
        self.shelve = shelve.open('geo_tag.shelve')
        try:
            # self.abbrev_us_state is the relation between the state's full name and abbrev.
            with open('us_state_abbrev.json') as json_file:
                self._abbrev_us_state = json.load(json_file)
            # cache is the hash map { (city, state), other geo info}
            self._city_state_mapping = dict()
            # self.city_datafile is a big json.
            self._city_json_file = pygeoj.load(filepath="city.json")
            self._init_city_state_mapping()
            self._init_coord_tree_cache()
        except (ValueError, FileExistsError, FileNotFoundError) as err:
            logger.critical(err)
            exit(1)

    def _init_city_state_mapping(self) -> None:
        """Load city state mapping from file or pickle"""

        if 'city_state_mapping' in self.shelve:
            self._city_state_mapping = self.shelve['city_state_mapping']
            logger.debug("loaded city_state_mapping from geo_tag.shelve")
        else:
            logger.debug("extracting features from city json file")
            for feature in self._city_json_file:
                # store the city, state as key; (cityID, countyID, stateID, countyName) as value.
                if feature.properties is not None:
                    _city_state = feature.properties["name"] + ", " + feature.properties["stateName"]
                    # key, value
                    self._city_state_mapping[_city_state] = (
                        feature.properties["cityID"], feature.properties["countyID"], feature.properties["stateID"],
                        feature.properties["countyName"])
                else:
                    raise ValueError("no feature properties found, load city.json failed.")

            logger.debug("shelving _city_state_mapping")
            self.shelve['city_state_mapping'] = self._city_state_mapping
            logger.debug(f"successfully shelved _city_state_mapping")

    def _init_coord_tree_cache(self) -> None:
        """Initialize or read hash map from pickle"""

        self._coord_mapping = dict()
        self._geometries = list()
        if 'coord_mapping' in self.shelve and 'geometries' in self.shelve:
            self._coord_mapping = self.shelve['coord_mapping']
            logger.debug("loaded coord_mapping from geo_tag.shelve")
            self._geometries = self.shelve['geometries']
            logger.debug("loaded geometries from geo_tag.shelve")
        else:
            logger.debug("extracting features from city json file")
            for feature in self._city_json_file:
                bbox = feature.geometry.bbox
                if bbox is None:
                    raise ValueError("can not find bbox.")

                # clockwise, (long, lat)
                points_list = [Point(bbox[0], bbox[1]), Point(bbox[0], bbox[3]), Point(bbox[2], bbox[3]),
                               Point(bbox[2], bbox[1])]
                poly = Polygon(points_list)
                self._geometries.append(poly)
                points_tuple = ((bbox[0], bbox[1]), (bbox[0], bbox[3]), (bbox[2], bbox[3]), (bbox[2], bbox[1]))

                self._coord_mapping[points_tuple] = (feature.properties["stateID"], feature.properties["stateName"],
                                                     feature.properties["countyID"], feature.properties["countyName"],
                                                     feature.properties["cityID"], feature.properties["name"])

            logger.debug("shelving _coord_mapping")
            self.shelve['coord_mapping'] = self._coord_mapping
            logger.debug(f"successfully shelved _coord_mapping")
            logger.debug("shelving _geometries")
            self.shelve['geometries'] = self._geometries
            logger.debug(f"successfully shelved _geometries")

    def get_coordinate(self, tweet_json: Dict, sigma=1) -> Optional[Tuple[float, float]]:
        """Returns a longitude, latitude pair found in coordinates or place; returns None if not applicable"""
        coord = tweet_json.get('coordinates')
        place = tweet_json.get('place')
        logger.debug(f"extracted coord: {coord}")
        logger.debug(f"extracted place: {place}")
        if coord is not None:
            logger.debug("using coord")
            return coord.get('coordinates')
        elif place is not None:

            # 2. get the central point of the bounding_box
            bounding_box = place.get('bounding_box').get('coordinates')[0]
            logger.debug(f"using bounding box {bounding_box}")

            # Twitter has some wield format historically, though it still rectangle,
            # but it is not always in (sw, se, ne, nw) order

            sw_lng = min(bounding_box[0][0], bounding_box[1][0], bounding_box[2][0], bounding_box[3][0])
            sw_lat = min(bounding_box[0][1], bounding_box[1][1], bounding_box[2][1], bounding_box[3][1])
            ne_lng = max(bounding_box[0][0], bounding_box[1][0], bounding_box[2][0], bounding_box[3][0])
            ne_lat = max(bounding_box[0][1], bounding_box[1][1], bounding_box[2][1], bounding_box[3][1])

            sw_lat, sw_lng = self._standardize_bounding_box(ne_lat, ne_lng, sw_lat, sw_lng)

            # Return Central Point or Random Point in the polygon each time.
            if self._random_mode == RandomMode.GEO_CENTER:
                return (sw_lng + ne_lng) / 2.0, (sw_lat + ne_lat) / 2.0

            elif self._random_mode == RandomMode.UNIFORM_DISTRIBUTION_RANDOM:
                return self._uniform_distribute_random_point(ne_lat, ne_lng, sw_lat, sw_lng)

            elif self._random_mode == RandomMode.NORMAL_DISTRIBUTION_RANDOM:
                return self._normal_distribution_random_point(ne_lat, ne_lng, sigma, sw_lat, sw_lng)
            else:
                raise ValueError("Invalid mode selection in bounding_box.")
        else:
            raise ValueError("no place field.")

    @staticmethod
    def _normal_distribution_random_point(ne_lat, ne_lng, sigma, sw_lat, sw_lng):
        poly = Polygon([(sw_lng, sw_lat), (ne_lng, sw_lat), (ne_lng, ne_lat), (sw_lng, ne_lat)])
        while True:
            random_point = Point([random.normalvariate(mu=(sw_lng + ne_lng) / 2, sigma=sigma),
                                  random.normalvariate(mu=(sw_lat + ne_lat) / 2, sigma=1)])
            if random_point.within(poly):
                return random_point.x, random_point.y

    @staticmethod
    def _uniform_distribute_random_point(ne_lat, ne_lng, sw_lat, sw_lng):
        poly = Polygon([(sw_lng, sw_lat), (ne_lng, sw_lat), (ne_lng, ne_lat), (sw_lng, ne_lat)])
        while True:
            random_point = Point([random.uniform(sw_lng, ne_lng), random.uniform(sw_lat, ne_lat)])
            if random_point.within(poly):
                # convert from Point to tuple
                return random_point.x, random_point.y

    @staticmethod
    def _standardize_bounding_box(ne_lat, ne_lng, sw_lat, sw_lng):
        # The AsterixDB is unhappy with this kind of point "rectangular"
        logger.debug(f"standardizing bounding box from [{ne_lat, ne_lng, sw_lat, sw_lng}]")
        if sw_lng == ne_lng and sw_lat == ne_lat:
            sw_lng = ne_lng - 0.0000001
            sw_lat = ne_lat - 0.0000001
        if sw_lng > ne_lng or sw_lat > ne_lat:
            raise ValueError(f"Invalid coordinates in bounding_box: "
                             f"sw_lng = f{sw_lng}, sw_lat = f{sw_lat}, "
                             f"ne_lng = f{ne_lng}, ne_lat = f{ne_lat}")
        logger.debug(f"\t\t\t\t\t\t\t  to [{ne_lat, ne_lng, sw_lat, sw_lng}]")
        return sw_lat, sw_lng

    def _extract_geo_tag_from_city_and_state(self, coord: tuple, city_state_name: str,
                                             flag: str) -> Dict:
        """helper function. city_state_name: eg. (Irvine, CA). flag: source of infer."""

        if not city_state_name:
            raise ValueError("Can not find city_name in extract function.")

        if "," not in city_state_name:
            raise ValueError("Invalid city_name to parse in extract function.")

        city_name, state_name = city_state_name.split(",")
        target_state = state_name.strip()
        full_state_name = self._abbrev_us_state[target_state]

        # search it in the cache(hash map) to get the county's name
        target_key = f'{city_name}, {full_state_name}'

        geo_content = self._city_state_mapping[target_key]

        geo_tag = dict()
        geo_tag["stateID"] = geo_content[2]
        geo_tag["stateName"] = full_state_name
        geo_tag["countyID"] = geo_content[1]
        geo_tag["countyName"] = geo_content[3]
        geo_tag["cityID"] = geo_content[0]
        geo_tag["cityName"] = city_state_name.split(",")[0]
        geo_tag["coordinate"] = coord
        geo_tag["source"] = flag
        return geo_tag

    def _infer_geo_from_place(self, tweet_json: Dict, coord: Tuple[int, int]):
        if not self._city_state_mapping:
            self._init_city_state_mapping()

        place = tweet_json.get('place')
        if place is not None:
            # extract the city and state abbrev
            city_state_name = place.get('full_name')
            if city_state_name is None:
                raise KeyError("Not find full_name key in place.")
            return self._extract_geo_tag_from_city_and_state(coord, city_state_name, "place")

        else:
            logger.debug("place is None.")

    def _infer_geo_from_coord(self, coord: tuple) -> Optional[Dict]:

        # only consider USA
        if coord[0] > -60 or coord[1] < 18:
            raise ValueError("this location is not in USA.")

        # traverse the hash map to see if this coord is in the tree.
        geo_content = tuple()
        cache_tree = STRtree(self._geometries)
        query_geom = Point(*coord)

        # ret is a list
        ret = cache_tree.query(query_geom)
        if not ret:
            raise ValueError("can not find location in STRTREE.")

        # we just pick the first one in the list as the inferred result.
        target = tuple(ret[0].exterior.coords[:-1])
        if self._coord_mapping.get(target):
            geo_content = self._coord_mapping[target]

        if geo_content:
            geo_tag = dict()
            geo_tag["stateID"] = geo_content[0]
            geo_tag["stateName"] = geo_content[1]
            geo_tag["countyID"] = geo_content[2]
            geo_tag["countyName"] = geo_content[3]
            geo_tag["cityID"] = geo_content[4]
            geo_tag["cityName"] = geo_content[5]
            geo_tag["coordinate"] = coord
            geo_tag["source"] = "coordinate"

            return geo_tag

        else:
            raise ValueError("can not load geo_content.")

    def _infer_geo_from_user(self, tweet_json, coord: tuple) -> Optional[dict]:
        user = tweet_json.get('user')
        if user is not None:
            # extract the city and state abbrev
            city_state_name = user.get('location')
            if city_state_name is None:
                raise KeyError("not find location in user field.")

            return self._extract_geo_tag_from_city_and_state(coord, city_state_name, "user")
        else:
            logger.debug("no user")
            return None

    def tag_one_tweet(self, tweet_json: Dict, sigma=1) -> Dict:
        logger.debug("-----------------------------------------")
        logger.debug(f"tweet id: {tweet_json['id']}")
        coord = None
        try:
            # get the coordinates
            coord = self.get_coordinate(tweet_json, sigma)

            # 1. check the place field
            tweet_json['geo_tag'] = self._infer_geo_from_place(tweet_json, coord)
        except (KeyError, ValueError) as err:
            logger.debug(err)

        # step 1 failed.
        if tweet_json.get('get_tag') is None:

            if coord:
                # 2. Check the coordinate.
                try:
                    tweet_json['geo_tag'] = self._infer_geo_from_coord(coord)
                except (KeyError, ValueError) as err:
                    logger.debug(err)
            else:
                # 3. infer from the "User" field
                try:
                    tweet_json['geo_tag'] = self._infer_geo_from_user(tweet_json, coord)

                except (KeyError, ValueError) as err:
                    logger.debug(err)

        # The methods above are all not working, for now we skip.
        # TODO: Use NLP to infer geo_tag from text in tweet.
        if 'geo_tag' not in tweet_json:
            tweet_json['geo_tag'] = None
        # Return None for exceptions.
        assert 'geo_tag' in tweet_json, "failed to tag geo information."

        return tweet_json


if __name__ == '__main__':
    # for debug
    logging.config.fileConfig(GENERAL_LOG_CONFIG_PATH)
    logger = logging.getLogger()

    # initialize
    twitter_json_tagger = TwitterJSONTagger()

    # test case

    # test one tweet
    # with open('test', 'r') as _input:
    #     test_tweet = json.load(_input)
    #     tagged_tweet = twitter_json_tagger.tag_one_tweet(test_tweet, 1)
    #     print(tagged_tweet)

    # test all
    counters = defaultdict(int)
    performance = list()
    with open("test", 'r') as _input:
        start_time = time.perf_counter()
        for line in _input:
            tweet_data = json.loads(line)
            counters['tweet'] += 1
            # we set sigma to 1
            tagged_tweet = twitter_json_tagger.tag_one_tweet(tweet_data, 1)

            if tagged_tweet.get('geo_tag') is not None:
                counters['geo_tag'] += 1
                if tagged_tweet.get('geo_tag').get('source') == 'user':
                    counters['user'] += 1

                elif tagged_tweet.get('geo_tag').get('source') == 'place':
                    counters['place'] += 1
                elif tagged_tweet.get('geo_tag').get('source') == 'coordinate':
                    counters['coordinate'] += 1

            if counters['tweet'] % 100 == 0:
                # mark and update
                end_time = time.perf_counter()
                performance.append(end_time - start_time)
                start_time = time.perf_counter()

    print(counters)
    print("average performance for 100 tweets: ", mean(performance), "s")

    # TODO: change _extract to return a dict, append a coord.
    # TODO: coordinate and bounding box.
    # TODO: debug place inconsistency (3 vs. 4) in test.
    # TODO: sigma
