import json
import logging
import os
import pickle
import pygeoj
import random
import time

from numpy import mean
from shapely.geometry import Polygon, Point
from shapely.strtree import STRtree

logger = logging.getLogger()

class TwitterJSONTagger:
    # default we set UNIFORM_DISTRIBUTION_RANDOM as Point in Polygon.
    GEO_CENTER = False
    UNIFORM_DISTRIBUTION_RANDOM = True
    NORMAL_DISTRIBUTION_RANDOM = False

    def __init__(self):
        # cache is the hashmap { (city, state), other geo info}
        self.cache_place = dict()
        # self.abbrev_us_state is the relation between the state's full name and abbrev.
        with open('us_state_abbrev.json') as json_file:
            self.abbrev_us_state = json.load(json_file)
        json_file.close()
        # self.city_datafile is a big json.
        self.city_datafile = pygeoj.load(filepath="city.json")

    def _init_place_cache(self):
        # check pickle module
        filename = "cache_place"
        if os.path.exists(filename):
            with open(filename, 'rb') as cache_file:
                self.cache_place = pickle.load(cache_file)
            cache_file.close()
        else:
            for feature in self.city_datafile:
                # store the city, state as key; (cityID, countyID, stateID, countyName) as value.
                if feature.properties is not None:
                    _city_state = feature.properties["name"] + ", " + feature.properties["stateName"]
                    # key, value
                    self.cache_place[_city_state] = (feature.properties["cityID"], feature.properties["countyID"], feature.properties["stateID"], feature.properties["countyName"])

                else:
                    logger.error("load city.json failed.")
                    raise KeyError()

            # store it using pickle
            with open(filename, 'wb+') as cache_file:
                pickle.dump(self.cache_place, cache_file)
            cache_file.close()

    def _init_coord_tree_cache(self):
        # initialize or get hashmap
        coord_filename = "cache_coord"
        tree_filename = "cache_tree"
        cache_coord = dict()
        geo_obj = list()

        if os.path.exists(coord_filename) and os.path.exists(tree_filename):
            with open(coord_filename, 'rb') as cache_file_1:
                cache_coord = pickle.load(cache_file_1)
            cache_file_1.close()

            with open(tree_filename, 'rb') as cache_file_2:
                geo_obj = pickle.load(cache_file_2)
            cache_file_2.close()

        else:
            for feature in self.city_datafile:
                bbox = feature.geometry.bbox
                if bbox is None:
                    logger.error("can not find bbox.")
                    raise ValueError()

                # clockwise, (long, lat)
                points_list = [Point(bbox[0], bbox[1]), Point(bbox[0], bbox[3]), Point(bbox[2], bbox[3]),
                               Point(bbox[2], bbox[1])]
                poly = Polygon(points_list)
                geo_obj.append(poly)
                points_tuple = ((bbox[0], bbox[1]), (bbox[0], bbox[3]), (bbox[2], bbox[3]), (bbox[2], bbox[1]))

                cache_coord[points_tuple] = (feature.properties["stateID"], feature.properties["stateName"],
                                             feature.properties["countyID"], feature.properties["countyName"],
                                             feature.properties["cityID"], feature.properties["name"])

            # store it using pickle
            with open(coord_filename, 'wb+') as cache_file_1:
                pickle.dump(cache_coord, cache_file_1)
            cache_file_1.close()

            with open(tree_filename, 'wb+') as cache_file_2:
                pickle.dump(geo_obj, cache_file_2)
            cache_file_2.close()

        return cache_coord, geo_obj

    def get_coordinate(self, tweet_json: dict, sigma: int):
        coord = tweet_json.get('coordinates')
        place = tweet_json.get('place')

        if coord is not None:
            return coord.get('coordinates')
        else:
            # 2. get the central point of the bounding_box
            if place is not None:
                bounding_box = place.get('bounding_box').get('coordinates')[0]
                ''' Twitter has some wield format historically, though it still rectangle, 
                but it is not always in (sw, se, ne, nw) order
                '''
                swLog = min(bounding_box[0][0], bounding_box[1][0], bounding_box[2][0], bounding_box[3][0])
                swLat = min(bounding_box[0][1], bounding_box[1][1], bounding_box[2][1], bounding_box[3][1])
                neLog = max(bounding_box[0][0], bounding_box[1][0], bounding_box[2][0], bounding_box[3][0])
                neLat = max(bounding_box[0][1], bounding_box[1][1], bounding_box[2][1], bounding_box[3][1])

                # AsterixDB is unhappy with this kind of point "rectangular"
                if (swLog == neLog and swLat == neLat):
                    swLog = neLog - 0.0000001
                    swLat = neLat - 0.0000001

                if (swLog > neLog or swLat > neLat):
                    logger.error("Invalid coordinates in bounding_box.")

                # Return Central Point or Random Point in the polygon each time.
                if TwitterJSONTagger.GEO_CENTER:
                    return ((swLog+neLog) / 2, (swLat+neLat) / 2)

                elif TwitterJSONTagger.UNIFORM_DISTRIBUTION_RANDOM:
                    poly = Polygon([(swLog, swLat), (neLog, swLat), (neLog, neLat), (swLog, neLat)])
                    while True:
                        random_point = Point([random.uniform(swLog, neLog), random.uniform(swLat, neLat)])
                        if random_point.within(poly):
                            # convert from Point to tuple
                            coord = (random_point.x, random_point.y)
                            return coord

                elif TwitterJSONTagger.NORMAL_DISTRIBUTION_RANDOM:
                    poly = Polygon([(swLog, swLat), (neLog, swLat), (neLog, neLat), (swLog, neLat)])
                    while True:
                        random_point = Point([random.normalvariate(mu=(swLog+neLog) / 2, sigma=sigma), random.normalvariate(mu=(swLat+neLat) / 2, sigma=1)])
                        if random_point.within(poly):
                            coord = (random_point.x, random_point.y)
                            return coord
                else:
                    logger.error("Invalid mode selection in bounding_box.")
            else:
                logger.error("no place field.")

    def _extract_geo_tag_from_city_and_state(self, tweet_json: dict, coord: tuple, city_state_name: str, flag: str):
        '''help function. city_state_name: eg. (Irvine, CA). flag: source of infer.'''

        if not city_state_name:
            logger.error("Can not find city_name in extract function.")
            raise KeyError()

        if "," not in city_state_name:
            logger.error("Invalid city_name to parse in extract function.")
            raise KeyError()

        state_of_city = city_state_name.split(",")[1]
        target_state = state_of_city.strip()
        full_state_name = self.abbrev_us_state.get(target_state)

        if full_state_name is None:
            logger.error("Can not find state_name in extract function.")
            raise KeyError()

        # search it in the cache(hashmap) to get the county's name
        target_key = city_state_name.split(",")[0] + ', ' + full_state_name

        geo_content = self.cache_place.get(target_key)
        if geo_content is not None:
            geo_tag = dict()
            geo_tag["stateID"] = geo_content[2]
            geo_tag["stateName"] = full_state_name
            geo_tag["countyID"] = geo_content[1]
            geo_tag["countyName"] = geo_content[3]
            geo_tag["cityID"] = geo_content[0]
            geo_tag["cityName"] = city_state_name.split(",")[0]
            geo_tag["coordinate"] = coord
            geo_tag["source"] = flag

            tweet_json['geo_tag'] = geo_tag
            logger.info(geo_tag)
            
        else:
            logger.error("not find county.")
            raise KeyError()

    def _infer_geo_from_place(self, tweet_json: dict, coord: tuple):
        if not self.cache_place:
            self._init_place_cache()

        place = tweet_json.get('place')
        if place is not None:
            # extract the city and state abbrev
            city_state_name = place.get('full_name')
            if city_state_name is None:
                logger.error("Not find full_name key in place.")
                raise KeyError()

            try:
                self._extract_geo_tag_from_city_and_state(tweet_json, coord, city_state_name, "place")
            except KeyError:
                logger.error("_extract_geo_tag_from_city_and_state() failed.")
                raise KeyError()
        else:
            logger.error("place is None.")
            raise KeyError()

    def _infer_geo_from_coord(self, tweet_json: dict, coord: tuple):

        # only consider USA
        if coord[0] > -60 or coord[1] < 18:
            logger.error("this location is not in USA.")
            raise ValueError()

        cache_coord, geo_obj = self._init_coord_tree_cache()

        # traverse the hashmap to see if this coord is in the tree.
        geo_content = tuple()
        cache_tree = STRtree(geo_obj)
        query_geom = Point(coord[0], coord[1])

        # ret is a list
        ret = cache_tree.query(query_geom)
        if len(ret) == 0:
            logger.error("can not find location in STRTREE.")
            raise ValueError()

        # we just pick the first one in the list as the inferred result.
        target = tuple(ret[0].exterior.coords[:-1])
        if cache_coord.get(target):
            geo_content = cache_coord[target]

        if len(geo_content) != 0:
            geo_tag = dict()
            geo_tag["stateID"] = geo_content[0]
            geo_tag["stateName"] = geo_content[1]
            geo_tag["countyID"] = geo_content[2]
            geo_tag["countyName"] = geo_content[3]
            geo_tag["cityID"] = geo_content[4]
            geo_tag["cityName"] = geo_content[5]
            geo_tag["coordinate"] = coord
            geo_tag["source"] = "coordinate"

            tweet_json['geo_tag'] = geo_tag
            logger.info(geo_tag)

        else:
            logger.error("can not load geo_content.")
            raise ValueError()

    def _infer_geo_from_user(self, tweet_json, coord: tuple):
        user = tweet_json.get('user')
        if user is not None:
            # extract the city and state abbrev
            city_state_name = user.get('location')
            if city_state_name is None:
                logger.error("not find location in user field.")
                raise KeyError()

            try:
                self._extract_geo_tag_from_city_and_state(tweet_json, coord, city_state_name, "user")
            except KeyError:
                logger.error("_extract_geo_tag_from_city_and_state() failed.")
                raise KeyError()

        else:
            # The methods above are all not working, for now we skip.
            # TODO: Use NLP to infer geo_tag from text in tweet.
            logger.error("no user field.")
            raise KeyError()

    def tag_one_tweet(self, tweet_json: dict, sigma: int):

        # get the coordinates
        coord = self.get_coordinate(tweet_json, sigma)
        # 1. check the place field
        try:
            self._infer_geo_from_place(tweet_json, coord)
        except KeyError:
            logger.error("_infer_geo_from_place(): infer failed.")

        # step 1 failed.
        if 'geo_tag' not in tweet_json:
            # 2. Check the coordinate.
            if coord:
                try:
                    self._infer_geo_from_coord(tweet_json, coord)
                except ValueError:
                    logger.error("_infer_geo_from_coord(): infer failed.")
            else:
                # 3. infer from the "User" field
                try:
                    self._infer_geo_from_user(tweet_json, coord)
                except KeyError:
                    logger.error("_infer_geo_from_user(): infer failed.")

        # Return None for exceptions.
        if 'geo_tag' not in tweet_json:
            # return None of geo_tag
            tweet_json['geo_tag'] = None

        return tweet_json

if __name__ == '__main__':
    # for debug
    logger.setLevel(logging.INFO)
    logger.addHandler(logging.FileHandler("log"))
    # logger.addHandler(logging.StreamHandler())

    # initialize
    twitter_json_tagger = TwitterJSONTagger()

    # test case
    counter = 0
    res = 0
    file = open("test", 'r')

    performance = list()
    start_time = time.clock()

    # test all
    lines = file.readlines()
    for line in lines:
        counter += 1
        tweet_data = json.loads(line)
        # we set sigma to 1
        temp = twitter_json_tagger.tag_one_tweet(tweet_data, 1)
        if temp.get('geo_tag') is not None:
            res += 1

        if counter % 100 == 0:
            # mark and update
            end_time = time.clock()
            print(end_time - start_time)
            performance.append(end_time - start_time)
            start_time = time.clock()

    file.close()
    print("valid tweets: ", res)
    print("average performance for 100 tweets: ", mean(performance), "s")



