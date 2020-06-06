import json
import os
import pickle

from geopy.geocoders import Nominatim

class TwitterJSONTagger:
    def __init__(self):
        # data is the tweet json which we add geo_tag.
        self.data = dict()
        # cache is the hashmap { (city, state), other geo info}
        self.cache = dict()
        # coord is the (longitude, latitude)
        self.coord = tuple()
        # GEOPY
        self.geo_locator = Nominatim()
        # self.abbrev_us_state is the relation between the state's full name and abbrev.
        with open('us_state_abbrev.json') as json_file:
            self.abbrev_us_state = json.load(json_file)
        # self.city_data is a big json from cloudberry -> cache
        city_datafile = open("city.json", "r", encoding='UTF-8')
        self.city_data = json.load(city_datafile)
        city_datafile.close()

    def init_geo_cache(self):
        # check pickle module
        filename = "cache_geo_hashmap"
        if os.path.exists(filename):
            cache_file = open(filename, 'rb')
            self.cache = pickle.load(cache_file)
            return
        else:
            city_json = self.city_data
            feature = city_json.get('features')
            for f in feature:
                item = f.get('properties')
                # store the city, state as key; (cityID, countyID, stateID, countyName) as value.
                if item is not None and "name" in item and "stateName" in item:
                    _city_state = item["name"] + ", " + item["stateName"]
                    # key, value
                    self.cache[_city_state] = (item["cityID"], item["countyID"], item["stateID"], item["countyName"])

                else:
                    raise KeyError("can not build cache from city.json.")

            # store it using pickle
            cache_file = open(filename, 'wb')
            pickle.dump(self.cache, cache_file)
            cache_file.close()

    def get_coordinate(self):
        coord = self.data.get('coordinates')
        place = self.data.get('place')

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
                    raise ValueError("Invalid coordinates in bounding_box.")

                # calculate the central point in the rectangle
                return ((swLog+neLog) / 2, (swLat+neLat) / 2)

    # private: help function. city_state_name: eg. (Irvine, CA). flag: source of infer.
    def __extract_geo_tag_from_city_and_state(self, city_state_name: str, flag: str):
        if city_state_name is None:
            raise ValueError("Can not find city_name in extract function.")

        if "," not in city_state_name:
            raise ValueError("Invalid city_name to parse in extract function.")

        state_of_city = city_state_name.split(",")[1]
        target_state = state_of_city.strip()
        full_state_name = self.abbrev_us_state.get(target_state)

        if full_state_name is None:
            raise KeyError("Can not find state_name in extract function.")

        # search it in the cache(hashmap) to get the county's name
        target_key = city_state_name.split(",")[0] + ', ' + full_state_name

        if self.coord is not None:
            # self.coord is empty now
            location = self.geo_locator.geocode(target_key)
            self.coord = (location.longitude, location.latitude)

        geo_content = self.cache.get(target_key)
        if geo_content is not None:
            geo_tag = dict()
            geo_tag["stateID"] = geo_content[2]
            geo_tag["stateName"] = full_state_name
            geo_tag["countyID"] = geo_content[1]
            geo_tag["countyName"] = geo_content[3]
            geo_tag["cityID"] = geo_content[0]
            geo_tag["cityName"] = city_state_name.split(",")[0]
            geo_tag["coordinate"] = self.coord
            geo_tag["source"] = flag

            self.data['geo_tag'] = geo_tag
            print(geo_tag)
            
        else:
            raise ValueError("Not find county.")

    def __infer_geo_from_place(self):
        place = self.data.get('place')
        if place is not None:
            # extract the city and state abbrev
            city_state_name = place.get('full_name')
            if city_state_name is None:
                try:
                    raise KeyError()
                except KeyError:
                    print("Not find full_name key in place.")
                return

            self.__extract_geo_tag_from_city_and_state(city_state_name, "place")

    def __infer_geo_from_coord(self):
        # geopy require (Lat, Long)
        t = (self.coord[1], self.coord[0])
        location = self.geo_locator.reverse(t)

        if location is None:
            self.data['geo_tag'] = {"coordinate": self.coord}
            return

        # make up the (city, state) as key to search
        target_key = location.address.split(",")[3].strip() + ', ' + location.address.split(",")[5].strip()
        geo_content = self.cache.get(target_key)

        if geo_content is not None:
            geo_tag = dict()
            geo_tag["stateID"] = geo_content[2]
            geo_tag["stateName"] = location.address.split(",")[5].strip()
            geo_tag["countyID"] = geo_content[1]
            geo_tag["countyName"] = geo_content[3]
            geo_tag["cityID"] = geo_content[0]
            geo_tag["cityName"] = location.address.split(",")[3].strip()
            geo_tag["coordinate"] = self.coord
            geo_tag["source"] = "coordinate"

            self.data['geo_tag'] = geo_tag
            print(geo_tag)

        else:
            raise ValueError("Not find county.")

    def __infer_geo_from_user(self):
        user = self.data.get('user')
        if user is not None:
            # extract the city and state abbrev
            city_state_name = user.get('location')
            if city_state_name is None:
                try:
                    raise KeyError()
                except KeyError:
                    print("Not find full_name key in place.")
                return

            self.__extract_geo_tag_from_city_and_state(city_state_name, "user")
        else:
            # The methods above are all not working, for now we skip.
            # TODO: Use NLP to infer geo_tag from text in tweet.
            pass

    def tag_one_tweet(self, tweet_json: dict):
        # call function
        self.init_geo_cache()
        self.data = tweet_json

        # get the coordinates
        self.coord = self.get_coordinate()

        # get the city, county, state
        # 1. check the place field
        self.__infer_geo_from_place()

        if 'geo_tag' not in self.data:
            # 2. Check the coordinate.
            if self.coord:
                self.__infer_geo_from_coord()
            else:
                # 3. infer from the "User" field
                self.__infer_geo_from_user()

        return self.data

if __name__ == '__main__':
    # test case
    tweet_datafile = open("example.json", "r", encoding='UTF-8')
    tweet_data = json.load(tweet_datafile)
    tweet_datafile.close()

    # call the function.
    twitter_json_tagger = TwitterJSONTagger()
    twitter_json_tagger.tag_one_tweet(tweet_data)

