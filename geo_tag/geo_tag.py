import json
import os
import pickle

from geopy.geocoders import Nominatim

class TwitterJSONTagger():
    def __init__(self):
        # data is the tweet json which we add geo_tag.
        self.data = dict()
        # cache is the hashmap { (city, state), other geo info}
        self.cache = dict()
        # coord is the (longitude, latitude)
        self.coord = tuple()
        # self.abbrev_us_state is the relation between the state's full name and abbrev.
        with open('us_state_abbrev.json', 'r') as inf:
            self.abbrev_us_state = eval(inf.read())

    def init_geo_cache(self, city_json):
        # check pickle module
        filename = "cache_geo_hashmap"
        if os.path.exists(filename):
            cache_file = open(filename, 'rb')
            self.cache = pickle.load(cache_file)
            return
        else:
            feature = city_json.get('features')
            for f in feature:
                item = f.get('properties')
                # store the city, state as key; (cityID, countyID, stateID, countyName) as value.
                if item["name"] is not None and item["stateName"] is not None:
                    _city_state = item["name"] + ", " + item["stateName"]
                    # key, value
                    self.cache[_city_state] = (item["cityID"], item["countyID"], item["stateID"], item["countyName"])

                else:
                    print("[ERROR] city.json data.")
                    return

            # store it using pickle
            cache_file = open(filename, 'wb')
            pickle.dump(self.cache, cache_file)

    def get_coordinate(self):
        coord = self.data.get('coordinates')
        place = self.data.get('place')

        if coord is not None:
            self.coord = coord.get('coordinates')
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
                    print("[ERROR] invalid coordinates of SW and NE.")
                    return

                # calculate the central point in the rectangle
                self.coord = ((swLog+neLog) / 2, (swLat+neLat) / 2)

    # help function
    def extract_geo_tag_from_city_and_state(self, city_state_name, flag):
        if city_state_name is None:
            print("[ERROR] There is no city_name.")
            return

        if "," not in city_state_name:
            print("[ERROR] parse the location name.")
            return

        state_of_city = city_state_name.split(",")[1]
        target_state = str(state_of_city).strip()
        full_state_name = self.abbrev_us_state[target_state]

        # search it in the cache(hashmap) to get the county's name
        target_key = city_state_name.split(",")[0] + ', ' + full_state_name

        if not self.coord:
            # self.coord is empty now
            geo_locator = Nominatim()
            location = geo_locator.geocode(target_key)
            self.coord = (location.longitude, location.latitude)

        geo_content = self.cache.get(target_key)
        if geo_content is not None:
            geo_tuple = dict()
            geo_tuple["stateID"] = geo_content[2]
            geo_tuple["stateName"] = full_state_name
            geo_tuple["countyID"] = geo_content[1]
            geo_tuple["countyName"] = geo_content[3]
            geo_tuple["cityID"] = geo_content[0]
            geo_tuple["cityName"] = city_state_name.split(",")[0]
            geo_tuple["coordinate"] = self.coord
            geo_tuple["source"] = flag

            self.data['geo_tag'] = geo_tuple
            print(geo_tuple)
            
        else:
            print("[ERROR] Not find the county of", target_key)

    def infer_geo_from_place(self):
        place = self.data.get('place')
        if place is not None:
            # extract the city and state abbrev
            city_state_name = place.get('full_name')
            self.extract_geo_tag_from_city_and_state(city_state_name, "place")

    def infer_geo_from_coord(self):
        geo_locator = Nominatim()
        # geopy require (Lat, Long)
        t = (self.coord[1], self.coord[0])
        location = geo_locator.reverse(t)

        if location is None:
            self.data['geo_tag'] = {"coordinate": self.coord}
            return

        # make up the (city, state) as key to search
        target_key = location.address.split(",")[3].strip() + ', ' + location.address.split(",")[5].strip()
        geo_content = self.cache.get(target_key)

        if geo_content is not None:
            geo_tuple = dict()
            geo_tuple["stateID"] = geo_content[2]
            geo_tuple["stateName"] = location.address.split(",")[5].strip()
            geo_tuple["countyID"] = geo_content[1]
            geo_tuple["countyName"] = geo_content[3]
            geo_tuple["cityID"] = geo_content[0]
            geo_tuple["cityName"] = location.address.split(",")[3].strip()
            geo_tuple["coordinate"] = self.coord
            geo_tuple["source"] = "coordinate"

            self.data['geo_tag'] = geo_tuple
            print(geo_tuple)

        else:
            print("[ERROR] Not find the county of", target_key)
            self.data['geo_tag'] = {"coordinate": self.coord}

    def infer_geo_from_user(self):
        user = self.data.get('user')
        if user is not None:
            # extract the city and state abbrev
            city_state_name = user.get('location')
            self.extract_geo_tag_from_city_and_state(city_state_name, "user")
        else:
            # The methods above are all not working, for now we skip.
            # TODO: Use NLP to infer geo_tag from text in tweet.
            self.data['geo_tag'] = dict()

    def tag_one_tweet(self, tweet_json, city_json):
        # call function
        self.init_geo_cache(city_json)
        self.data = tweet_json

        # get the coordinates
        self.get_coordinate()

        # get the city, county, state
        # 1. check the place field
        self.infer_geo_from_place()

        if 'geo_tag' not in self.data.keys():
            # 2. Check the coordinate.
            if self.coord:
                self.infer_geo_from_coord()
            else:
                # 3. infer from the "User" field
                self.infer_geo_from_user()

        return self.data

if __name__ == '__main__':
    # test case
    city_datafile = open("city.json", "r", encoding='UTF-8')
    city_data = json.load(city_datafile)
    city_datafile.close()

    tweet_datafile = open("example.json", "r", encoding='UTF-8')
    tweet_data = json.load(tweet_datafile)
    tweet_datafile.close()

    # call the function.
    twitter_json_tagger = TwitterJSONTagger()
    twitter_json_tagger.tag_one_tweet(tweet_data, city_data)

