import logging
import pickle
import os
import json

import rootpath

rootpath.append()

from shapely.geometry import Point, Polygon
from geopy.geocoders import Nominatim

class TwitterJSONTagger():
    def __init__(self):
        print("TwitterJSONTagger()")
        # data is the geo tagged json.
        self.data = dict()
        # cache is the hashmap { (city, state), county }
        self.cache = dict()
        # coord is the (longitude, latitude)
        self.coord = tuple()
        # {polygen, geo_tag}
        self.cache_polygon = list()
        # self.abbrev_us_state is the relation between the state's full name and abbrev.
        with open('us_state_abbrev.json', 'r') as inf:
            self.abbrev_us_state = eval(inf.read())

    def init_geo_cache(self, city_json):
        print("init_geo_cache")
        # check pickle module
        filename = "cache_geo_hashmap"
        if os.path.exists(filename):
            cache_file = open(filename, 'rb')
            self.cache = pickle.load(cache_file)
            return
        else:
            feature = city_json.get('features')
            counter = 0
            for f in feature:
                item = f.get('properties')
                counter += 1
                # print(item["name"])
                # store the city, state as key; (cityID, countyID, stateID, countyName) as value.
                if item["name"] is not None and item["stateName"] is not None:
                    _city_state = item["name"] + ", " + item["stateName"]
                    # key, value
                    self.cache[_city_state] = (item["cityID"], item["countyID"], item["stateID"], item["countyName"])

                else:
                    print("[ERROR] city.json data.")
                    return

            # print("Length of hashmap: ", len(self.cache), counter)
            # store it using pickle

            # TODO: COMMENT FOR DEBUG
            # cache_file = open(filename, 'wb')
            # pickle.dump(self.cache, cache_file)

    def get_coordinate(self):
        coord = self.data.get('coordinates')
        place = self.data.get('place')

        if coord is not None:
            self.coord = coord.get('coordinates')
            print("1.", self.coord)
        else:
            # 2. get the central point of the bounding_box
            if place.get('bounding_box') is not None:
                bounding_box = place.get('bounding_box').get('coordinates')[0]
                # print(bounding_box)  [longitude, latitude]
                ''' Twitter has some wield format historically, though it still rectangle, 
                but it is not always in (sw, se, ne,nw) order
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
                    print("[ERROR] coordinates of SW and NE.")
                    return

                # calculate the central point in the rectangle
                self.coord = ((swLog + neLog) / 2, (swLat + neLat) / 2)
                print("2.", self.coord)
            else:
                print("Can not get coordinate.")

    # help function
    def extract_geotag_from_city_and_state(self, city_state_name, flag):
        if city_state_name is None:
            print("[ERROR] There is no city_name in place.")
            return
        print(city_state_name)

        if "," not in city_state_name:
            print("[ERROR] parse the location name.")
            return

        state_of_city = city_state_name.split(",")[1]
        target_state = str(state_of_city).strip()
        full_state_name = self.abbrev_us_state[target_state]

        # search it in the cache(hashmap) to get the county's name
        target_key = city_state_name.split(",")[0] + ', ' + full_state_name
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
            print(flag, geo_tuple)
            # print(self.data)

        else:
            print("[ERROR] Not find the county of", target_key)


    def infer_geo_from_place(self):
        place = self.data.get('place')
        if place is not None:
            # extract the city and state abbrev
            city_state_name = place.get('full_name')
            self.extract_geotag_from_city_and_state(city_state_name, "place")


    ''' # May Abandon 
    def infer_geo_from_coord(self, city_json):
        print("infer_geo_from_coord")
        # check pickle module
        filename = "cache_geo_polygen"
        if os.path.exists(filename):
            cache_file = open(filename, 'rb')
            self.cache = pickle.load(cache_file)
            return
        else:
            feature = city_json.get('features')
            for f in feature:
                item = f.get('geometry')
                type = item.get('type')
                coords = []

                if type == "Polygon":
                    print("1")
                    if len(item["coordinates"][0]) > 3:
                        for i in item["coordinates"][0]:
                            p = Point(i[0], i[1])
                            coords.append(p)

                elif type == "MultiPolygon":
                    print("2")
                    for i in item["coordinates"][0]:
                        if len(i) > 3:
                            for ii in i:
                                p = Point(ii[0], ii[1])
                                coords.append(p)

                else:
                    print("Wrong type geojson format.")
                    return

                # make polygon as key
                poly = Polygon(coords)
                geo_tuple = dict()
                geo_info = f.get('properties')
                geo_tuple['cityName'] = geo_info["name"]
                geo_tuple['cityID'] = geo_info["cityID"]
                geo_tuple['countyName'] = geo_info["countyName"]
                geo_tuple['stateName'] = geo_info["stateName"]
                geo_tuple['stateID'] = geo_info["stateID"]

                # TODO: get county ID

                self.cache_polygon.append([poly, geo_tuple])
                # TODO: COMMENT FOR DEBUG
                # cache_file = open(filename, 'wb')
                # pickle.dump(self.cache_polygon, cache_file)
                print(self.cache_polygon)
    '''

    def infer_geo_from_coord(self):
        geolocator = Nominatim()
        # geopy require (Lat, Long)
        t = (self.coord[1], self.coord[0])
        location = geolocator.reverse(t)
        print(location.address.split(","))
        # make up the (city, state) as key to search
        target_key = location.address.split(",")[3].strip() + ', ' + location.address.split(",")[5].strip()
        print(target_key)
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
            print("[Coordinate]", geo_tuple)
            # print(self.data)

        else:
            print("Not find the county of", target_key)

    def infer_geo_from_user(self):
        user = self.data.get('user')
        if user is not None:
            # extract the city and state abbrev
            city_state_name = user.get('location')
            self.extract_geotag_from_city_and_state(city_state_name, "[user]")

    def tag_one_tweet(self, tweet_json, city_json):
        print("tag_one_tweet")

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
            print('No place field in this tweet. Infer...')
            # self.infer_geo_from_coord(city_json)

            # if coordinate exists, use it to infer.
            if self.coord:
                self.infer_geo_from_coord()
            else:
                # 3. infer from the "User" field
                self.infer_geo_from_user()

        # print(self.data.keys())
        return self.data


if __name__ == '__main__':
    # test case
    city_datafile = open("city.json", "r", encoding='UTF-8')
    city_data = json.load(city_datafile)
    city_datafile.close()

    tweet_datafile = open("example.json", "r", encoding='UTF-8')
    tweet_data = json.load(tweet_datafile)
    tweet_datafile.close()

    twitter_json_tagger = TwitterJSONTagger()
    twitter_json_tagger.tag_one_tweet(tweet_data, city_data)

