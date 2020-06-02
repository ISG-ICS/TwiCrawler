import logging
import json

import rootpath

rootpath.append()

class TwitterJSONTagger():
    def __init__(self):
        print("TwitterJSONTagger()")
        self.data = dict()

    def tag_one_tweet(self, tweet_json):
        print("tag_one_tweet")
        self.data = tweet_json
        place = self.data.get('place')
        if place is not None:
            city_name = place.get('name')
            print(city_name)
            # TODO: use this to find corresponding county and state information.

        else:
            print('No place field in this tweet.')
            # TODO: Continue to search "user" field.

        # print(self.data.keys())
        return self.data


if __name__ == '__main__':
    # test case
    tweet_datafile = open("example.json", "r", encoding='UTF-8')
    tweet_data = json.load(tweet_datafile)
    tweet_datafile.close()

    ''' json_str = '{"country":"United States",' \
               '"country_code":"US",' \
               '"name":"Washington, DC",' \
               '"id":"01fbe706f872cb32"}'
    '''

    twitter_json_tagger = TwitterJSONTagger()
    twitter_json_tagger.tag_one_tweet(tweet_data)

