import os
import json
from pymongo import MongoClient


client = MongoClient('localhost', 27017)
db = client['wows']
collection = db['na-basic']
player_list = os.listdir('../PlayerDB/NA_BASIC')
count = 0
for l in player_list:
    with open('../PlayerDB/NA_BASIC/' + l) as json_file:
        player_json = json.load(json_file)

    player_json['_id'] = player_json['player_id']

    with open('../PlayerDB/NA_BASIC/' + l, 'w') as json_file:
        json.dump(player_json, json_file, indent=4)

    count += 1
    print(str(count) + ': ' + player_json['player_id'])
