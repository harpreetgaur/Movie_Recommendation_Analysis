import pymongo

collection = pymongo.MongoClient('mongodb://localhost:27017/')['wows']['na_player_db']

print(collection.count_documents({}))


counter = 0
for d in collection.find({}):
    counter += 1
    print(str(counter) + ': ' + d['player_name'])
    if d['set_private'] is False and d['removed'] is False:
        collection.update_one({'_id': d['_id']}, {'$set': {"overall_stats.win_rate": d['overall_stats']['win_rate'] / 100}})
        collection.update_one({'_id': d['_id']}, {'$set': {"overall_stats.battles_survived": d['overall_stats']['battles_survived'] / 100}})
        ship_stats = d['ship_stats']
        for s in ship_stats:
            s['win_rate'] = s['win_rate'] / 100
        collection.update_one({'_id': d['_id']}, {'$set': {"ship_stats": ship_stats}})
