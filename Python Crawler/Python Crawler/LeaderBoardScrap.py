import re
import json
import time
import pymongo
import requests
from bs4 import BeautifulSoup


def main():
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:72.0) Gecko/20100101 Firefox/72.0'}
    cookies = load_cookies()
    url = 'https://asia.wows-numbers.com/ranking/type,all/'

    current_record = 0
    page_num = 890
    for pn in range(418, page_num+1):
        start_time = time.time()
        params = {'p': pn}
        response = requests.get(url, headers=headers, params=params, cookies=cookies)
        player_number = parse_data_to_json(response.content.decode('utf-8'))
        current_record += player_number
        print('[' + str(round(pn / page_num * 100, 4)) + '%]: P[' + str(pn) + '] of [' + str(page_num) +
              '] : <' + str(player_number) + '> => <' + str(current_record) + '>' +
              '\tTime used:' + str(round(time.time() - start_time, 3)) + 's | [URL]: ' + response.url)
        # time.sleep(1)


def load_cookies():
    with open('cookies/leaderboard_cookies.json') as json_file:
        cookies = json.load(json_file)
    return cookies['Request Cookies']


def parse_data_to_json(content):
    bs = BeautifulSoup(content, 'lxml')
    player_rows = bs.find_all('tr', {'class': 'cells-middle'})
    collection = pymongo.MongoClient('mongodb://localhost:27017/')['wows']['as_player_list']

    player_counter = 0
    for player in player_rows:
        player_name = player['data-nickname']

        if len(player.select('tr > td')[1].select('td > a')) > 1:
            player_id = re.search('player/(.+?),', player.select('tr > td')[1].select('td > a')[1]['href']).group(1)
            player_clan = player.select('tr > td')[1].select('td > a')[0].decode_contents()[1:-1]
        else:
            player_id = re.search('player/(.+?),', player.select('tr > td')[1].select('td > a')[0]['href']).group(1)
            player_clan = ''

        player_data = {
            '_id': player_id,
            'player_name': player_name,
            'player_id': player_id,
            'clan': player_clan,
            'scraped': False
        }

        if collection.count_documents({"_id": player_id}) == 0:
            collection.insert_one(player_data)
            player_counter += 1

        # file_name = 'players/' + str(player_id) + '.json'
        #
        # with open(file_name, 'w', encoding='utf-8') as player_file:
        #     json.dump(player_data, player_file, indent=4)
    return player_counter


if __name__ == '__main__':
    main()
