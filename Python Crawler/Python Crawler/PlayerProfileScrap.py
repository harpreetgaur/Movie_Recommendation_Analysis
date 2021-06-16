import re
import time
import html
import json
import random
import pymongo
import requests
from bs4 import BeautifulSoup


def main():
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:73.0) Gecko/20100101 Firefox/73.0'}
    cookies = load_cookies()
    player_list = get_player_list()
    collection = pymongo.MongoClient('mongodb://localhost:27017/')['wows']['na_player_db']  # !!!!!!!!!!!!!!!!!!!!!!!!!

    for p in player_list:
        print('Player [' + p['player_name'] + '] ', end='')
        start_time = time.time()
        player = player_page_request(headers, cookies, p['player_id'], p['player_name'], p['clan'])
        if player != 0:
            collection.insert_one(player)
        else:
            copy_to_special(p['player_id'])
        update_scraped(p['player_id'])
        t = time.time() - start_time
        print('finished importing:\t %s s' % t, end=''),
        if t < 1:
            s = random.uniform(1.0, 1.35) - t
            time.sleep(s)
            print(' + %s s' % s)
        else:
            print()


def load_cookies():
    with open('cookies/player_cookies.json') as json_file:
        cookies = json.load(json_file)
    return cookies['Request Cookies']


def get_player_list():
    """
    get the list of player id, name, and clan

    :return:  the list of player id, name, and clan
    """
    return list(
        pymongo.MongoClient('mongodb://localhost:27017/')['wows']['na_player_list'].find(  # !!!!!!!!!!!!!!!!!!!!!!!!!
            {'scraped': False}, {'_id': 0, 'player_id': 1, 'player_name': 1, 'clan': 1}
        )
    )


def update_scraped(_id):
    pymongo.MongoClient('mongodb://localhost:27017/')['wows']['na_player_list'].update_one(
        {'_id': _id}, {'$set': {'scraped': True}}
    )


def copy_to_special(_id):
    pymongo.MongoClient('mongodb://localhost:27017/')['wows']['special_player_db'].insert_one(
        pymongo.MongoClient('mongodb://localhost:27017/')['wows']['na_player_list'].find({'_id': _id})[0]
    )

    pymongo.MongoClient('mongodb://localhost:27017/')['wows']['special_player_db'].update_one(
        {'_id': _id}, {'$set': {'server': 'NA'}}
    )


def player_page_request(headers, cookies, player_id, player_name, clan):
    url = 'https://na.wows-numbers.com/player/' + player_id + ',' + player_name + '/'  # !!!!!!!!!!!!!!!!!!!!!!!!!
    response = requests.get(url, headers=headers, cookies=cookies)
    return parse_page_to_json(response.content.decode('utf-8'), player_id, player_name, clan)


def parse_page_to_json(content, player_id, player_name, clan):
    soup = BeautifulSoup(content, 'lxml')

    if len(soup.find_all('div', {'class': 'alert-danger'})) > 0:
        return {
            '_id': player_id,
            'player_id': player_id,
            'player_name': player_name,
            'clan': clan,
            'set_private': True,
            'removed': False
        }
    elif len(soup.find_all('div', {'class': 'table-responsive'})) == 0:
        return {
            '_id': player_id,
            'player_id': player_id,
            'player_name': player_name,
            'clan': clan,
            'set_private': False,
            'removed': True
        }
    elif int(re.sub(r'\s+', '', soup.find_all('div', {'class': 'table-responsive'})[0].select('tr')[1].select('td')[1].select('span')[0].decode_contents())) == 0:
        return 0
    else:
        # ----- overall random battles stats ----- #
        overall_stats = get_overall_data(soup)

        # ----- warship stats by types ----- #
        ship_type_stats = get_ship_type_stats(soup)

        # ----- warship stats by tiers ----- #
        ship_tier_stats = get_ship_tier_stats(soup)

        # ----- weapons stats ----- #
        weapons_stats = get_weapons_stats(soup)

        # ----- records ----- #
        records = get_records(soup)

        # ----- important moment ----- #
        important_moment = get_important_moment(soup)

        # ----- online history ----- #
        online_history = get_online_history(soup)

        # ----- ship stats ----- #
        ship_stats = get_ship_stats(soup)

        # ----- rank history ----- #
        rank_history = get_rank_history(soup)

        return {
            '_id': player_id,
            'player_id': player_id,
            'player_name': player_name,
            'clan': clan,
            'set_private': False,
            'removed': False,
            'overall_stats': overall_stats,
            'ship_type_stats': ship_type_stats,
            'ship_tier_stats': ship_tier_stats,
            'weapons_stats': weapons_stats,
            'records': records,
            'important_moment': important_moment,
            'online_history': online_history,
            'ship_stats': ship_stats,
            'rank_history': rank_history
        }


def get_overall_data(soup):
    overall_tb = soup.find_all('div', {'class': 'table-responsive'})[0].select('tr')

    return {
        'battles': int(re.sub(r'\s+', '', overall_tb[1].select('td')[1].select('span')[0].decode_contents())),
        'win_rate': float(
            re.search(r'^(.+?)%', overall_tb[2].select('td')[1].select('span')[0].decode_contents()).group(1)
        ) / 100,
        'personal_rating': int(
            re.search(
                r'^(.+?)<br', re.sub(r'\s+', '', overall_tb[3].select('td')[1].select('span')[0].decode_contents())
            ).group(1)
        ) if overall_tb[3].select('td')[1].decode_contents().strip() != '-' else 0,
        'battles_survived': float(
            re.search(r'^(.+?)%', overall_tb[4].select('td')[1].decode_contents().strip()).group(1)
        ) / 100,
        'avg_battle_values': {
            'avg_dmg': int(
                re.sub(r'\s+', '', overall_tb[6].select('td')[1].select('span')[0].decode_contents().strip())
            ),
            'avg_frags': float(overall_tb[7].select('td')[1].select('span')[0].decode_contents().strip()),
            'avg_shoot_down': float(overall_tb[8].select('td')[1].select('span')[0].decode_contents().strip()),
            'avg_exp': int(re.sub(r'\s+', '', overall_tb[9].select('td')[1].decode_contents().strip())),
            'avg_kd': float(re.sub(r'\s+', '', overall_tb[10].select('td')[1].decode_contents().strip())),
            'avg_spot': float(re.sub(r'\s+', '', overall_tb[11].select('td')[1].decode_contents().strip())),
            'avg_spot_dmg': int(re.sub(r'\s+', '', overall_tb[12].select('td')[1].decode_contents().strip())),
            'avg_tier': float(re.sub(r'\s+', '', overall_tb[13].select('td')[1].decode_contents().strip()))
        }
    }


def get_ship_type_stats(soup):
    ship_type_stats_list = soup.find_all('div', {'class': 'table-responsivee'})[0].select('table tbody tr')
    ship_type_stats = []
    for t in ship_type_stats_list:
        td = t.select('td')
        temp = {'type': td[0].select('a')[0].decode_contents(),
                'battles': int(td[1].decode_contents().replace(' ', '')),
                'win_rate': float(td[2].select('span')[0].decode_contents()[:-1]) / 100,
                'personal_rating': int(td[3].select('span')[0].decode_contents().replace(' ', '')),
                'avg_frags': float(td[4].select('span')[0].decode_contents()),
                'avg_dmg': int(td[5].select('span')[0].decode_contents().replace(' ', '')),
                'avg_shoot_down': float(td[6].select('span')[0].decode_contents()),
                'max_frags': {
                    'max_frags_number': int(re.search(r'^(.+?)<br', td[7].decode_contents().strip()).group(1)),
                    'max_frags_ship': html.unescape(td[7].select('small')[0].decode_contents())
                } if td[7].decode_contents().strip() != '' else {
                    'max_frags_number': 0,
                    'max_frags_ship': ''
                },
                'max_dmg': {
                    'max_dmg_number': int(
                        re.search(r'^(.+?)<br', re.sub(r'\s+', '', td[8].decode_contents())).group(1)
                    ),
                    'max_dmg_ship': html.unescape(td[8].select('small')[0].decode_contents())
                } if td[8].decode_contents().strip() != '' else {
                    'max_dmg_number': 0,
                    'max_dmg_ship': ''
                },
                'max_shoot_down': {
                    'max_shoot_down_number': int(re.search(r'^(.+?)<br', td[9].decode_contents().strip()).group(1)),
                    'max_shoot_down_ship': html.unescape(td[9].select('small')[0].decode_contents())
                } if td[9].decode_contents().strip() != '' else {
                    'max_shoot_down_number': 0,
                    'max_shoot_down_ship': ''
                }
                }

        ship_type_stats.append(temp)

    return ship_type_stats


def get_ship_tier_stats(soup):
    ship_tier_stats_list = soup.find_all('div', {'class': 'table-responsivee'})[1].select('table tbody tr')
    ship_tier_stats = []
    for t in ship_tier_stats_list:
        temp = {}
        td = t.select('td')
        temp['tier'] = td[0].decode_contents().strip()
        temp['battles'] = int(td[1].decode_contents().replace(' ', ''))
        temp['win_rate'] = float(td[2].select('span')[0].decode_contents()[:-1]) / 100
        temp['personal_rating'] = int(td[3].select('span')[0].decode_contents().replace(' ', ''))
        temp['avg_frags'] = float(td[4].select('span')[0].decode_contents())
        temp['avg_dmg'] = int(td[5].select('span')[0].decode_contents().replace(' ', ''))
        temp['avg_shoot_down'] = float(td[6].select('span')[0].decode_contents())
        temp['max_frags'] = {
            'max_frags_number': int(re.search(r'^(.+?)<br', td[7].decode_contents().strip()).group(1)),
            'max_frags_ship': html.unescape(td[7].select('small')[0].decode_contents())
        } if td[7].decode_contents().strip() != '' else {
            'max_frags_number': 0,
            'max_frags_ship': ''
        }
        temp['max_dmg'] = {
            'max_dmg_number': int(re.search(r'^(.+?)<br', re.sub(r'\s+', '', td[8].decode_contents())).group(1)),
            'max_dmg_ship': html.unescape(td[8].select('small')[0].decode_contents())
        } if td[8].decode_contents().strip() != '' else {
            'max_dmg_number': 0,
            'max_dmg_ship': ''
        }
        temp['max_shoot_down'] = {
            'max_shoot_down_number': int(re.search(r'^(.+?)<br', td[9].decode_contents().strip()).group(1)),
            'max_shoot_down_ship': html.unescape(td[9].select('small')[0].decode_contents())
        } if td[9].decode_contents().strip() != '' else {
            'max_shoot_down_number': 0,
            'max_shoot_down_ship': ''
        }

        ship_tier_stats.append(temp)

    return ship_tier_stats


def get_weapons_stats(soup):
    weapons_stats_tb = soup.find_all('div', {'class': 'table-responsivee'})[2].select('table tr')[1:]
    weapons_stats_list = []
    for w in weapons_stats_tb:
        temp = {'weapon_type': w.select('td')[0].decode_contents().strip(),
                'total_frags': int(w.select('td')[1].decode_contents().strip()),
                'hit_ratio': float(w.select('td')[2].decode_contents().replace('%', '').strip()) / 100
                if w.select('td')[2].decode_contents().strip() != '-' else 0,
                'max_kills': {
                    'max_kills_num': int(
                        re.search(r'^(.+?)<br', re.sub(r'\s+', '', w.select('td')[3].decode_contents())).group(1)
                    ),
                    'max_kills_ship': html.unescape(w.select('td')[3].select('small')[0].decode_contents())
                    } if w.select('td')[3].decode_contents().strip() != '0' else {
                        'max_kills_num': 0,
                        'max_kills_ship': ''
                    }
                }

        weapons_stats_list.append(temp)

    return weapons_stats_list


def get_records(soup):
    records_tb = soup.select('div.col.col-centered.col-sm-4.col-xs-12 tr')
    records_list = []
    for r in records_tb:
        temp = {
            'type': r.select('td')[0].decode_contents(),
            'record': {
                'record_num': int(
                    re.search(r'^(.+?)<br', re.sub(r'\s+', '', r.select('td')[1].decode_contents())).group(1)
                ),
                'record_ship': html.unescape(r.select('td small')[0].decode_contents().strip())
            } if r.select('td')[1].decode_contents().strip() != '0' else {
                'record_num': 0,
                'record_ship': ''
            }
        }
        records_list.append(temp)

    return records_list


def get_important_moment(soup):
    more_moment = soup.find(id='milestones')
    moment_small_tb = soup.select('div.col.col-centered.col-sm-6')[0].select('tr')
    moment_list = None

    if more_moment is not None:
        moment_list = parse_moment_table(more_moment.select('tr')[1:])
    elif len(moment_small_tb) > 1:
        moment_list = parse_moment_table(moment_small_tb[1:])

    return moment_list


def parse_moment_table(moment_list):
    new_ship_list = []
    new_record_list = []

    for m in moment_list:
        date = m.select('td')[0].decode_contents().split('.')
        if m.select('td')[1].decode_contents().strip() == 'New warship':
            new_ship_list.append({
                'year': int(date[2]),
                'month': int(date[1]),
                'day': int(date[0]),
                'ship': html.unescape(re.search(r'> (.+?)$', str(m.select('td')[3].decode_contents().strip())).group(1))
                if m.select('td')[3].find('img') is not None
                else html.unescape(m.select('td')[3].decode_contents().strip())
            })
        else:
            new_record_list.append({
                'year': int(date[2]),
                'month': int(date[1]),
                'day': int(date[0]),
                'number': int(re.match(r'\d+', re.sub(r'\s+', '', m.select('td')[2].decode_contents())).group(0)),
                'type': ''.join(re.findall(r'\D+', m.select('td')[2].decode_contents().strip())).strip(),
                'ship': html.unescape(re.search(r'> (.+?)$', str(m.select('td')[3].decode_contents().strip())).group(1))
                if m.select('td')[3].find('img') is not None
                else html.unescape(m.select('td')[3].decode_contents().strip())
            })

    return {
        'new_ship_list': new_ship_list,
        'new_record_list': new_record_list
    }


def get_online_history(soup):
    graphs = soup.find_all('div', {'class': 'col-sm-5'})
    has_online_history = False
    chart_id = None
    target_script = None
    for g in graphs:
        if g.select('strong')[0].decode_contents() == 'Battles per month':
            chart_id = g.select('div')[0]['id']
            has_online_history = True
            break

    if not has_online_history:
        return {}
    else:
        scrips = soup.find_all('script')
        for s in scrips:
            if chart_id in str(s):
                target_script = s.decode_contents()

        dates = json.loads(re.search(r'\"ticks\":(.+?)},\"grid\":', target_script).group(1))
        battles_numbers = json.loads(re.search(chart_id + r'\", \[(.+?)\],options', target_script).group(1))
        online_history = []
        for i in range(len(dates)):
            online_history.append({
                'year': int(dates[i][1].split('.')[1]),
                'month': int(dates[i][1].split('.')[0]),
                'battles': int(battles_numbers[i][1])
            })

        return online_history


def get_ship_stats(soup):
    scripts = soup.find_all('script')
    target_script = None

    for s in scripts:
        if 'dataProvider' in str(s):
            target_script = s
            break

    ships_data = json.loads(re.search(
        r'(?s)dataProvider\.ships = (.+?)dataProvider\.accountId', target_script.decode_contents()
    ).group(1).strip()
                            )

    extracted_data = []
    for sd in ships_data:
        extracted_data.append({
            'ship_name': sd['ship']['name'],
            'total_battle': sd['battles_original'],
            'win_rate': sd['win_rate_original'] / 100,
            'avg_dmg': sd['average_damage_original'],
            'avg_frags': sd['average_frags'],
            'avg_plane_kills': sd['average_planes_killed']
        })

    return extracted_data


def get_rank_history(soup):
    rank_tb = soup.select("div.col.col-centered.col-sm-10.col-xs-12 tr")[1:]

    rank_history = []

    if len(rank_tb) > 0:
        for r in rank_tb:
            td = r.select('td')
            rank_history.append({
                "season": td[0].decode_contents().strip(),
                "rank": int(re.search(r'div> (.+?)$', td[1].decode_contents()).group(1).strip()),
                "best_rank": int(td[2].decode_contents().strip()),
                "battles": int(re.sub(r'\s+', '', td[3].decode_contents().strip())),
                "win_rate": float(td[4].select('span')[0].decode_contents().strip()[:-1]) / 100
            })

    return rank_history


if __name__ == '__main__':
    main()
