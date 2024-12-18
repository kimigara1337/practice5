import json
import csv
from pymongo import MongoClient

# Предметная область  "Статистика матчей аккаунта в CS:GO"
def connect_to_mongodb():
    client = MongoClient('localhost', 27017)
    db = client['CSGO_database']
    collection = db['CSGO_collection']
    return collection


def read_json_file(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        data = json.load(file)
    return data


def read_csv_file(file_path):
    with open(file_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        data = list(reader)
    return data


def converted_data_1(data_1):
    converted_data = {
        'Map': data_1['Map'],
        'Day': int(data_1['Day']),
        'Month': int(data_1['Month']),
        'Year': int(data_1['Year']),
        'Date': data_1['Date'],
        'Wait Time(s)': int(data_1['Wait Time(s)']),
        'Match Time(s)': int(data_1[' Match Time(s)']),
    }
    return converted_data


def converted_data_2(data_2):
    converted_data = {
        'Team A Rounds': int(data_2['Team A Rounds']),
        'Team B Rounds': int(data_2['Team B Rounds']),
        'Ping': int(data_2["Ping"]),
        'Kills': int(data_2['Kills']),
        'Assists': int(data_2['Assists']),
        'Deaths': int(data_2['Deaths']),
        "Mvp's": int(data_2["Mvp's"]),
        'HS%': float(data_2['HS%']),
        'Points': int(data_2['Points']),
        'Result': data_2['Result']
    }
    return converted_data


def combine_and_insert_data(collection, data_1, data_2):
    combined_data = []
    for i in range(len(data_1)):
        converted_data_12 = converted_data_1(data_1[i])
        converted_data_22 = converted_data_2(data_2[i])
        converted_data_12.update(converted_data_22)
        combined_data.append(converted_data_12)
    collection.insert_many(combined_data)


def query_and_write_results(collection):
    # Запрос 1: вывод первых 10 записей, отсортированных по убыванию по полю Kills
    result1 = list(collection.find().sort('Kills', -1).limit(10))
    write_data_to_json(result1, 'query1_output.json')

    # Запрос 2: вывод первых 10 записей, отсортированных по возрастанию по полю Kills
    result2 = list(collection.find().sort('Kills', 1).limit(10))
    write_data_to_json(result2, 'query2_output.json')

    # Запрос 3: вывод первых 15 записей, отфильтрованных по предикату HS% > 50, отсортировать по убыванию по полю Points
    result3 = list(collection.find({'HS%': {'$gt': 50}}).sort('Points', 1).limit(15))
    write_data_to_json(result3, 'query3_output.json')

    # Запрос 4: вывод первых 10 записей, отфильтрованных по сложному предикату: (записи только из произвольного поля Year, записи только из трех произвольно взятых Map),
    # отсортировать по возрастанию по полю Points
    result4 = list(
        collection.find({'Year': {'$in': [2015, 2016]}, 'Map': {'$in': ["Dust II", "Mirage", "Overpass"]}}).sort('Points', 1).limit(
            10))
    write_data_to_json(result4, 'query4_output.json')

    # Запрос 5: вывод количества записей, получаемых в результате следующей фильтрации (kills в произвольном диапазоне, year в [2015,2016], 85 < Ping <= 100 || 101 < Ping < 105).
    result5_count = collection.count_documents({
        '$or': [
            {'$and': [
                {'Kills': {'$gt': 20}},
                {'Year': {'$in': [2015, 2016]}},
                {'$or': [
                    {'Ping': {'$gt': 85, '$lte': 100}},
                    {'Ping': {'$gt': 101, '$lt': 105}}
                ]}
            ]}
        ]})
    write_integer_to_json(result5_count, 'query5_output.json')

    # Запрос 6: вывод минимальной, средней, максимальной Kills
    kills_stats = collection.aggregate([
        {"$group": {
            "_id": "$Map",
            "minKills": {"$min": "$Kills"},
            "avgKills": {"$avg": "$Kills"},
            "maxKills": {"$max": "$Kills"}
        }}
    ])
    write_data_to_json(list(kills_stats), 'query6_output.json')

    # Запрос 7: вывод минимальной, средней, максимальной Kills по произвольным полям Map
    map_kills_stats = collection.aggregate([
        {"$match": {"Map": {"$in": ["Mirage", "Cobblestone", "Cache", "Overpass", "Dust II", "Inferno"]}}},
        {"$group": {
            "_id": "$Map",
            "minKills": {"$min": "$Kills"},
            "avgKills": {"$avg": "$Kills"},
            "maxKills": {"$max": "$Kills"}
        }}
    ])
    write_data_to_json(list(map_kills_stats), 'query7_output.json')

    # Запрос 8: вывод минимального, среднего, максимального Deaths по произвольным полям Map
    map_deaths_stats = collection.aggregate([
        {"$match": {"Map": {"$in": ["Mirage", "Cobblestone", "Cache", "Overpass", "Dust II", "Inferno"]}}},
        {"$group": {
            "_id": "$Map",
            "minDeaths": {"$min": "$Deaths"},
            "avgDeaths": {"$avg": "$Deaths"},
            "maxDeaths": {"$max": "$Deaths"}
        }}
    ])
    write_data_to_json(list(map_deaths_stats), 'query8_output.json')

    # Запрос 9: вывод минимального, среднего, максимального HS% по произвольным полям Map
    map_hs_stats = collection.aggregate([
        {"$match": {"Map": {"$in": ["Mirage", "Cobblestone", "Cache", "Overpass", "Dust II", "Inferno"]}}},
        {"$group": {
            "_id": "$Map",
            "minHS": {"$min": "$HS%"},
            "avgHS": {"$avg": "$HS%"},
            "maxHS": {"$max": "$HS%"}
        }}
    ])
    write_data_to_json(list(map_hs_stats), 'query9_output.json')

    # Запрос 10: вывод минимального, среднего, максимального Assists по полю Map, также поле Result должно быть Win
    map_assists_stats = collection.aggregate([
        {"$match": {"Map": {"$in": ["Mirage", "Cobblestone", "Cache", "Overpass", "Dust II", "Inferno"]},
                    "Result": "Win"}},
        {"$group": {
            "_id": "$Map",
            "minAssists": {"$min": "$Assists"},
            "avgAssists": {"$avg": "$Assists"},
            "maxAssists": {"$max": "$Assists"}
        }}
    ])
    write_data_to_json(list(map_assists_stats), 'query10_output.json')

    # Запрос 11: удалить из коллекции документы по предикату: Ping < 20 || Ping > 190
    collection.delete_many({'$or': [{'Ping': {'$lt': 20}}, {'Ping': {'$gt': 190}}]})

    # Запрос 12: увеличить поле награды (Mvp's) все на 1
    collection.update_many({}, {'$inc': {"Mvp's": 1}})

    # Запрос 13: поднять Assists на 5% для полей Map со следующими картами - Nuke и Cache
    collection.update_many({'Map': {'$in': ['Nuke', 'Cache']}}, {'$mul': {'Assists': 1.05}})

    # Запрос 14: поднять Assists на 7% для полей Map со следующими картами - Dust II и Overpass
    collection.update_many({'Map': {'$in': ['Dust II', 'Overpass']}}, {'$mul': {'Assists': 1.07}})

    # Запрос 15: удалить из коллекции записи по произвольному предикату
    collection.delete_many({'Map': 'Random Map'})
    result = list(collection.find())
    write_data_to_json(result, 'final_output.json')


def write_data_to_json(data, file_name):
    result = []
    for item in data:
        item_dict = {}
        for key, value in item.items():
            if key == '_id':
                item_dict[key] = str(value)
            else:
                item_dict[key] = value
        result.append(item_dict)

    with open(file_name, 'w', encoding='utf-8') as outfile:
        json.dump(result, outfile, default=str, indent=4)


def write_integer_to_json(data, file_name):
    result = {'count': data}

    with open(file_name, 'w', encoding='utf-8') as outfile:
        json.dump(result, outfile, default=str, indent=4)


collection = connect_to_mongodb()
data_1 = read_json_file('CSGOStatistics1.json')
data_2 = read_csv_file('CSGOStatistics2.csv')
combine_and_insert_data(collection, data_1, data_2)
query_and_write_results(collection)