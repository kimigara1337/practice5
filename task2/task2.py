import pickle
import json
import pymongo
from pymongo import MongoClient


def connect_to_mongodb():
    # Подключение к MongoDB
    client = MongoClient('localhost', 27017)
    db = client['mydatabase']
    collection = db['mycollection']
    return collection


def read_data_from_pickle(file_path):
    # Чтение данных из pickle файла
    with open(file_path, 'rb') as file:
        data = pickle.load(file)
    return data


def write_data_to_json(data, file_path):
    # Запись данных в JSON файл
    with open(file_path, 'w', encoding='utf-8') as file:
        json.dump(data, file, ensure_ascii=False, indent=4)


def query_min_avg_max_salary(collection):
    result = collection.aggregate([
        {
            '$group': {
                '_id': None,
                'min_salary': {'$min': '$salary'},
                'avg_salary': {'$avg': '$salary'},
                'max_salary': {'$max': '$salary'}
            }
        }
    ])
    data = next(result, None)
    return data


def query_professions_count(collection):
    result = collection.aggregate([
        {
            '$group': {
                '_id': '$job',
                'count': {'$sum': 1}
            }
        }
    ])

    output = {}
    for doc in result:
        output[doc['_id']] = doc['count']

    return output


def query_min_avg_max_salary_by_city(collection):
    result = collection.aggregate([
        {
            '$group': {
                '_id': '$city',
                'min_salary': {'$min': '$salary'},
                'avg_salary': {'$avg': '$salary'},
                'max_salary': {'$max': '$salary'}
            }
        }
    ])

    output = {}
    for doc in result:
        output[doc['_id']] = {
            'min_salary': doc['min_salary'],
            'avg_salary': doc['avg_salary'],
            'max_salary': doc['max_salary']
        }

    return output


# Пример основной части программы

# Путь к вашему pickle файлу
file_path = 'task_2_item.pkl'

# Читаем данные из pickle файла
data = read_data_from_pickle(file_path)

# Подключаемся к MongoDB
collection = connect_to_mongodb()

# Удаляем старые записи из коллекции, чтобы избежать дублирования
collection.delete_many({})

# Загружаем данные в MongoDB
collection.insert_many(data)

# Пример выполнения задач:
# Задание 1: Вывод минимальной, средней, максимальной зарплаты
task_1_output = query_min_avg_max_salary(collection)
write_data_to_json(task_1_output, 'task_1_output.json')

# Задание 2: Вывод количества данных по профессиям
task_2_output = query_professions_count(collection)
write_data_to_json(task_2_output, 'task_2_output.json')

# Задание 3: Вывод минимальной, средней, максимальной зарплаты по городу
task_3_output = query_min_avg_max_salary_by_city(collection)
write_data_to_json(task_3_output, 'task_3_output.json')
