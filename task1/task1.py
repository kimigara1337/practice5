import json
import pymongo
from pymongo import MongoClient


def connect_to_mongodb():
    # Подключение к MongoDB
    client = MongoClient('localhost', 27017)
    db = client['mydatabase']
    collection = db['mycollection']
    return collection


def read_data_from_file(file_path):
    # Чтение данных из файла task_1_item.text и преобразование в список словарей
    data = []
    with open(file_path, 'r', encoding='utf-8') as file:
        content = file.read()
        records = content.split('=====\n')  # Разделение записей
        for record in records:
            if record.strip():  # Пропуск пустых строк
                record_dict = {}
                for line in record.strip().split('\n'):
                    if '::' in line:
                        key, value = line.split('::', 1)
                        record_dict[key.strip()] = value.strip()
                data.append(record_dict)
    return data


def write_data_to_json(data, file_path):
    with open(file_path, 'w', encoding='utf-8') as file:
        json.dump(data, file, ensure_ascii=False, indent=4, default=str)


def task_1(collection):
    # Вывод первых 10 записей, отсортированных по убыванию по полю salary
    data = list(collection.find().sort('salary', pymongo.DESCENDING).limit(10))
    write_data_to_json(data, 'task_1_output.json')


def task_2(collection):
    # Вывод первых 15 записей, отфильтрованных по предикату age < 30, отсортированных по убыванию по полю salary
    data = list(collection.find({'age': {'$lt': 30}}).sort('salary', pymongo.DESCENDING).limit(15))
    write_data_to_json(data, 'task_2_output.json')


def task_3(collection):
    # Вывод первых 10 записей, отфильтрованных по сложному предикату
    data = list(collection.find(
        {'city': 'Тудела', 'job': {'$in': ['Учитель', 'Оператор call-центра', 'Менеджер']}}).sort('age',
                                                                                                    pymongo.ASCENDING).limit(
        10))
    write_data_to_json(data, 'task_3_output.json')


def task_4(collection):
    # Вывод количества записей, получаемых в результате фильтрации
    count = collection.count_documents({'$and': [{'age': {'$gt': 20}}, {'age': {'$lt': 40}},
                                                 {'year': {'$in': [2019, 2020]}}, {
                                                     '$or': [{'salary': {'$gt': 50000, '$lte': 75000}},
                                                             {'salary': {'$gt': 125000, '$lt': 150000}}]}]})
    write_data_to_json({'count': count}, 'task_4_output.json')


# Основной блок программы
file_path = 'task_1_item.text'

# Читаем данные из файла
data = read_data_from_file(file_path)

# Преобразуем числовые поля в правильный формат (если необходимо)
for record in data:
    if 'salary' in record:
        record['salary'] = int(record['salary'])
    if 'age' in record:
        record['age'] = int(record['age'])
    if 'year' in record:
        record['year'] = int(record['year'])

# Подключаемся к MongoDB и загружаем данные
collection = connect_to_mongodb()
collection.delete_many({})  # Очистка коллекции перед загрузкой данных
collection.insert_many(data)

# Выполняем задания
task_1(collection)
task_2(collection)
task_3(collection)
task_4(collection)
