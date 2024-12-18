import msgpack
import json
import pymongo
from pymongo import MongoClient


def connect_to_mongodb():
    client = MongoClient('localhost', 27017)
    db = client['mydatabase']
    collection = db['mycollection']
    return collection


def read_data_from_msgpack(file_path):
    # Чтение данных из MessagePack файла
    with open(file_path, 'rb') as file:
        data = msgpack.unpackb(file.read(), raw=False)
    return data


def write_data_to_json(data, file_path):
    # Преобразование данных в сериализуемый JSON формат
    data_serializable = []
    for item in data:
        item_serializable = {**item}
        if '_id' in item_serializable:
            item_serializable['_id'] = str(item_serializable['_id'])  # Преобразование ObjectId в строку
        data_serializable.append(item_serializable)

    with open(file_path, 'w', encoding='utf-8') as file:
        json.dump(data_serializable, file, ensure_ascii=False, indent=4)


def task_1(collection):
    # Удаление записей с зарплатой вне указанного диапазона
    collection.delete_many({
        '$or': [
            {'salary': {'$lt': 25000}},
            {'salary': {'$gt': 175000}}
        ]
    })


def task_2(collection):
    # Обновление возраста: преобразование в число и увеличение на 1
    collection.update_many({}, [{'$set': {'age': {'$toInt': '$age'}}}])
    collection.update_many({}, {'$inc': {'age': 1}})


def task_3(collection):
    # Увеличение зарплаты для указанных профессий
    collection.update_many({}, [{'$set': {'salary': {'$toDouble': '$salary'}}}])
    collection.update_many({'job': {'$in': ['Программист', 'Аналитик']}}, {'$mul': {'salary': 1.05}})


def task_4(collection):
    # Увеличение зарплаты для указанных городов
    collection.update_many({'city': {'$in': ['Вроцлав', 'Сеговия']}}, {'$mul': {'salary': 1.07}})


def task_5(collection):
    # Увеличение зарплаты для сотрудников в Москве с указанными профессиями и возрастом
    collection.update_many(
        {
            'city': 'Москва',
            'job': {'$in': ['Программист', 'Аналитик', 'Дизайнер']},
            'age': {'$gte': 25, '$lte': 40}
        },
        {'$mul': {'salary': 1.10}}
    )


def task_6(collection):
    # Удаление записей с профессией "Менеджер"
    collection.delete_many({'job': 'Менеджер'})


# Основная часть программы
file_path = 'task_3_item.msgpack'

# Чтение данных из MessagePack файла
data = read_data_from_msgpack(file_path)

# Подключение к MongoDB
collection = connect_to_mongodb()

# Очистка коллекции перед загрузкой новых данных
collection.delete_many({})

# Загрузка данных в MongoDB
collection.insert_many(data)

# Выполнение заданий
task_1(collection)
task_2(collection)
task_3(collection)
task_4(collection)
task_5(collection)
task_6(collection)

# Получение всех данных из коллекции и сохранение в JSON
result = list(collection.find())
write_data_to_json(result, 'output.json')
