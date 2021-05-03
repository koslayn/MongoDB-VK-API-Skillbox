# -*- coding: utf-8 -*-

import requests
import json
import warnings
import os
import re
import pymorphy2
from pymongo import MongoClient
from pymongo import UpdateOne
from datetime import datetime

# Ключ доступа в отдельном файле
def __ask_credentials():
    """
    Функция для получения ключа доступа для работы с VK-API из файла '__credentials.txt'
    return: возвращает ключ доступа str
    """
    # Для задания папки, где лежит модуль рабочей
    os.chdir(os.path.dirname(__file__))
    try:
        with open('./__credentials.txt', 'r+') as C:
            VK_TOKEN = C.readline()
            if VK_TOKEN:
                return VK_TOKEN
            VK_TOKEN = input('Если знаете VK token, введите его здесь, или оставьте поле пустым для работы с загруженными данными. Если введёте ключ он будет сохранём в файл __credentials.txt')
            
            if len(VK_TOKEN) > 5:
                C.write(VK_TOKEN)
                return VK_TOKEN
            return None
    except:
        warnings.warn(("\nОтсутствует файл '__credentials.txt' c VK token."
                       "\nТокен можно указать в переменной «VK_TOKEN» для отладки и тестирования."
                       "\nТакже можно работать и без ключа, в этом случае будет происходить работа с предзагруженными данными"
                     ))
        VK_TOKEN = input('Если знаете VK token, введите его здесь, или оставьте поле пустым для работы с загруженными данными')
        if len(VK_TOKEN) > 5:
            return VK_TOKEN
        return None


# Функции для проверки параметров функций
def __param_cheker_owner_id(OWNER_ID=None):
    if OWNER_ID is None:
        warnings.warn('Не указано сообщество в VK в параметре <OWNER_ID>, данные не будут обновлены')
        return False
    else:
        return True

def __param_cheker_vk_token(VK_TOKEN=None):
    if VK_TOKEN is None and __ask_credentials() is None:
        warnings.warn('Отсутствует ключ, для обновления данных в параметре <VK_TOKEN> и в файле <__credentials.txt>, данные не будут обновлены')
        return False
    else:
        return True

def __param_cheker_post_id(post_id=None):
    if post_id is None:
        warnings.warn('Нет номера поста для запроса комментариев')
        return False
    else:
        return True

# Функции для работы с VK-API
def get_posts(count=1, offset=0, OWNER_ID=-66669811, VK_TOKEN=None):
    """
    Получаем посты из сообщества VK:
        * count: количество запрашиваемыз постов (не более 100 за раз),
        * offset:смещение относительно последнего доступного поста,
        * OWNER_ID - id сообщества,
        * VK_TOKEN - ключ от приложения в ВК
        * return: JSON
    """
    if __param_cheker_owner_id(OWNER_ID) and __param_cheker_vk_token(VK_TOKEN):
        #breakpoint()
        if VK_TOKEN is None:
            VK_TOKEN = __ask_credentials()

        url_post = f'https://api.vk.com/method/wall.get?access_token={VK_TOKEN}'
        response = requests.get(url_post, params={'owner_id':OWNER_ID,'count':count, 'offset':offset, 'v':5.92})
        response = response.json()

        return response


def get_comments(post_id=50679, OWNER_ID=-66669811, VK_TOKEN=None):
    """
    Получаем комментарии к посту по его ID в сообществе OWNER_ID
      * post_id: номер поста
      * return: JSON
    """
    if __param_cheker_owner_id(OWNER_ID) and __param_cheker_vk_token(VK_TOKEN) and __param_cheker_post_id(post_id):
        if VK_TOKEN is None:
            VK_TOKEN = __ask_credentials()
    
        url_post = f'https://api.vk.com/method/wall.getComments?access_token={str(VK_TOKEN)}'
        response = requests.get(url_post, params={'post_id':post_id, 'owner_id': OWNER_ID,'count':100, 'offset':0, 'extended':1, 'need_likes':1, 'v':5.92})
        response = response.json()

        return response


# Функции для разбора ответа от API-VK
def parse_json_vk_posts(data):
    """
    Преобразование JSON документа в JSON с меньшим количеством полей
      * data: JSON-пост из VK - response от VK-API
      * return: [{id: 1, date: 2, ...}, {}, ... {}]
    """
    root = data['response']['items']

    all_posts = []

    for p in root:
        post = {
            'id': p['id'],
            'from_id': p['from_id']*(-1),
            'date': p['date'],
            'text': p['text'],
            'views': p['views']['count'],
            'likes': p['likes']['count'],
            'reposts': p['reposts']['count'],
            'comments': p['comments']['count']
        }
        all_posts.append(post)
    return all_posts


def parse_json_vk_comments(data):
    """
    Разбирает комментарии на элементы: комментарий
      * data: response VK-API - комментарии и данные о профайле
      * return: comments = [JSON, ]
    """
    all_comments = []
    root_comments = data['response']['items']
    for comm in root_comments:
        if 'deleted' in comm:
            continue
        else:
            comment = {'id': comm.get('id'),
                'post_id': comm.get('post_id'),
                'from_id': comm.get('from_id'),
                'date': comm.get('date'),
                'text': comm.get('text'),
                'likes': comm.get('likes').get('count')}
            all_comments.append(comment)
    return all_comments

def parse_json_vk_profiles(data):
    """
    Не используемая функция - использовалась, для получения общих сведений о профайле из комментария
    Разбирает комментарии на элементы: профайл коментатора
      * data: response VK-API - комментарии и данные о профайле
      * return: profiles = [JSON, ]
    """
    warnings.warn('Функция использовалась для получения общей инофрмации о профайле вмемсте с получением комментария, не используется в коде')
    exc_prof = 0
    all_profiles = []

    root_profile = data['response']['profiles']
    for p in root_profile:

        profile = {
            'id': p['id'],
            'first_name': p.get('first_name'),
            'last_name': p.get('last_name'),
            'is_closed': p.get('is_closed'),
            'sex': p.get('sex'),
            'screen_name': p.get('screen_name')
        }
        all_profiles.append(profile)

    return all_profiles

def get_profiles(ids, VK_TOKEN=None):
    """
    Получаем расширенную информацию с профилей VK
      * ids: список id профилей, не более 100 за раз
      * return: сообщение об успешном выполнении
    """
    if __param_cheker_vk_token(VK_TOKEN):
        if VK_TOKEN is None:
            VK_TOKEN = __ask_credentials()

    # Превращаем лист в строку для запроса в ВК (до 100 профайлов)
    ids_str = ', '.join([str(i) for i in ids])
    url_post = f'https://api.vk.com/method/users.get?access_token={str(VK_TOKEN)}'
    response = requests.get(url_post, params={'user_ids':ids_str, 'fields': 'about, bdate, country, city, education, interests, occupation, relation, movies, personal, relation', 'v':5.92})
    response = response.json()

    profiles_to_collection = []
    for id in response['response']:
        # Списковая сборка словаря
        my_dict = {i: j for (i, j) in id.items()}
        profiles_to_collection.append(my_dict)
    
    return profiles_to_collection


# Функции для записи документов в MongoDB
def duplicate_cleaner(data, collection):
    """
    Функция определения наличия документа в коллекции по id и очистки постов для бобавления
      * input: list - со словарями
      * return: [{'id': 123, 'id': 111, ...}, {}, ...]
    """
    # Какие посты в базе данных
    
    mongo_cursor = collection.find()
    id_in_collection = [i['id'] for i in mongo_cursor]
    # Какие новые посты выгрузили - id
    id_for_collection = [data_id['id'] for data_id in data]
    # ID постов/Комментариев, которые ещё не в БД
    id_to_add = set(id_for_collection) - set(id_in_collection)
    # Формируем список новых постов
    cleaned_data = [data_id for data_id in data if data_id['id'] in id_to_add]

    if cleaned_data:
        print(f'Новых документов для добавления: {len(cleaned_data)}')
        return cleaned_data
    else:
        print('Новых документов для добавления нет')
        return False

def write_posts_to_collection(posts, collection):
    """
    Функция записи новых постов в коллекцию
      * input: посты [{'id': 123, 'id': 111, ...}, {}, ...], Коллекция для записи
      * return: True - для продолжении записи, False для прерываения цикла в т.ч. предупреждение
    """
    posts_cleaned = duplicate_cleaner(data=posts, collection=collection)

    if posts_cleaned is False:
        print("Новые посты для записи в коллекцию отсутствуют")
        posts_total = collection.find()
        print('Сейчас «Постов» в коллекции: {posts_total.count()}')
        return False

    if posts_cleaned:
        posts_ids = collection.insert_many(posts_cleaned)
        print(f'Записано постов: {len(posts_ids.inserted_ids)}')
        posts_total = collection.find()
        print(f'Сейчас «Постов» в коллекции: {posts_total.count()}')
        return True

def write_comments_to_collection(comments, collection):
    """
    Функция для записи комментариев в коллекцию
      * input:
      * return:
    """
    comments_cleaned = duplicate_cleaner(data=comments, collection=collection)

    if comments_cleaned is False:
        print("Новые комментарии для записи в коллекцию отсутствуют")
        comments_total = collection.find()
        print(f'Сейчас «Комментариев» в коллекции: {comments_total.count()}')
        return False

    if comments_cleaned:
        comments_ids = collection.insert_many(comments_cleaned)
        print(f'Записано комментариев: {len(comments_ids.inserted_ids)}')
        comments_total = collection.find()
        print(f'Сейчас «Комментариев» в коллекции: {comments_total.count()}')
        return True

def write_profiles_to_collection(profiles, collection):
    """
    Функция для записи профайлов в коллекцию, не пишет дубликаты при повторных запросах на добавление
      * input: список профайлов [{}, {}, ...]
      * return: None делает операции с БД
    """
    def cleaner(prof):
        if prof['id'] in in_collection:
            return None
        else:
            return prof

    ids_response = [i['id'] for i in profiles]
    print(f'Профайлов получено для обработки: {len(ids_response)}')

    profiles_in_coll = collection.find({}, {'_id': False, 'id': True})
    profiles_in_coll = [i['id'] for i in profiles_in_coll]

    in_collection = []
    for i in ids_response:
        if i in profiles_in_coll:
            in_collection.append(i)

    print(f'Количество дубликатов профайлов: {len(in_collection)}')

    profiles_cleaned = map(cleaner, profiles)
    profiles_cleaned = [x for x in profiles_cleaned if x is not None]
    
    if profiles_cleaned:
        profiles_ids = collection.insert_many(profiles)
        print(f'Записано профайлов: {len(profiles_ids.inserted_ids)}')
        profiles_total = collection.find()
        print(f'Сейчас «Профайлов» в коллекции: {profiles_total.count()}')
        return None
    else:
        print('Новых профайлов нет, ничего не записываем')
        return None

# Работа со временем в документах MongoDB
def unixtime_to_datetime(collection):
    """
    Преобразование unixtime в datetime
      * input: MongoDB collection with data fields
      * return: [(_id, datetatetime), (...)]
    """
    # Получаем все документы в которых нет поля со временем
    unix_time_field = [(d['_id'], d['date']) for d in collection.find({'datetime': None})]
    new_field = []
    for _id, utime in unix_time_field:
        new_field.append((_id, datetime.fromtimestamp(utime)))
    return new_field

def add_datetime_to_documents(collection):
    """
    Function add datetime object to mongoDB's documents
      * input: MongoDB collection
      * return: None
      * log: console - result of modification
    """
    operations = []
    
    for _id, date in unixtime_to_datetime(collection):
        operations.append(UpdateOne({'_id':_id}, {"$set": {'datetime': date}}))
    result = collection.bulk_write(operations)
    print(f'Внесены изменения в {len(operations)} документ из коллекции {collection.name} в базе данных {collection.database.name}')

# Преобразование текса в документах MongoDB
def split_post(text):
    """
    Split text to words and separate tags if they exists
      * input: text: str
      * return: (tags, words)
    """
    # Регулярка - выделение тегов
    re_tag = r'#\w{3,}\b'
    # Регулярка - разделение на слова (предлоги и др. отсекать будем при помощи pymorphy2).
    re_words = r'[а-яА-ЯёЁa-zA-Z]{1,}'
    # Выделяем таги из текста
    matched_tags = re.findall(re_tag, text)
    # Чистим текст от tag через замену
    if matched_tags:
        for tag in matched_tags:
            # f-string + raw-string
            text = re.sub(fr'{tag}', '', text)
    
    matched_words = re.findall(re_words, text)

    return matched_tags, matched_words

def get_and_prepare_post(collection):
    """
    Преобразование тексата поста и(или) комментария
      * input: коллекция документов, которую нужно обработать
      * return: list [('_id', 'tags', 'words'), ...] - поля для добавления к документу по его _id 
    """
    texts = [(d['_id'], d['text']) for d in collection.find({'$or': [{'words': None},{'tags': None}]})]
    new_fields = []

    for _id, text in texts:
        tags, words = split_post(text)

        new_fields.append((_id, tags, words))
    
    return new_fields

def add_tags_words_to_documents(collection):
    """
    Функция добавления полей в документы (слова и таги (если есть))
      * input: коллекция для обработки
      * log: пишет сообщения в консоль о результатах изменения
      * return: None
    """
    operation_tags = []
    operation_words = []

    data_for_coll = get_and_prepare_post(collection)

    if data_for_coll:
        for _id, tags, words in data_for_coll:
            operation_tags.append(UpdateOne({'_id':_id}, {"$set": {'tags': tags}}))
            operation_words.append(UpdateOne({'_id':_id}, {"$set": {'words': words}}))

        result_tags = collection.bulk_write(operation_tags)
        result_words = collection.bulk_write(operation_words)

    print('---------------TAG---------------')
    print(f'Внесены изменения в {len(operation_tags)} документ из коллекции {collection.name} в базе данных {collection.database.name}.')
    
    print('------------WORDS-----------------')
    print(f'Внесены изменения в {len(operation_words)} документ из коллекции {collection.name} в базе данных {collection.database.name}.')


# Преобразование слов в документах MongoDB (нормализация и выборка отдельных частей речи)
def text_norm(collection):
    """
    Функция преобразования слов: приводит слова к нормальной форме, разбивает слова на три группы: существительные, глаголы, прилогательные
      * input: коллекция для обработки
      * log: пишет в консоль "Всё успешно", статистику не считает и не выводит
      * return: None
    """
    operation = []

    morph = pymorphy2.MorphAnalyzer()

    _id_words = [(d['_id'], d['words']) for d in collection.find({'$or': [{'norm_NOUN': None}, {'norm_VERB': None}, {'norm_ADJF': None}]})] 

    for _id, words in _id_words:
        norm_NOUN = []
        norm_VERB = []
        norm_ADJF = []
        
        for word in words:
            p = morph.parse(word.lower())[0]

            if 'NOUN' in p.tag:
                
                normal_form = p.normal_form
                norm_NOUN.append(normal_form)
                continue

            if 'VERB' in p.tag:
                normal_form = p.normal_form
                norm_VERB.append(normal_form)

            elif 'INFN' in p.tag:
                normal_form = p.normal_form
                norm_VERB.append(normal_form)
                continue

            if 'ADJF' in p.tag:
                normal_form = p.normal_form
                norm_ADJF.append(normal_form)

            elif 'ADJS' in p.tag:
                normal_form = p.normal_form
                norm_ADJF.append(normal_form)
                continue
        
        operation.append(UpdateOne({'_id': _id}, {"$set": {'norm_NOUN': norm_NOUN, 'norm_VERB': norm_VERB, 'norm_ADJF': norm_ADJF}}))
    
    if operation:
        result = collection.bulk_write(operation)
        print(f'Обработано {len(_id_words)} документов')
    else:
        print('Все документы обработаны ранее, ничего не изменяем')
    return None

# Категорирование постов и комментариев
def category_adder(mongo_cursor):
    """
    Define category of the post
      * input: mongo_cursor
      * return (_id, {Програмирование, Кейс, ...})
    """
    category = categories_words_collection_dic.keys()
    category_tags = set()
    for tag in mongo_cursor['tags']:
        for name in category:
            if tag in categories_words_collection_dic[name]:
                category_tags.add(name)

    for word in mongo_cursor['norm_NOUN']:
        for name in category:
            if word in categories_words_collection_dic[name]:
                category_tags.add(name)              

    return mongo_cursor['_id'], list(category_tags)

def category_comment_adder(mongo_cursor):
    """
    TODO - дописать доки
    """
    type_of_comment = set()
    for word in mongo_cursor['norm_ADJF']:
        if word in pos_neg_comment_dic['Позитивный']:
            type_of_comment.add('Позитивный')
        
        if word in pos_neg_comment_dic['Негативный']:
            type_of_comment.add('Негативный')

    if type_of_comment:
        return mongo_cursor['_id'], list(type_of_comment)
    
    return False


# Словарь с категориями постов
categories_words_collection_dic = {
    'Анимация':['#CG_Skillbox', 'анимация',  ],
    'Дизайн':['#Design_Battle_Skillbox', '#Design_battle', '#Digitalлинейка', '#191970', '#7FFFD4', '#FF4500', '#MailDesignCup', '#Skillbox_Sreda', '#Skillbox_Дизайн', '#Skillbox_дизайн', '#Skillboxдизайн', '#Skillbохдизайн', '#design', '#designbattle', '#s2JsJK', '#sje5Lq', '#y0fJdx', '#Графдизайн_Skillbox', '#ДеньКомпьютернойГрафики', '#Дизайн_Skillbox', '#Дизайн_упаковки_Skillbox', '#Инфографика_Skillbox', '#Плакаты_Skillbox', '#Уроки_Фотошопа_Skillbox', '#Уроки_фотошопа', '#Уроки_фотошопа_Skillbox', '#Фотошопзаминуту', '#Шрифт_Skillbox', '#Шрифты_Skillbox', 'дизайн', 'дизайнер', 'шрифт', 'логотип', ],
    'Маркетинг':['#Skillbox_eTXT', '#Skillbox_Маркетинг', '#Skillbox_маркетинг', '#Skillbохмаркетинг', '#marketing', 'рассылка', 'маркетинг', 'реклама', 'маркетолог', 'текст', 'продвижение', 'контент', 'бренд', 'продажа', ],
    'Программирование':['#Skillbox_программирование', '#code', '#Защита_данных', '#Программирование_Skillbox', '#Разработчики_шутят', 'программирование', 'разработчик', 'разработка', 'программист', 'программа', 'код',  ],
    'Управление':['#Skillbox_управление', '#Skillbохуправление', '#management', '#ИграSkillbохпоуправлению', '#Игра_Skillbox', '#Игра_Skillbox_по_управлению', 'бизнес', 'управление', 'менеджер', ],
    'Трудоустройство':['#Вакансии', '#Вакансии_Skillbox', '#После_Skillbox',  ],
    'Материалы':['#Взакладки', '#Skillbox_в_закладки', '#Skillbox_взакладки', '#collections', '#read', '#skillbox_в_закладки', '#В_закладки_Skillbox', '#Взакладки_Skillbox', '#Полезное_Skillbox', '#Полезное_от_Skillbox', '#Полезное_от_Скиллбокс', '#Статья_Skillbox', '#статья_Skillbox' ],
    'Вопрос-Ответ-Совет':['#Skillbox_вопросы', '#Skillbox_отвечает', '#answer','#Совет_препода_Skillbox', '#Советы_Skillbox', ],
    'Партнёры':['#Skillbox_Pinkman', '#Skillbox_РБК', '#Skillbox_Усадьба_JAZZ', '#Skyeng', '#reebok', '#Skillbox_МИФ', '#OFFF_Moscow', '#Skillbox_AIC', '#Skillbox_DMC', '#Skillbox_OFFF', '#Skillbox_SceinceMe', '#Skillbox_ULCAMP', '#OFFF_конкурс', ],
    'Кейс':['#Skillbox_кейс', '#Задачка_Skillbox', '#Кейс_Skillbox', '#Разбор_в_Skillbox', '#Разбор_кейса_Skillbox', '#кейсSkillbox', '#Skillboc_кейс',],
    'Промо':['#Skillbox_акция', '#Распродажа_Skillbox', '#Черная_пятница_Skillbox', '#Skillbox_конкурс', '#Конкурс_Skillbox', '#Рабочая_ситуация_Skillbox', '#Рабочая_ситуация_Skillbox', '#РабочаяситуацияSkillbox', '#Skillbox_игра',],
    'Подкаст':['#Skillbox_подкаст', '#Skillboxподкаст', '#Подкасты_Skillbox', '#Кант_поймет', '#Кантпоймет', ],
    'Тренды':['#skillbox_тренды', '#Тренды',],
    'Вдохновение':['#Skillbox_вдохновляет', '#Skillboxвдохновляет', ],
    'Студенты':['#Лучшие_студенты_Skillbox', '#Защита_дипломов_Skillbox', '#Работа_cтудента_Skillbox', '#Работа_студента_Skillbox', '#Студенты_Skillbox',],
    'Образование':['#Skillbox_курс', '#Skillbox_об_учебе', '#Skillbox_образование', '#Анонс_Skillbox', '#Анонс_курса_Skillbox', '#Курс_Skillbox', '#Лекторий_Skillbox', '#Интенсив_Skillbox', '#Вебинар_Skillbox', '#Расписание_Skillbox',  '#Уроки_Skillbox', '#Skillbox_AFP', ],
    'Другое':['#Avocard', '#Skillbox_Mildberry', '#Skillbox_Симпсоны', '#Skillbox_Сострадамус', '#calendar', '#friday_typeface', '#history',  '#reviews', '#skillbox_sreda',  '#team', '#weekbook', '#wordoftheday', '#Айдентик_Skillbox', '#Айдентика_Skillbox',  '#Бешенаясушка', '#Вам_слово', '#Вам_слово_Skillbox',  '#ДеньБезИнтернета',  '#Моушен_Skillbox', '#Преподы_Skillbox', '#Проще_говоря_Skillbox', '#Стачка_Skillbox', '#Чат_Skillbox', '#Челлендж_Skillbox', '#вскилбоксе', '#гдеВАШдиплом', '#неткибербуллингу', '#нивкакиерамки'],
}

pos_neg_comment_dic = {
    'Позитивный': ['прекрасный', 'любимый', 'годный', 'творческий', 'приятный', 'перспективный', 'яркий', 'прикольный', 'креативный', 'красивый', 'полезный', 'отличный', 'интересный', 'хороший', 'крутой', 'полезный', 'приятный', 'удобный', 'популярный', 'реальный', 'современный', 'успешный', 'креативный', 'эффективный', 'актуальный', ''],
    'Негативный': ['неполноценный', 'бессмысленный', 'скучный', 'абсурдный', 'нелогичный', 'неактуальный', 'фиговый', 'убогий', 'бестолковый', 'устаревший', 'обидный', 'негативный', 'глупый', 'сомнительный', 'отвратительный', 'вредный', 'банальный', 'тупой', 'ужасный', 'опасный', 'бесполезный', 'сожаление', 'бред', 'жопа', 'фигня', 'дерьмо', 'хрен', 'отстой', 'говно', 'дно', 'хрень', 'бросить', 'отписаться', 'лезть', 'бесить', 'никакой',]
}


###############################################
### -------------Тестирвоание-------------- ###
###############################################
import unittest
# Модуль для юниттестирвоания
# Более простой вариант использовать конструкцию assert
# assert my_func([1, 2]) == ['one', 'two'], 'что-то не так'

# Наследуемся от базового класса
class ExtractorTest(unittest.TestCase):
# Тесты должны начинаться с test_ они будут тестирующие
# Методы, которые названы по другому, не будет отнесены к тестам

# setUp - метод, который вызывается перед каждым тестом - параметры
    def setUp(self):
        MC = {'host': 'localhost', 'port': 27017, 'user': None, 'password': None, 'authSource': 'vk'}
        self.client = MongoClient(MC['host'], MC['port'])
        self.db = self.client.vk
        self.coll_posts = self.db.posts
        self.coll_comments = self.db.comments
        self.posts_ids = self.coll_posts.insert_many([{'id': 4}, {'id': 5}])

# tearDown - метод очистка после выполнения теста, подчищаем за тестами
    def tearDown(self):
        self.prof_del_1 = self.coll_posts.delete_many({'id': {"$in": [4, 5]}})
        self.prof_del_2 = self.coll_posts.delete_many({'id': {"$in": [1, 2, 3]}})
        self.prof_del_2 = self.coll_comments.delete_many({'id': {"$in": [1, 2, 3]}})
        # print('----------ОТРАБОТАЛ ОЧИСТКА------------------')

# Тесты - Очистка постов от повторов
    # @unittest.skip
    def test_duplicate_cleaner_new_data(self):
        result = duplicate_cleaner(data=[{'id': 1}, {'id': 2}, {'id': 3}], collection=self.coll_posts)
        self.assertEqual(result, [{'id': 1}, {'id': 2}, {'id': 3}])

    # @unittest.skip
    def test_duplicate_cleaner_no_new_data(self):
        result = duplicate_cleaner(data=[{'id': 4}, {'id': 5}], collection=self.coll_posts)
        self.assertEqual(result, False)

    # @unittest.skip
    def test_duplicate_cleaner_empty_data(self):
        result = duplicate_cleaner(data=[], collection=self.coll_posts)
        self.assertEqual(result, False)

# Тесты запись постов в коллекцию - их очистка от повторов выше
    # @unittest.skip
    def test_write_posts_to_collection_new_data(self):
        result = write_posts_to_collection(posts=[{'id': 1}, {'id': 2}, {'id': 3}], collection=self.coll_posts)
        search = self.coll_posts.find({'id': {'$in': [1, 2, 3]}}, {'_id': False})
        search_list = [i for i in search]
        self.assertEqual(result, True)
        self.assertEqual(search_list, [{'id': 1}, {'id': 2}, {'id': 3}])

    # @unittest.skip
    def test_write_posts_to_collection_empty_data(self):
        result = write_posts_to_collection(posts=[], collection=self.coll_posts)
        self.assertEqual(result, False)

# Тест записи в коллекцию - комментариев
    def test_write_comments_to_collection_new_data(self):

        result = write_comments_to_collection(comments=[{'id': 1}, {'id': 2}, {'id': 3}], collection=self.coll_comments)
        search = self.coll_comments.find({'id': {'$in': [1, 2, 3]}}, {'_id': False})
        search_list = [i for i in search]
        self.assertEqual(result, True)
        self.assertEqual(search_list, [{'id': 1}, {'id': 2}, {'id': 3}])

### Если запускаем скрипт из консоли, то переменной присваивается значение и выполняется комманда
### Если импортируем библиотеку, то нет присвоения и не запускается комманда
if __name__ == '__main__':
    unittest.main()