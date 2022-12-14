import csv
import json
import os
import re
import urllib.parse
from datetime import datetime
import openpyxl
import pandas as pd
import requests
from aiogram import Bot, types
from aiogram.dispatcher import Dispatcher, FSMContext
from aiogram.dispatcher.filters import Text
from aiogram.utils import executor
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher.filters.state import StatesGroup, State
from mpire import WorkerPool
from bs4 import BeautifulSoup

# from functions import parse_query, parse

TOKEN = '5153471664:AAFRBe205Pa0BQ1PsrcNnllrRVOlyoBySOc'
# TOKEN = '1365881511:AAGLqiyYRgnEcip2tPaBmauQENMCkRJ6jcA' # Мой токен
# 13738269 носки

bot = Bot(token=TOKEN)
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)


# dp = Dispatcher(bot)
class UserState(StatesGroup):
    document = State()
    site = State()
    city = State()


USER_IDS = {}

headers = {
    'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 '
                  '(KHTML, like Gecko) Chrome/99.0.4844.51 Safari/537.36'
}


def read_xl_file(filename, colums):
    wb = openpyxl.load_workbook(filename)
    ws = wb.active
    data = []
    for row in ws.rows:
        if colums == 1:
            try:
                if row[0].value is None or row[1].value is None:
                    continue
                data.append([int(row[0].value), row[1].value])
            except:
                continue
        if colums == 2:
            try:
                if row[0].value is None:
                    continue
                data.append(int(row[0].value))
            except:
                continue
    return data


def write_csv_file(filename, data, mode='a'):
    with open(filename, mode) as f:
        writer = csv.writer(f)
        writer.writerow(data)


def parse_position_wb(shared_objects, *data):
    filename = shared_objects['document'] + '.csv'
    id = data[0]
    q = data[1]
    text = urllib.parse.quote_plus(q)
    headers = {
        'accept': '*/*',
        'accept-encoding': 'gzip, deflate, br',
        'accept-language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
        'dnt': '1',
        'origin': 'https://www.wildberries.ru',
        'referer': 'https://www.wildberries.ru/catalog/0/search.aspx?search={query}&xsearch=true',
        'sec-ch-ua': '"Chromium";v="92", " Not A;Brand";v="99", "Google Chrome";v="92"',
        'sec-ch-ua-mobile': '?0',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-site',
        # 'cookie': cookie,
        'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36',
    }
    # Перезаписываем referer для правдоподобности
    headers['referer'] = headers['referer'].replace('{query}', text)
    session = requests.Session()

    if shared_objects['city'] == 'краснодар':
        target = f'https://search.wb.ru/exactmatch/ru/common/v4/search?appType=1&couponsGeo=2,6,7,3,19,21,8&curr=rub&dest=-1059500,-108082,-269701,12358063&emp=0&lang=ru&locale=ru&pricemarginCoeff=1.0&query={text}&reg=0&regions=68,64,83,4,38,80,33,70,82,86,30,69,48,22,1,66,31,40&resultset=catalog&sort=popular&spp=0&stores=117673,122258,122259,130744,117501,507,3158,124731,121709,120762,117986,159402,2737'
        cookie = urllib.parse.quote_plus(
            '__wbl=cityId=0&regionId=0&city=Краснодар&phone=84957755505&latitude=45,050437&longitude=38,959727&src=1')
        headers['cookie'] = cookie
    elif shared_objects['city'] == 'санкт-петербург':
        target = f'https://search.wb.ru/exactmatch/ru/common/v4/search?appType=1&couponsGeo=12,6,7,5,3,18,21&curr=rub&dest=-1216601,-337422,-1114252,-1124719&emp=0&lang=ru&locale=ru&pricemarginCoeff=1.0&query={text}&reg=0&regions=68,64,83,4,38,80,33,70,82,86,30,69,48,22,1,66,31,40&resultset=catalog&sort=popular&spp=0&stores=125238,125239,125240,117673,122258,122259,117734,159402,2737,161812,117544,132043,121709,124731,117501,507,3158,120762,117986,130744'
        cookie = urllib.parse.quote_plus(
            '__wbl=cityId=0&regionId=0&city=Санкт-Петербург&phone=84957755505&latitude=59,934568&longitude=30,298117&src=1')
        headers['cookie'] = cookie
    elif shared_objects['city'] == 'казань':
        target = f'https://search.wb.ru/exactmatch/ru/common/v4/search?appType=1&couponsGeo=12,6,7,3,18,22,21&curr=rub&dest=-1075831,-79374,-367666,-2133462&emp=0&lang=ru&locale=ru&pricemarginCoeff=1.0&query={text}&reg=0&regions=68,64,83,4,38,80,33,70,82,86,30,69,48,22,1,66,31,40&resultset=catalog&sort=popular&spp=0&stores=117673,122258,122259,117986,1733,117501,507,3158,120762,159402,2737,130744,686,1193,121709,124731'
        cookie = urllib.parse.quote_plus(
            '__wbl=cityId=0&regionId=0&city=Казань&phone=84957755505&latitude=55,789604073&longitude=49,124949102&src=1')
    elif shared_objects['city'] == 'екатеринбург':
        target = f'https://search.wb.ru/exactmatch/ru/common/v4/search?appType=1&couponsGeo=2,12,6,7,13,3,21&curr=rub&dest=-1113276,-79379,-1104258,-5803327&emp=0&lang=ru&locale=ru&pricemarginCoeff=1.0&query={text}&reg=0&regions=64,58,83,4,38,80,33,70,82,86,30,69,48,22,1,66,31,40&resultset=catalog&sort=popular&spp=0&stores=117673,122258,122259,1733,686,117986,117501,507,3158,120762,130744,159402,2737,121709,124731,1193'
        cookie = urllib.parse.quote_plus(
            '__wbl=cityId=0&regionId=0&city=Екатеринбург&phone=84957755505&latitude=56,843829&longitude=60,625187&src=1')
        headers['cookie'] = cookie
    elif shared_objects['city'] == 'новосибирск':
        target = f'https://search.wb.ru/exactmatch/ru/common/v4/search?appType=1&couponsGeo=2,12,6,7,3,21,16&curr=rub&dest=-1221148,-140294,-1751445,-364763&emp=0&lang=ru&locale=ru&pricemarginCoeff=1.0&query={text}&reg=0&regions=64,58,83,4,38,80,33,70,82,86,30,69,48,22,1,66,31,40&resultset=catalog&sort=popular&spp=0&stores=686,1733,1193,117501,507,3158,120762,117986,159402,2737,130744'
        cookie = urllib.parse.quote_plus(
            '__wbl=cityId=0&regionId=0&city=Новосибирск&phone=84957755505&latitude=55,034727&longitude=82,917024&src=1')
        headers['cookie'] = cookie
    elif shared_objects['city'] == 'хабаровск':
        target = f'https://search.wb.ru/exactmatch/ru/common/v4/search?appType=1&couponsGeo=2,12,6,7,9,21,11&curr=rub&dest=-1221185,-151223,-1782064,-1785058&emp=0&lang=ru&locale=ru&pricemarginCoeff=1.1&query={text}&reg=0&regions=64,4,38,80,70,82,86,30,69,48,22,1,66,40&resultset=catalog&sort=popular&spp=0&stores=1193,686,1733,117986,117501,507,3158,120762'
        cookie = urllib.parse.quote_plus(
            '__wbl=cityId=0&regionId=0&city=Хабаровск&phone=84957755505&latitude=48,476717&longitude=135,078796&src=1')
        headers['cookie'] = cookie
    else:
        target = f'https://search.wb.ru/exactmatch/ru/common/v4/search?appType=1&couponsGeo=12,3,18,15,21&curr=rub&dest=-1029256,-102269,-1278703,-1255563&emp=0&lang=ru&locale=ru&pricemarginCoeff=1.0&query={text}&reg=0&regions=68,64,83,4,38,80,33,70,82,86,75,30,69,48,22,1,66,31,40,71&resultset=catalog&sort=popular&spp=0&stores=117673,122258,122259,125238,125239,125240,6159,507,3158,117501,120602,120762,6158,121709,124731,159402,2737,130744,117986,1733,686,132043'
        cookie = urllib.parse.quote_plus(
            '__wbl=cityId=77&regionId=0&city=Москва&phone=84957755505&latitude=55,776218&longitude=37,629171&src=1')
        headers['cookie'] = cookie

    try:
        data = json.loads(session.get(target, headers=headers).content)
    except Exception as e:
        print(f'Ошибка получения страницы выдачи: ', e, f'артикул={id}, запрос={q}')
        write_csv_file(filename, [id, q, 0, 0, 0])
        return None

    page_cnt = 1  # Счетчик страниц
    breaker = False  # Прерывает цикл
    while True:
        position_cnt = 1  # Счетчик позиций
        if 'data' in data:
            for i in data['data']['products']:
                if str(id) == str(i['id']):
                    try:
                        price = int(i['salePriceU'] / 100)
                    except:
                        price = int(i['priceU'] / 100)
                    write_csv_file(filename, [id, q, page_cnt, position_cnt, price])
                    return
                else:
                    position_cnt += 1
                    continue
        else:
            write_csv_file(filename, [id, q, 0, 0, 0])
        # if breaker:
        #     break
        if page_cnt == 5:
            # print(f'ID: {id} на первых 5 страницах не найден...')
            write_csv_file(filename, [id, q, 0, 0, 0])
            return
        page_cnt += 1
        url = target + f'&page={page_cnt}'
        try:
            data = json.loads(session.get(url, headers=headers).content)
        except Exception:
            # print(e, data)
            # return [id, q, 0, 0]
            write_csv_file(filename, [id, q, 0, 0, 0])
            return


def parse_price_wb(shared_objects, *data):
    filename = shared_objects['document'] + '.csv'
    article = data[0]

    url = f'https://card.wb.ru/cards/detail?spp=27&regions=68,64,83,4,38,80,33,70,82,86,75,30,69,1,48,22,66,31,40,71' \
          f'&pricemarginCoeff=1.0&reg=1&appType=1&emp=0&locale=ru&lang=ru&curr=rub&couponsGeo=12,3,18,15,21' \
          f'&dest=-1029256,-102269,-2162196,-1257786&nm={article}'

    data = requests.get(url).json()

    try:
        price = int(data['data']['products'][0]['extended']['basicPriceU']) / 100
    except:
        price = int(data['data']['products'][0]['priceU']) / 100

    if len(data['data']['products'][0]['sizes'][0]['stocks']) > 0:
        stock = 'Да'
    else:
        stock = 'Нет'

    write_csv_file(filename, [article, int(price), stock])


@dp.message_handler(commands=['start'])
async def send_welcome(msg: types.Message, state: FSMContext):
    await state.finish()
    await msg.answer(f'Привет! Я Бот. Приятно познакомиться, {msg.from_user.first_name}\n'
                     f'Отправь мне .excel файл артикулов и запросов\n'
                     f'Например:', reply_markup=types.ReplyKeyboardRemove())
    with open('docs/p.png', 'rb') as photo:
        # await msg.reply_photo(photo, caption='Cats are here 😺')
        await bot.send_photo(msg.chat.id, photo)
    await msg.answer(msg.from_user.id)


@dp.message_handler(content_types=['document'], state='*')
async def handle_docs(msg: types.document, state: FSMContext):
    chat_id = msg.chat.id
    file_info = await bot.get_file(msg.document.file_id)
    downloaded_file = await bot.download_file(file_info.file_path)

    user_id = msg.from_user.id
    USER_IDS[user_id] = file_info.file_unique_id
    src = 'docs/' + USER_IDS[user_id] + '.xlsx'
    # temp_file = 'docs/' + USER_IDS[user_id] + '.csv'

    with open(src, 'wb') as new_file:
        new_file.write(downloaded_file.read())

    await state.update_data(document=src)
    # user_data = await state.get_data()

    keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True)
    buttons = ["Позиции WB", "Цены WB"]
    keyboard.add(*buttons)
    await msg.answer("Укажите направление парсинга", reply_markup=keyboard)

    await UserState.site.set()


@dp.message_handler(Text(equals="Позиции WB"), state=UserState.site)
async def parse_wb(message: types.Message, state: FSMContext):
    await state.update_data(site='Wildberries')

    keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True)
    buttons = ["Санкт-Петербург", "Москва", "Краснодар", "Казань", "Екатеринбург", "Новосибирск", "Хабаровск"]
    keyboard.add(*buttons)
    await message.answer("Укажите город для парсинга", reply_markup=keyboard)

    await UserState.city.set()


@dp.message_handler(
    Text(equals=["Санкт-Петербург", "Москва", "Краснодар", "Казань", "Екатеринбург", "Новосибирск", "Хабаровск"]),
    state=UserState.city)
async def parse_wb_positions(message: types.Message, state: FSMContext):
    chat_id = message.chat.id
    user_id = message.from_user.id

    await state.update_data(city=message.text.lower())
    user_data = await state.get_data()

    src = user_data['document']
    temp_file = user_data['document'] + '.csv'
    await message.answer('Пожалуйста, ожидайте...', reply_markup=types.ReplyKeyboardRemove())
    write_csv_file(temp_file, [datetime.now().date(), ' ', ' ', ' ', ' '], 'w')
    write_csv_file(temp_file, ['Артикул', 'Запрос', 'Страница', 'Позиция', 'Цена'], 'w')
    try:
        data = read_xl_file(src, colums=1)
        # print(data)
    except Exception as e:
        os.remove(src)
        os.remove(temp_file)
        await message.answer('Не верный формат файла...')
        return

    jobs = 20 if len(data) > 20 else len(data)
    with WorkerPool(n_jobs=jobs, shared_objects=user_data) as p:
        p.map(parse_position_wb, data)

    temp_data = pd.read_csv(temp_file, header=None)
    temp_data.to_excel(src, header=None, index=False)

    await message.answer('Вот ваш файл...')
    await bot.send_document(chat_id, open(src, 'rb'))

    os.remove(src)
    os.remove(temp_file)
    await state.finish()


@dp.message_handler(Text(equals="Цены WB"), state=UserState.site)
async def parse_wb_price(message: types.Message, state: FSMContext):
    await state.update_data(site='Wildberries')
    chat_id = message.chat.id
    user_id = message.from_user.id

    # await state.update_data(city=message.text.lower())
    user_data = await state.get_data()

    src = user_data['document']
    temp_file = user_data['document'] + '.csv'
    await message.answer('Пожалуйста, ожидайте...', reply_markup=types.ReplyKeyboardRemove())
    write_csv_file(temp_file, [datetime.now().date(), ' '], 'w')
    write_csv_file(temp_file, ['Артикул', 'Цена', 'Наличие'], 'w')
    try:
        data = read_xl_file(src, colums=2)
    except Exception as e:
        os.remove(src)
        os.remove(temp_file)
        await message.answer('Не верный формат файла...')
        return

    jobs = 20 if len(data) > 20 else len(data)
    with WorkerPool(n_jobs=jobs, shared_objects=user_data) as p:
        p.map(parse_price_wb, data)

    temp_data = pd.read_csv(temp_file, header=None)
    temp_data.to_excel(src, header=None, index=False)

    await message.answer('Вот ваш файл...')
    await bot.send_document(chat_id, open(src, 'rb'))

    os.remove(src)
    os.remove(temp_file)
    await state.finish()


if __name__ == '__main__':
    executor.start_polling(dp, skip_updates=True)
