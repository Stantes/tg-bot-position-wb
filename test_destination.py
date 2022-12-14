import csv
import json
import urllib.parse
import requests
from bs4 import BeautifulSoup


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

headers_0 = {
        'accept': '*/*',
        'accept-encoding': 'gzip, deflate, br',
        'accept-language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
        'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'cookie': urllib.parse.quote_plus(
            '__wbl=cityId=0&regionId=0&city=Хабаровск&phone=84957755505&latitude=48,476717&longitude=135,078796&src=1'),
        'referer': 'https://www.wildberries.ru/catalog/0/search.aspx?sort=popular&search=%D0%B0%D0%BF%D0%BF%D0%B0%D1%80%D0%B0%D1%82+%D0%B4%D0%BB%D1%8F+%D0%BC%D0%B8%D0%BA%D1%80%D0%BE%D1%82%D0%BE%D0%BA%D0%BE%D0%B2%D0%BE%D0%B9+%D1%82%D0%B5%D1%80%D0%B0%D0%BF%D0%B8%D0%B8',
}
s = requests.Session()
s.post('https://www.wildberries.ru/lk/poo/add?version=3', headers=headers_0, data={'version': '3', 'tem.AddressId': '16973'})

post_url = 'https://www.wildberries.ru/geo/saveprefereduserloc'
data = {'address': 'Хабаровск', 'longitude': '135.061341927', 'latitude': '48.481386816'}
r = s.post(post_url, headers=headers, json=data)

print(s.cookies)
