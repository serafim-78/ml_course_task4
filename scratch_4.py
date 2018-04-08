# -*- coding: utf-8 -*-
import pandas as pd
import requests as rq
from time import sleep
from random import choice
from random import uniform
from lxml import html
from fake_useragent import UserAgent
import os

'загрузка данных'
def load_data(page, useragent, proxy):
    url = 'https://rateyourmusic.com/customchart?page=%d&chart_type=top&type=album&year=alltime&genre_include=1&include_child_genres=1&genres=&include_child_genres_chk=1&include=both&origin_countries=&limit=none&countries=' % (page)
    request = rq.get(url, headers=useragent, proxies=proxy)
    return request.text

'получение списка прокси'
def get_proxy():
    r=rq.get('https://www.ip-adress.com/proxy-list')
    str=html.fromstring(r.content)
    ip=str.xpath("//tbody/tr/td[1]/a/text()")
    port=str.xpath("//tbody/tr/td[1]/text()")
    result=[]
    for i in range(len(ip)):
        result.append(ip[i]+port[i])
    return result

'случайный выбор прокси из списка'
def choice_proxy(proxy_list):
    while(True):
        proxy=choice(proxy_list)
        url='http://'+proxy
        try:
            r=rq.get('http://ya.ru', proxies={'http': url})
            if r.status_code==200:
                return url
        except:
            continue


'проверка данных (отсутствющие страницы, неполные данные)'
need_pages=[]
for i in range(1,26):
    try:
        page=open('page_%d.html'%(i), 'r')
        if os.path.getsize('page_%d.html'%(i))<=10000:
            need_pages.append(i)
        page.close()
    except:
        need_pages.append(i)
        continue

'если данные не все, то грузим'
if len(need_pages)!=0:
    print 'Неполный список данных. Начата загрузка'
    proxy_list = get_proxy()
    for i in need_pages:
        if i%5==1:
            proxy_list=get_proxy()
        proxy={'http': choice_proxy(proxy_list)}
        useragent={'User-Agent': UserAgent().chrome}
        data=load_data(i, useragent, proxy)
        with open('./page_%d.html' %(i), 'w') as output_file:
          output_file.write(data.decode('utf8'))
        print "Загружена страница ",i,'; Proxy=',proxy
        if os.path.getsize('page_%d.html' % (i)) <= 10000:
            print 'Получил бан. Повторите минут через 10'
            break
        sleep(uniform(40, 80))

'загрузка данные в csv-файл: альбом, артист, рейтинг и кол-во голосовавших'
data=[]
for j in range(1,26):
    r=open('page_%d.html' % (j), 'r')
    str=html.fromstring(r.read())
    album = str.xpath('//a[@class="album"]/text()')
    artist = str.xpath('//a[@class="artist"]/text()')
    rate = str.xpath('//div[@class="chart_stats"]/a/b[1]/text()')
    ratings = str.xpath('//div[@class="chart_stats"]/a/b[2]/text()')
    for i in range(0, 40):
        album[i]=album[i].encode('utf8')
        artist[i]=artist[i].encode('utf8')
        ratings[i]=int(ratings[i].replace(",", ""))
        data.append([album[i], artist[i], rate[i], ratings[i]])
    r.close()
data = pd.DataFrame(data)
data.to_csv('data.csv', index=False, header=['album', 'artist', 'rate', 'ratings'])  # экспортируем в файл
data=pd.read_csv('data.csv', sep=',')

'Запросы'
'исполнитель, наиболее часто упоминающийся в топе'
query=(data.groupby(['artist'])['artist'].count()).sort_values(ascending=False)
print '#1. Больше всего в топе:',query.keys()[0],'. Количество альбомов:',query[0]
'10 исполнителей с наибольшим количеством суммарно проголосоваших'
print '#2. 10 исполнителей с наибольшим количеством суммарно проголосоваших:'
query=(data.groupby(['artist'])['ratings'].sum()).sort_values(ascending=False)
print query[:10]
'10 исполнителей с самой высокой средней оценкой'
print '#3. 10 исполнителей с самой высокой средней оценкой:'
query=(data.groupby(['artist'])['rate'].mean()).sort_values(ascending=False)
print query[:10]