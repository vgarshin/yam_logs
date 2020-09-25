#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import json
import requests
import pandas as pd
import time
import calendar
from datetime import datetime, timedelta

with open('creds.txt', 'r') as file:
    OAUTH_TOKEN, COUNTER_ID = file.read().replace('\n', '').split()

MAIN_URL = 'https://api-metrika.yandex.net/management/v1/counter/{}/logrequests'.format(COUNTER_ID)
MAIN_URL_R = 'https://api-metrika.yandex.net/management/v1/counter/{}/logrequest'.format(COUNTER_ID)
HEADERS = {
    'Authorization': 'OAuth {}'.format(OAUTH_TOKEN),
    'Content-Type': 'application/x-yametrika+json',
    'Content-Length': '123'
}
SLEEP = 5
DEEP_M = 1
SOURCES = ['hits', 'visits']

def send_request_get_status(data, headers, time_sleep=3):
    request_metadata = None
    request = requests.post(
        MAIN_URL, 
        params=data, 
        headers=headers
    )
    try:
        log_request = request.json()['log_request']
        request_id = log_request['request_id']
        print('request id:', request_id)
        requests_info = requests.get(
            MAIN_URL, 
            headers=headers
        )
        request_status = [
            x for x in requests_info.json()['requests'] 
            if x['request_id'] == request_id
        ][0]['status']
        while request_status != 'processed':
            requests_info = requests.get(
                MAIN_URL, 
                headers=headers
            )
            request_status = [
                x for x in requests_info.json()['requests'] 
                if x['request_id'] == request_id
            ][0]['status']
            print(time.ctime(), '| request id:', request_id, '| status:', request_status)
            time.sleep(time_sleep)
        request_metadata = [
            x for x in requests_info.json()['requests'] 
            if x['request_id'] == request_id
        ][0]
    except:
        print('error getting data:', request.text)
    return request_metadata

def get_data_logs(req_id, parts, headers):
    print(f'request id {req_id} loading...')
    text_data = ''
    for part in parts:
        load_url = '{}/{}/part/{}/download'.format(MAIN_URL_R, req_id, part)
        r = requests.get(
            load_url, 
            headers=headers
        )
        print('part no: ', part, '| status code:', r.status_code)
        text_data += r.text
    print(f'request id {req_id} loaded')
    return text_data

def main():
    
    hits_fields = [
        'ym:pv:watchID', # Идентификатор просмотра
        'ym:pv:counterID', # Номер счетчика
        'ym:pv:date', # Дата события
        'ym:pv:dateTime', # Дата и время события
        'ym:pv:title', # Заголовок страницы
        'ym:pv:URL', # Адрес страницы
        'ym:pv:referer', # Реферер
        'ym:pv:UTMCampaign', # UTM Campaign
        'ym:pv:UTMContent', # UTM Content
        'ym:pv:UTMMedium', # UTM Medium
        'ym:pv:UTMSource', # UTM Source
        'ym:pv:UTMTerm', # UTM Term
        'ym:pv:browser', # Браузер
        'ym:pv:browserMajorVersion', # Major-версия браузера
        'ym:pv:browserMinorVersion', # Minor-версия браузера
        'ym:pv:browserCountry', # Страна браузера
        'ym:pv:browserEngine', # Движок браузера
        'ym:pv:browserEngineVersion1', # Major-версия движка браузера
        'ym:pv:browserEngineVersion2', # Minor-версия движка браузера
        'ym:pv:browserEngineVersion3', # Build-версия движка браузера
        'ym:pv:browserEngineVersion4', # Revision-версия движка браузера
        'ym:pv:browserLanguage', # Язык браузера
        'ym:pv:clientTimeZone', # Часовой пояс на компьютере посетителя
        'ym:pv:cookieEnabled', # Наличие Cookie
        'ym:pv:deviceCategory', # Тип устройства. Возможные значения: 1 — десктоп, 2 — мобильные телефоны, 3 — планшеты, 4 — TV
        'ym:pv:from', # Метка from
        'ym:pv:hasGCLID', # Наличие GCLID
        'ym:pv:GCLID', # GCLID
        'ym:pv:ipAddress', # IP адрес
        'ym:pv:javascriptEnabled', # Наличие JavaScript
        'ym:pv:mobilePhone', # Производитель устройства
        'ym:pv:mobilePhoneModel', # Модель устройства
        'ym:pv:openstatAd', # Openstat Ad
        'ym:pv:openstatCampaign', # Openstat Campaign
        'ym:pv:openstatService', # Openstat Service
        'ym:pv:openstatSource', # Openstat Source
        'ym:pv:operatingSystem', # Операционная система (детально)
        'ym:pv:operatingSystemRoot', # Группа операционных систем
        'ym:pv:physicalScreenHeight', # Физическая высота
        'ym:pv:physicalScreenWidth', # Физическая ширина
        'ym:pv:regionCity', # Город (английское название)
        'ym:pv:regionCountry', # Страна (ISO)
        'ym:pv:regionCityID', # ID города
        'ym:pv:regionCountryID', # ID страны
        'ym:pv:screenColors', # Глубина цвета
        'ym:pv:screenFormat', # Соотношение сторон
        'ym:pv:screenHeight', # Логическая высота
        'ym:pv:screenOrientation', # Ориентация экрана
        'ym:pv:screenWidth', # Логическая ширина
        'ym:pv:windowClientHeight', # Высота окна
        'ym:pv:windowClientWidth', # Ширина окна
        'ym:pv:lastTrafficSource', # Источник трафика
        'ym:pv:lastSearchEngine', # Поисковая система (детально)
        'ym:pv:lastSearchEngineRoot', # Поисковая система
        'ym:pv:lastAdvEngine', # Рекламная система
        'ym:pv:artificial', # Искусственный хит, переданный с помощью функций hit(), event() и пр.
        'ym:pv:pageCharset', # Кодировка страницы сайта
        'ym:pv:isPageView', # Просмотр страницы. Принимает значение 0, если хит не нужно учитывать как просмотр
        'ym:pv:link', # Переход по ссылке
        'ym:pv:download', # Загрузка файла
        'ym:pv:notBounce', # Специальное событие «неотказ» (для точного показателя отказов)
        'ym:pv:lastSocialNetwork', # Социальная сеть
        'ym:pv:httpError', # Код ошибки
        'ym:pv:clientID', # Идентификатор пользователя на сайте
        'ym:pv:networkType', # Тип соединения
        'ym:pv:lastSocialNetworkProfile', # Страница социальной сети, с которой был переход
        'ym:pv:goalsID', # Идентификаторы достигнутых целей
        'ym:pv:shareService', # Кнопка «Поделиться», имя сервиса
        'ym:pv:shareURL', # Кнопка «Поделиться», URL
        'ym:pv:shareTitle', # Кнопка «Поделиться», заголовок страницы
        'ym:pv:iFrame', # Просмотр из iframe
        'ym:pv:parsedParamsKey1', # Параметры просмотра, ур. 1
        'ym:pv:parsedParamsKey2', # Параметры просмотра, ур. 2
        'ym:pv:parsedParamsKey3', # Параметры просмотра, ур. 3
        'ym:pv:parsedParamsKey4', # Параметры просмотра, ур. 4
        'ym:pv:parsedParamsKey5', # Параметры просмотра, ур. 5
        'ym:pv:parsedParamsKey6', # Параметры просмотра, ур. 6
        'ym:pv:parsedParamsKey7', # Параметры просмотра, ур. 7
        'ym:pv:parsedParamsKey8', # Параметры просмотра, ур. 8
        'ym:pv:parsedParamsKey9', # Параметры просмотра, ур. 9
        'ym:pv:parsedParamsKey10', # Параметры просмотра, ур. 10
    ]
    visits_fields = [
        'ym:s:visitID', # Идентификатор визита
        'ym:s:counterID', # Номер счетчика
        'ym:s:watchIDs', # Просмотры, которые были в данном визите. Ограничение массива — 500 просмотров
        'ym:s:date', # Дата визита
        'ym:s:dateTime', # Дата и время визита
        'ym:s:dateTimeUTC', # Unix timestamp времени первого хита
        'ym:s:isNewUser', # Первый визит посетителя
        'ym:s:startURL', # Страница входа
        'ym:s:endURL', # Страница выхода
        'ym:s:pageViews', # Глубина просмотра (детально)
        'ym:s:visitDuration', # Время на сайте (детально)
        'ym:s:bounce', # Отказность
        'ym:s:ipAddress', # IP адрес
        'ym:s:regionCountry', # Страна (ISO)
        'ym:s:regionCity', # Город (английское название)
        'ym:s:regionCountryID', # ID страны
        'ym:s:regionCityID', # ID города
        'ym:s:clientID', # Идентификатор пользователя на сайте
        'ym:s:networkType', # Тип соединения
        'ym:s:goalsID', # Идентификатор целей, достигнутых за данный визит
        'ym:s:goalsSerialNumber', # Порядковые номера достижений цели с конкретным идентификатором
        'ym:s:goalsDateTime', # Время достижения каждой цели
        'ym:s:goalsPrice', # Ценность цели
        'ym:s:goalsOrder', # Идентификатор заказов
        'ym:s:goalsCurrency', # Идентификатор валюты
        'ym:s:lastTrafficSource', # Источник трафика
        'ym:s:lastAdvEngine', # Рекламная система
        'ym:s:lastReferalSource', # Переход с сайтов
        'ym:s:lastSearchEngineRoot', # Поисковая система
        'ym:s:lastSearchEngine', # Поисковая система (детально)
        'ym:s:lastSocialNetwork', # Cоциальная сеть
        'ym:s:lastSocialNetworkProfile', # Группа социальной сети
        'ym:s:referer', # Реферер
        'ym:s:lastDirectClickOrder', # Кампания Яндекс.Директа
        'ym:s:lastDirectBannerGroup', # Группа объявлений
        'ym:s:lastDirectClickBanner', # Объявление Яндекс.Директа
        'ym:s:lastDirectClickOrderName', # Название кампании Яндекс.Директа
        'ym:s:lastClickBannerGroupName', # Название группы объявлений
        'ym:s:lastDirectClickBannerName', # Название объявления Яндекс.Директа
        'ym:s:lastDirectPhraseOrCond', # Условие показа объявления
        'ym:s:lastDirectPlatformType', # Тип площадки
        'ym:s:lastDirectPlatform', # Площадка
        'ym:s:lastDirectConditionType', # Тип условия показа объявления
        'ym:s:lastCurrencyID', # Валюта
        'ym:s:from', # Метка from
        'ym:s:UTMCampaign', # UTM Campaign
        'ym:s:UTMContent', # UTM Content
        'ym:s:UTMMedium', # UTM Medium
        'ym:s:UTMSource', # UTM Source
        'ym:s:UTMTerm', # UTM Term
        'ym:s:openstatAd', # Openstat Ad
        'ym:s:openstatCampaign', # Openstat Campaign
        'ym:s:openstatService', # Openstat Service
        'ym:s:openstatSource', # Openstat Source
        'ym:s:hasGCLID', # Наличие метки GCLID
        'ym:s:lastGCLID', # GCLID последнего визита
        'ym:s:firstGCLID', # GCLID первого визита
        'ym:s:lastSignificantGCLID', # GCLID последнего значимого визита
        'ym:s:browserLanguage', # Язык браузера
        'ym:s:browserCountry', # Страна браузера
        'ym:s:clientTimeZone', # Часовой пояс на компьютере посетителя
        'ym:s:deviceCategory', # Тип устройства. Возможные значения: 1 — десктоп, 2 — мобильные телефоны, 3 — планшеты, 4 — TV
        'ym:s:mobilePhone', # Производитель устройства
        'ym:s:mobilePhoneModel', # Модель устройства
        'ym:s:operatingSystemRoot', # Группа операционных систем
        'ym:s:operatingSystem', # Операционная система (детально)
        'ym:s:browser', # Браузер
        'ym:s:browserMajorVersion', # Major-версия браузера
        'ym:s:browserMinorVersion', # Minor-версия браузера
        'ym:s:browserEngine', # Движок браузера
        'ym:s:browserEngineVersion1', # Major-версия движка браузера
        'ym:s:browserEngineVersion2', # Minor-версия движка браузера
        'ym:s:browserEngineVersion3', # Build-версия движка браузера
        'ym:s:browserEngineVersion4', # Revision-версия движка браузера
        'ym:s:cookieEnabled', # Наличие Cookie
        'ym:s:javascriptEnabled', # Наличие JavaScript
        'ym:s:screenFormat', # Соотношение сторон
        'ym:s:screenColors', # Глубина цвета
        'ym:s:screenOrientation', # Ориентация экрана
        'ym:s:screenWidth', # Логическая ширина
        'ym:s:screenHeight', # Логическая высота
        'ym:s:physicalScreenWidth', # Физическая ширина
        'ym:s:physicalScreenHeight', # Физическая высота
        'ym:s:windowClientWidth', # Ширина окна
        'ym:s:windowClientHeight', # Высота окна
        'ym:s:purchaseID', # Идентификатор покупки
        'ym:s:purchaseDateTime', # Дата и время покупки
        'ym:s:purchaseAffiliation', # Магазин или филиал, в котором произошла транзакция
        'ym:s:purchaseRevenue', # Полученный доход
        'ym:s:purchaseTax', # Сумма всех налогов, связанных с транзакцией
        'ym:s:purchaseShipping', # Стоимость доставки, связанная с транзакцией
        'ym:s:purchaseCoupon', # Промокод, ассоциированный со всей покупкой целиком
        'ym:s:purchaseCurrency', # Валюта
        'ym:s:purchaseProductQuantity', # Количество товаров в покупке
        'ym:s:productsPurchaseID', # Идентификатор покупки
        'ym:s:productsID', # Идентификатор товара
        'ym:s:productsName', # Название товара
        'ym:s:productsBrand', # Производитель товара
        'ym:s:productsCategory', # Категория, к которой относится товар
        'ym:s:productsCategory1', # Категория, к которой относится товар, уровень 1
        'ym:s:productsCategory2', # Категория, к которой относится товар, уровень 2
        'ym:s:productsCategory3', # Категория, к которой относится товар, уровень 3
        'ym:s:productsCategory4', # Категория, к которой относится товар, уровень 4
        'ym:s:productsCategory5', # Категория, к которой относится товар, уровень 5
        'ym:s:productsVariant', # Разновидность товара
        'ym:s:productsPosition', # Положение товара в списке
        'ym:s:productsPrice', # Цена товара
        'ym:s:productsCurrency', # Валюта товара
        'ym:s:productsCoupon', # Промокод ассоциированный с товаром
        'ym:s:productsQuantity', # Количество товара
        'ym:s:impressionsURL', # URL страницы с товаром
        'ym:s:impressionsDateTime', # Дата и время просмотра
        'ym:s:impressionsProductID', # Идентификатор просмотренного товара
        'ym:s:impressionsProductName', # Название просмотренного товара
        'ym:s:impressionsProductBrand', # Производитель просмотренного товара
        'ym:s:impressionsProductCategory', # Категория, к которой относится просмотренный товар
        'ym:s:impressionsProductCategory1', # Категория, к которой относится просмотренный товар, уровень 1
        'ym:s:impressionsProductCategory2', # Категория, к которой относится просмотренный товар, уровень 2
        'ym:s:impressionsProductCategory3', # Категория, к которой относится просмотренный товар, уровень 3
        'ym:s:impressionsProductCategory4', # Категория, к которой относится просмотренный товар, уровень 4
        'ym:s:impressionsProductCategory5', # Категория, к которой относится просмотренный товар, уровень 5
        'ym:s:impressionsProductVariant', # Разновидность просмотренного товара
        'ym:s:impressionsProductPrice', # Цена просмотренного товара
        'ym:s:impressionsProductCurrency', # Валюта для товара
        'ym:s:impressionsProductCoupon', # Промокод ассоциированный с просмотренным товаром
        'ym:s:offlineCallTalkDuration', # Длительность звонка в секундах
        'ym:s:offlineCallHoldDuration', # Длительность ожидания звонка в секундах
        'ym:s:offlineCallMissed', # Пропущен ли звонок
        'ym:s:offlineCallTag', # Произвольная метка
        'ym:s:offlineCallFirstTimeCaller', # Первичный ли звонок
        'ym:s:offlineCallURL', # URL, с которого был звонок (ассоциированная с событием страница)
        'ym:s:parsedParamsKey1', # Параметры визита, ур. 1
        'ym:s:parsedParamsKey2', # Параметры визита, ур. 2
        'ym:s:parsedParamsKey3', # Параметры визита, ур. 3
        'ym:s:parsedParamsKey4', # Параметры визита, ур. 4
        'ym:s:parsedParamsKey5', # Параметры визита, ур. 5
        'ym:s:parsedParamsKey6', # Параметры визита, ур. 6
        'ym:s:parsedParamsKey7', # Параметры визита, ур. 7
        'ym:s:parsedParamsKey8', # Параметры визита, ур. 8
        'ym:s:parsedParamsKey9', # Параметры визита, ур. 9
        'ym:s:parsedParamsKey10', # Параметры визита, ур. 10
    ]
    fields_dict = {
        SOURCES[0]: ','.join(hits_fields), 
        SOURCES[1]: ','.join(visits_fields[:131]) # do not include parsedParamsKey otherwise not working
    }
    
    #---get argv params---
    data_path = str(sys.argv[1])
    print('data path: ', data_path)
    date_start = sys.argv[2]
    print('date start: ', data_path)
    
    #---create cache path---
    cache_path = f'{data_path}/yam_logs'
    if not os.path.exists(cache_path):
        os.makedirs(cache_path)
        print('dir created:', cache_path)
    
    #---calculate dates to load logs---
    date2 = datetime.strptime(date_start, '%Y%m%d')
    month_days = calendar.monthrange(date2.year, date2.month - DEEP_M)[1]
    date1 = (date2 - timedelta(days=month_days - 1)).strftime('%Y-%m-%d')
    date2 = date2.strftime('%Y-%m-%d')
    print('get logs from', date1, 'to', date2)
    
    #---load logs for hits and visits---
    for source in SOURCES:
        print('-' * 10, 'source:', source, '-' * 10)
        data = {
            'date1': date1,
            'date2': date2,
            'fields': fields_dict[source],
            'source': source
        }
        request_metadata = send_request_get_status(data, headers=HEADERS, time_sleep=SLEEP)
        if request_metadata:
            request_id = request_metadata['request_id']
            parts = [x['part_number'] for x in request_metadata['parts']]
            print('request id:', request_id, '| total parts:', parts)
            text_data = get_data_logs(req_id=request_id, parts=parts, headers=HEADERS)
            print('length of text data:', len(text_data))
            print(f'size of text data: {(sys.getsizeof(text_data) / 2 ** 30):.2f}', 'GB')
            file_name = f'{cache_path}/yam_{source}_from_{date1}_to_{date2}.tsv'
            with open(file_name, 'w', encoding='utf-8') as file:
                file.write(text_data)
                print('saved:', file_name)
        else:
            print('error getting data:', source)

    print('all done')

if __name__ == '__main__':
    main()
