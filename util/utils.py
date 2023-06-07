#!/usr/bin/env python3
# encoding: utf-8


import redis
import datetime
import socket
import socket
import redis
# %% 爬取贝壳数据
# s
import requests
from cmreslogging.handlers import CMRESHandler
from loguru import logger
from lxml import etree
import json
import time
import re
import random
import pandas as pd
# import sendMsg
# import plot
import itertools
pool = redis.ConnectionPool(host='10.147.20.80', port=6380,db=1, decode_responses=True)
r = redis.Redis(connection_pool=pool)
from pymongo import MongoClient
# handler = CMRESHandler(hosts=[{'host': '10.147.20.80', 'port': 9200}],
#                            auth_type=CMRESHandler.AuthType.NO_AUTH,
#                            # host = 'imac',
#                            es_index_name="new")

# logger.add("1.log")
# 日志写入elasticsearch 由kibana分析
# logger.add(handler)
logger.debug('this is a debug message')

client = MongoClient(host='10.147.20.80', port=27017)  # 连接mongodb端口
db = client['all_house']
#collection = db['wuhan']



collection = db["city_district_urls"]
#
for wuhan in collection.find():
    print(wuhan)
    del wuhan['_id']
    r.lpush('all_beike',str(wuhan))

def run():
    #exit()
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.77 Safari/537.36', }


    msg_new = '当日新增房源：\n'
    msg_chg = '当日改价房源：\n'
    city = 'sh'
    while True:
        url_and_district = r.rpoplpush("all_beike", "all_beike")
        logger.info(url_and_district)
        url_and_district_dict = eval(
        url_and_district)  # eval('{"url":"https://bj.ke.com/ershoufang/","district_name":"北京","city":"北京"}') #
            # logger.info(url_and_district_dict)
            # pages = parse_info(url_and_district_dict['district_url'].replace("https://wh.ke.com/",""),
            #                    url_and_district_dict['district_details'],
            #                    url_and_district_dict['district_name'],
            #                    url_and_district_dict['city_name'])

        url = url_and_district_dict['url']

        district_details = url_and_district_dict['dis_name']
        district_name = url_and_district_dict['dis_name']
        city_name = url_and_district_dict['city_name']

            # print(district)
            # url_get_page_num = 'https://{0}.ke.com/ershoufang/{1}'.format(
            #     city, district)
        url_get_page_num = url
        resp = requests.get(url=url_get_page_num, headers=headers).text
        _element = etree.HTML(resp)
        try:
            page = json.loads(_element.xpath(
                '//div[@class="page-box house-lst-page-box"]/@page-data')[0])["totalPage"]
        except Exception as e:
            logger.error(e)
            continue
        print(page)
        for collate in ["co42"]: # ,"co12","co11","co32","21","co22","co41",""]:
            for i in range(page):
               
                url = '{0}pg{1}{2}/'.format(url_get_page_num, i+1, collate)
                print(url)
                try:
                    resp = requests.get(url=url, headers=headers).text
                    _element = etree.HTML(resp)
                    tags = _element.xpath('//li/div[@class="info clear"]')
                    for tag in tags:
                        temp = {}
                        # 房源卖点及小区
                        temp['district_details'] = district_details
                        temp['district_name'] = district_name
                        temp['city_name'] = city_name
                        temp['maidianDetail'] = tag.xpath('./div[1]/a/text()')[0]
                        temp['href'] = tag.xpath('./div[1]/a/@href')[0]
                        temp['dataAction'] = tag.xpath(
                             './div[1]/a/@data-action')[0]
                        _dataActions = temp['dataAction'].split("&")
                        for _aa in _dataActions:
                            if 'housedel_id' in _aa:
                                 temp['housedel_id'] = "'" + \
                                      str(_aa.split('=')[1])+"'"
                        try:
                            temp['goodhouse_tag'] = tag.xpath(
                                './div[1]/span/text()')[0]
                        except:
                            temp['goodhouse_tag'] = ''
                        temp['address'] = tag.xpath(
                            './div[2]/div[1]/div/a/text()')[0]
                        # 房源基本情况：楼层、年份、面积、房型、朝向
                        _houseInfo = tag.xpath(
                            './div/div[@class="houseInfo"]/text()')
                        _houseInfo = _houseInfo[1].replace(' ', '').split('\n')
                        for j in _houseInfo:
                            if '' in _houseInfo:
                                _houseInfo.remove('')
                        _floor = _houseInfo[0]
                        temp['floor'] = _floor
                        _totalFloor = int(
                            re.search("共(.*?)层", (_houseInfo[1])).group(1))
                        temp['totalFloor'] = _totalFloor
                        if len(_houseInfo) == 5:
                            year = int(re.search("\|(.*?)年建\|",
                                             (_houseInfo[2])).group(1))
                        else:
                            year = ''
                        temp['year'] = year
                        _area = float(
                             re.search("\|(.*?)平米", (_houseInfo[-2])).group(1))
                        temp['area'] = _area
                        _roomType = re.search("(.*?)\|", (_houseInfo[-2])).group(1)
                        temp['roomType'] = _roomType
                        _direction = _houseInfo[-1].replace('|', '')
                        temp['direction'] = _direction

                        # 房源关注情况
                        _followInfo = tag.xpath(
                            './div/div[@class="followInfo"]/text()')
                        for k in range(len(_followInfo)):
                            _followInfo[k] = _followInfo[k].replace(
                                '\n', '').replace('\r', '').replace(' ', '')

                        _followers = int(
                            re.search("(.*?)人关注", (_followInfo[1])).group(1))
                        temp['followers'] = _followers
                        try:
                            _publishDate = re.search(
                                "/(.*?)发布", (_followInfo[1])).group(1)
                            temp['publishDate'] = _publishDate
                        except:
                            temp['publishDate'] = '今日新上'
                        # 房源tag
                        subway = tag.xpath(
                            './div/div/span[@class="subway"]/text()')

                        # 房源价价格、单价
                        try:
                            _totalPrice = float(
                                tag.xpath('./div[2]/div[5]/div[1]/span/text()')[0].replace(' ', ''))
                        except:
                            if "暂无数据"==tag.xpath('./div[2]/div[5]/div[1]/span/text()')[0].replace(' ', ''):
                                _totalPrice = 0.
                        _meterPrice = tag.xpath(
                            './div[2]/div[5]/div[2]/span/text()')[0]
                        _meterPrice = float(
                            re.search("(.*?)元/平", _meterPrice).group(1).replace(',', ''))
                        temp['totalPrice'] = _totalPrice
                        temp['meterPrice'] = _meterPrice
                        temp['city'] = city_name
                        temp['region'] = url_and_district_dict #region
                        temp['subway'] = subway
                        temp['district'] =  district_name #district
                        temp['time'] = time.strftime("%Y/%m/%d", time.localtime())
                        temp['insert_time'] = datetime.datetime.now()
                        if r.zscore(f'{city_name}_houseid',temp['housedel_id']) is not None:
                        # 当日重复房源，直接跳过
                            logger.error("24h内重复房源")
                            continue

                        if not r.sismember(f'{city_name}_houseid_set',temp['housedel_id'] ):

                            # 历史新增房源，需记录
                            msg_new = '\n {district}/\t{address}:\n{area}/\t{roomType}/\t{totalPrice}/\t{meterPrice} \n {href} \n'.format_map(
                                    temp)
                            logger.error(msg_new)  # 测试用
                        elif r.zscore(name=f"{city_name}_house_price",value=temp['housedel_id']) != temp['totalPrice']:
                        #价格变动
                            last_time_price = r.zscore(name=f"{city_name}_house_price",value=temp['housedel_id'])
                            # last_time = r.zscore(name=f"{city_name}_house_price",value=temp['housedel_id'])
                            logger.error("价格变化")
                            logger.error(f"上一次价格{last_time_price},当前价格{temp['totalPrice']},上一次价格的时间是不确定")
                            msg_chg =  '\n {district}/\t{address}:\n{area}/\t{roomType}/\t{totalPrice_h}→{totalPrice}/\t{meterPrice} \n {href} \n'.format_map(
                                    temp)
                            logger.error(msg_chg)

                        temp['host'] = socket.gethostname()
                        logger.info(temp)
                        db[f"{city_name}"].insert_one(temp)
                        r.zadd(f'{city_name}_houseid',{temp['housedel_id']: int(time.time()) + 24 * 60 * 60 })
                        # result = result.append(temp, ignore_index=True)
                        r.zremrangebyscore(f'{city_name}_houseid', 0, int(time.time()))
                        r.sadd(f'{city_name}_houseid_set',temp['housedel_id'] )
                        r.zadd(f'{city_name}_house_price',{temp['housedel_id'] : temp['totalPrice']})
                except Exception as e:
                    print(e)
                # time.sleep(random.randint(1, 5))
    #    except Exception as e:
    #        logger.error(e)
    # result.drop_duplicates(['time','housedel_id'],keep='first')
    # result.to_excel(excel_writer='./output/spider_beike.xlsx',
    #                 sheet_name='beike')
    # # plot.plot_msg()
    # txt = msg_new+'\n'*2+'~'*20+'\n'*2+msg_chg
    # with open('./output/sendtxt.txt', 'w') as f:
    #     print(txt)
    #     f.write('\n------\n{0}\n------\n'.format(txt))
    # sendMsg.send(txt)
    # print(result, len(result))


if __name__ == '__main__':
    # 启动
    run()
