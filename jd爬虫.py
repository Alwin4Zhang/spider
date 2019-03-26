# -*-coding: utf-8 -*-
"""
    @author:alwin
    @file:jd爬虫.py
    @time:2019-03-24 12:01:06
    @github:alwin114@hotmail.com
"""

import requests
import scrapy
from redis import StrictRedis, ConnectionPool
from gevent.queue import LifoQueue
from gevent.pool import Pool
from urllib.parse import quote, unquote
import gevent.monkey
from pymongo import MongoClient
import time
import traceback
import json
import math
import argparse
from pprint import pprint

redis_queue = StrictRedis(connection_pool=ConnectionPool(host='127.0.0.1', port=6379, db=0, decode_responses=True))
task_queue = LifoQueue()

SEARCH_KEY = 'search_key'
SEARCH_HTML = 'search_html'
LIST_LINK = 'list_link'
LIST_HTML = 'list_html'
FIRST_COMMENT_LINK = 'first_comment_link'
FIRST_COMMENT_HTML = 'first_comment_html'
COMMENT_LINK = 'comment_link'
COMMENT_HTML = 'comment_html'
# MAX_PAGE = 'max_page'
IP_KEY = 'ip'


class JDCrawler(object):
    def __init__(self):
        self.base_url = 'https://search.jd.com/Search?keyword={0}&enc=utf-8'
        self.list_base_url = 'https://search.jd.com/Search?keyword={0}&enc=utf-8&page={1}'
        self.comment_base_url = 'https://sclub.jd.com/comment/productPageComments.action?callback=fetchJSON_comment98vv2489&productId={0}&score=0&sortType=5&page={1}&pageSize=10'
        self.proxy = {
            'http': "202.108.22.5",
            'https': "202.108.22.5"
        }
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.28 Safari/537.36'
        }
        self.collection = MongoClient('localhost', 27017)['spider']['jd']

    def get_ip(self):  # 从ip代理池获取
        ip = redis_queue.lpop(IP_KEY)
        if ip:
            proxy = {
                'http': ip,
                'https': ip,
            }
            return proxy
        else:
            time.sleep(30)
            return self.get_ip()

    def get_html(self, word=None, d=200):
        if not word.startswith('http'):
            url = self.base_url.format(word)
        else:
            url = word
        n = 0
        h = ''
        while not h and n < d:
            try:
                h = requests.get(url, headers=self.headers, proxies=self.proxy, timeout=2)
                if 'gbk' in h.text or 'gb2312' in h.text:
                    h.encoding = 'gbk'
                elif 'utf-8' in h.text:
                    h.encoding = 'utf-8'
                elif 'latin-1' in h.text:
                    h.encoding = 'latin-1'
                h = h.text
            except Exception as e:
                print(e)
                self.proxy = self.get_ip()
        return word, h

    def parse_query_html(self, word, h):  # 获取关键词检索页面的每个商品词条
        selector = scrapy.Selector(text=h)
        total_page_num = selector.xpath("//div[@id='J_topPage']/span[@class='fp-text']/i/text()").extract()
        total_page_num = int(total_page_num[0]) if total_page_num else 10
        total_page_urls = [self.list_base_url.format(word, str(2 * i + 1)) for i in range(total_page_num)]
        for total_page_url in total_page_urls:
            list_str = '丨'.join([word, total_page_url])
            redis_queue.rpush(LIST_LINK, list_str)

    def parse_list_html(self, word, h):  # 解析翻页页面数据
        selector = scrapy.Selector(text=h)
        product_ids = selector.xpath("//ul[@class='gl-warp clearfix']/li/@data-sku").extract()
        # product_urls = ['https://item.jd.com/{0}.html'.format(product_id) for product_id in product_ids]
        comment_urls = [self.comment_base_url.format(product_id, 1) for product_id in product_ids]
        print('urls......', comment_urls)
        #         for product_id,product_url in zip(product_ids,product_urls):
        #             connect_str = '丨'.join([word,product_id,product_url])
        #             redis_queue.rpush(DETAIL_LINK,connect_str)
        for product_id, comment_url in zip(product_ids, comment_urls):
            comment_str = '丨'.join([word, product_id, comment_url])
            redis_queue.rpush(FIRST_COMMENT_LINK, comment_str)

    def parse_comments_parameters(self, word, product_id, h):  # 从第一个评论页面获取pages参数
        try:
            comment_json = json.loads(h.replace('fetchJSON_comment98vv2489(', '')[:-2])
            max_page = int(comment_json.get('maxPage', 1))
            for i in range(max_page):
                temp_url = self.comment_base_url.format(product_id, str(i))
                connect_str = '丨'.join([word, product_id, temp_url])
                redis_queue.rpush(COMMENT_LINK, connect_str)
        except Exception as e:
            print(e)
            return

    def parse_comment_detail_html(self, word, h):  # 获取每个评论页面的内容
        # comments = requests.get(url,headers=headers).text
        try:
            comment_json = json.loads(h.replace('fetchJSON_comment98vv2489(', '')[:-2])
            comments = comment_json.get('comments', [])  # 评论
            productCommentSummary = comment_json.get('productCommentSummary', {})  # 评论头部简要评论统计 全部评价、追评、好评、差评、中评等
            hotCommentTagStatistics = {
                'hotCommentTagStatistics': comment_json.get('hotCommentTagStatistics', [])}  # 评论选择的标签统计 简单方便、操作方便、打印清晰等
            for comment in comments:
                comment.update({'search_keyword': word})
                comment.update(productCommentSummary)
                comment.update(hotCommentTagStatistics)
                self.collection.insert_one(comment)
        except Exception as e:
            print(e)
            return


def push_key():
    test_keys = ['小米', '苹果', '锤子', '坚果', '华为', '三星', '360手机', 'oppo', 'vivo', 'iqoo', 'reno']
    for key in test_keys:
        redis_queue.rpush(SEARCH_KEY, key)

        # 获取ip代理


def request_free_proxy(page_num):
    url = 'http://www.66ip.cn/{0}.html'.format(str(page_num))
    r = requests.get(url)
    r.encoding = 'gbk'
    selector = scrapy.Selector(text=r.text)
    ips = selector.xpath("//table[@width='100%']/tr/td[1]/text()").extract()
    ports = selector.xpath("//table[@width='100%']/tr/td[2]/text()").extract()
    proxies = [ip + ':' + port for ip, port in zip(ips[1:], ports[1:])]
    return proxies


def request_free_proxy2(page_num):
    url = 'https://www.xicidaili.com/nt/{0}'.format(str(page_num))
    html = requests.get(url, headers=bc.headers).text
    selector = scrapy.Selector(text=html)
    ips = selector.xpath("//table[@id='ip_list']/tr/td[2]/text()").extract()
    ports = selector.xpath("//table[@id='ip_list']/tr/td[3]/text()").extract()
    proxies = [ip + ':' + port for ip, port in zip(ips[1:], ports[1:])]
    return proxies


def get_proxy():
    # proxy_base_url = 'http://www.66ip.cn/{0}.html'
    while True:
        if redis_queue.llen(IP_KEY) > 500:
            time.sleep(3)
            if not redis_queue.llen(SEARCH_KEY) and not redis_queue.llen(SEARCH_HTML) and not redis_queue.llen(
                    FIRST_COMMENT_LINK) \
                    and not redis_queue.llen(FIRST_COMMENT_HTML) and not redis_queue.llen(
                FIRST_COMMENT_HTML) and not redis_queue.llen(COMMENT_LINK) \
                    and not redis_queue.llen(COMMENT_HTML):
                print('ip stop!!!')
                break
            else:
                continue
        try:
            i = 1
            while i < 1561:
                if i == 1560:  # 从头开始获取
                    i = 0
                proxies = request_free_proxy(i)
                print(i, proxies)
                for proxy in proxies:
                    redis_queue.rpush(IP_KEY, proxy)
                proxies2 = request_free_proxy2(i)
                print(i, proxies2)
                for proxy in proxies2:
                    redis_queue.rpush(IP_KEY, proxy)
                time.sleep(1)
                if not proxies:
                    continue
                i += 1
        except Exception as e:
            print(e)
            time.sleep(300)


def run_search():  # 搜索页面
    while True:
        redis_id = redis_queue.lpop(SEARCH_KEY)
        if redis_id:
            try:
                word, text = bc.get_html(redis_id)
                connect_str = '丨'.join([word, text])
                redis_queue.rpush(SEARCH_HTML, connect_str)
            except Exception as e:
                traceback.print_exc()
        else:
            print('search link queue is empty!!!')
            break


def parse_search():  # 关键词搜索页面
    while True:
        redis_id = redis_queue.lpop(SEARCH_HTML)  # 拿到的是word,html
        if redis_id:
            try:
                split_redis_id = redis_id.split('丨')
                bc.parse_query_html(split_redis_id[0], split_redis_id[1])
            except Exception as e:
                traceback.print_exc()
        else:
            if not redis_queue.llen(SEARCH_KEY):
                gevent.sleep(3)
                if not redis_queue.llen(SEARCH_HTML):
                    print('search html queue is empty!!!')
                    break
                else:
                    gevent.sleep(1)


def run_list():  # 搜索页面翻页
    while True:
        redis_id = redis_queue.lpop(LIST_LINK)  # 拿到的是word,url
        if redis_id:
            try:
                split_redis_id = redis_id.split('丨')
                word, h = bc.get_html(split_redis_id[1])
                connect_str = '丨'.join([split_redis_id[0], h])
                redis_queue.rpush(LIST_HTML, connect_str)
            except Exception as e:
                traceback.print_exc()
        else:
            if not redis_queue.llen(SEARCH_KEY) and not redis_queue.llen(SEARCH_HTML):
                gevent.sleep(8)
                if not redis_queue.llen(LIST_LINK):
                    print('list queue is empty!!!')
                    break
                else:
                    gevent.sleep(1)


def parse_list():  # 解析翻页结果
    while True:
        redis_id = redis_queue.lpop(LIST_HTML)  # 拿到的是word,h
        if redis_id:
            try:
                split_redis_id = redis_id.split('丨')
                bc.parse_list_html(split_redis_id[0], split_redis_id[-1])
            except Exception as e:
                traceback.print_exc()
        else:
            if not redis_queue.llen(SEARCH_KEY) and not redis_queue.llen(SEARCH_HTML) and not redis_queue.llen(
                    LIST_LINK):
                gevent.sleep(10)
                if not redis_queue.llen(LIST_HTML):
                    print('list html queue is empty!!!')
                    break
                else:
                    gevent.sleep(1)


def run_comment_parameter():  # 获取第一个评论页面
    while True:
        redis_id = redis_queue.lpop(FIRST_COMMENT_LINK)  # word,product_id,comment_url
        if redis_id:
            try:
                split_redis_id = redis_id.split('丨')
                word, first_comment_html = bc.get_html(split_redis_id[-1])
                connect_str = '丨'.join([split_redis_id[0], split_redis_id[1], first_comment_html])
                redis_queue.rpush(FIRST_COMMENT_HTML, connect_str)
            except Exception as e:
                traceback.print_exc()
        else:
            if not redis_queue.llen(SEARCH_KEY) and not redis_queue.llen(SEARCH_HTML) and not redis_queue.llen(
                    LIST_LINK) and not redis_queue.llen(LIST_HTML):
                gevent.sleep(20)
                if not redis_queue.llen(FIRST_COMMENT_LINK):
                    print('first comment link queue is empty!!!')
                    break
                else:
                    gevent.sleep(1)


def parse_comment_parameter():  # 解析评论第一个页面获取参数
    while True:
        redis_id = redis_queue.lpop(FIRST_COMMENT_HTML)  # word,product_id,first_comment_html
        if redis_id:
            try:
                split_redis_id = redis_id.split('丨')
                bc.parse_comments_parameters(split_redis_id[0], split_redis_id[1], split_redis_id[-1])
            except Exception as e:
                traceback.print_exc()
        else:
            if not redis_queue.llen(SEARCH_KEY) and not redis_queue.llen(SEARCH_HTML) and not redis_queue.llen(
                    LIST_LINK) and not redis_queue.llen(LIST_HTML) and not redis_queue.llen(FIRST_COMMENT_LINK):
                gevent.sleep(30)
                if not redis_queue.llen(FIRST_COMMENT_HTML):
                    print('first comment html queue is empty!!!')
                    break
                else:
                    gevent.sleep(1)


def run_comment():  # 获取每个评论页面html
    while True:
        redis_id = redis_queue.lpop(COMMENT_LINK)  # word,product_id,temp_url
        if redis_id:
            try:
                split_redis_id = redis_id.split('丨')
                word, h = bc.get_html(split_redis_id[-1])
                connect_str = '丨'.join([split_redis_id[0], h])
                redis_queue.rpush(COMMENT_HTML, connect_str)
            except Exception as e:
                traceback.print_exc()
        else:
            if not redis_queue.llen(SEARCH_KEY) and not redis_queue.llen(SEARCH_HTML) and not redis_queue.llen(
                    LIST_LINK) and not redis_queue.llen(LIST_HTML) and not redis_queue.llen(
                FIRST_COMMENT_LINK) and not redis_queue.llen(FIRST_COMMENT_HTML):
                gevent.sleep(40)
                if not redis_queue.llen(COMMENT_LINK):
                    print('comment link queue is empty!!!')
                    break
                else:
                    gevent.sleep(1)


def parse_comment():  # 解析每个评论的页面
    while True:
        redis_id = redis_queue.lpop(COMMENT_HTML)  # word,h
        if redis_id:
            try:
                split_redis_id = redis_id.split('丨')
                bc.parse_comment_detail_html(split_redis_id[0], split_redis_id[-1])
            except Exception as e:
                traceback.print_exc()
        else:
            if not redis_queue.llen(SEARCH_KEY) and not redis_queue.llen(SEARCH_HTML) and not redis_queue.llen(
                    LIST_LINK) and not redis_queue.llen(LIST_HTML) and not redis_queue.llen(
                FIRST_COMMENT_LINK) and not redis_queue.llen(FIRST_COMMENT_HTML) \
                    and not redis_queue.llen(COMMENT_HTML):
                gevent.sleep(50)
                if not redis_queue.llen(COMMENT_HTML):
                    print('comment html queue is emtpy!!!')
                    break
                else:
                    gevent.sleep(1)


def gevent_from_html(rs=20, ps=10, rl=10, pl=10, rcp=10, pcp=10, rc=10, pc=10):
    gevent.monkey.patch_all()
    push_key()
    p = Pool(rs + ps + rl + pl + rcp + pcp + rc + pc + 1)
    p.apply_async(get_proxy)
    for i in range(rs):
        p.apply_async(run_search)
    for i in range(ps):
        p.apply_async(parse_search)
    for i in range(rl):
        p.apply_async(run_list)
    for i in range(pl):
        p.apply_async(parse_list)
    for i in range(rcp):
        p.apply_async(run_comment_parameter)
    for i in range(pcp):
        p.apply_async(parse_comment_parameter)
    for i in range(rc):
        p.apply_async(run_comment)
    for i in range(pc):
        p.apply_async(parse_comment)
    p.join()


bc = JDCrawler()

if __name__ == '__main__':
    gevent_from_html(rs=20, ps=5, rl=30, pl=2, rcp=50, pcp=8, rc=50, pc=8)
