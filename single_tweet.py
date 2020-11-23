#!/usr/bin/env python
# encoding: utf-8

import re
from xml import etree

from scrapy import Selector
from scrapy.http import Request
from scrapy_redis.spiders import RedisSpider
import time
from items import TweetItem
from spiders.utils import extract_weibo_content, time_fix


class SingleTweetSpider(RedisSpider):
    """
    爬取单个微博页面，如 https://weibo.cn/comment/JltR1rvSK 中id为JltR1rvSK的微博数据的爬取
    """
    name = "single_tweet_spider"
    base_url = "https://weibo.cn"
    redis_key = "single_tweet_spider:start_urls"
    custom_settings = {
        "CLOSESPIDER_ERRORCOUNT": 10,
    }
    def parse(self, response):
        tweet_item = TweetItem()
        tweet_item['crawl_time'] = int(time.time())
        selector = Selector(response)

        # todo: 完成单个微博页面数据的爬取，可参考 tweet.py、fan.py 的实现
        #  页面布局参考 https://weibo.cn/comment/JltR1rvSK。
        '''
        tweet_item['crawl_time'] = ''
        tweet_repost_url = ''
        user_tweet_id = ''
        tweet_item['weibo_url'] = ''
        tweet_item['user_id'] = ''
        tweet_item['_id'] = ''
        tweet_item['created_at'] = ''
        tweet_item['tool'] = ''
        tweet_item['created_at'] = ''
        tweet_item['like_num'] = ''
        tweet_item['repost_num'] = ''
        tweet_item['comment_num'] = ''
        tweet_item['image_url'] = ''
        tweet_item['video_url'] = ''
        '''

        if response.url.endswith('page=1'):
            all_page = re.search(r'/>&nbsp;1/(\d+)页</div>', response.text)
            if all_page:
                all_page = all_page.group(1)
                all_page = int(all_page)
                for page_num in range(2, all_page + 1):
                    page_url = response.url.replace('page=1', 'page={}'.format(page_num))
                    yield Request(page_url, self.parse, dont_filter=True, meta=response.meta)

        tweet_item = TweetItem()
        tweet_item['crawl_time'] = int(time.time())
        tweet_repost_url = selector.xpath('.//a[contains(text(),"转发[")]/@href')[0]
        user_tweet_id = re.search(r'/repost/(.*?)\?uid=(\d+)', tweet_repost_url)
        tweet_item['weibo_url'] = 'https://weibo.com/{}/{}'.format(user_tweet_id.group(2),
                                                                           user_tweet_id.group(1))
        tweet_item['user_id'] = user_tweet_id.group(2)
        tweet_item['_id'] = user_tweet_id.group(1)
        create_time_info_node = selector.xpath('.//span[@class="ct"]')[-1]
        create_time_info = create_time_info_node.xpath('string(.)')
        if "来自" in create_time_info:
            tweet_item['created_at'] = time_fix(create_time_info.split('来自')[0].strip())
            tweet_item['tool'] = create_time_info.split('来自')[1].strip()
        else:
            tweet_item['created_at'] = time_fix(create_time_info.strip())

        like_num = selector.xpath('.//a[contains(text(),"赞[")]/text()')[-1]
        tweet_item['like_num'] = int(re.search('\d+', like_num).group())

        repost_num = selector.xpath('.//a[contains(text(),"转发[")]/text()')[-1]
        tweet_item['repost_num'] = int(re.search('\d+', repost_num).group())

        comment_num = selector.xpath(
            './/a[contains(text(),"评论[") and not(contains(text(),"原文"))]/text()')[-1]
        tweet_item['comment_num'] = int(re.search('\d+', comment_num).group())

        images = selector.xpath('.//img[@alt="图片"]/@src')
        if images:
            tweet_item['image_url'] = images

        videos = selector.xpath('.//a[contains(@href,"https://m.weibo.cn/s/video/show?object_id=")]/@href')
        if videos:
            tweet_item['video_url'] = videos

        map_node = selector.xpath('.//a[contains(text(),"显示地图")]')
        if map_node:
            map_node = map_node[0]
            map_node_url = map_node.xpath('./@href')[0]
            map_info = re.search(r'xy=(.*?)&', map_node_url).group(1)
            tweet_item['location_map_info'] = map_info

        repost_node = selector.xpath('.//a[contains(text(),"原文评论[")]/@href')
        if repost_node:
            tweet_item['origin_weibo'] = repost_node[0]

        all_content_link = selector.xpath('.//a[text()="全文" and contains(@href,"ckAll=1")]')
        if all_content_link:
            all_content_url = self.base_url + all_content_link[0].xpath('./@href')[0]
            yield Request(all_content_url, callback=self.parse_all_content, meta={'item': tweet_item},
                          priority=1)
        else:
            # 用于在其他函数中对内容进一步处理时，将已有数据放在 request.meta['item'] 中传递
            request_meta = response.meta
            request_meta['item'] = tweet_item
            yield tweet_item




