# -*- coding: utf-8 -*-
import pymongo
import redis
from pymongo.errors import DuplicateKeyError
from scrapy.exceptions import DropItem
from items import RelationshipItem, TweetItem, UserItem, CommentItem
from settings import MONGO_HOST, MONGO_PORT
from spiders.utils import get_urls_by_id

class RedisPipeline(object):
    def __init__(self):
        self.r = redis.Redis(host="redis")
        pass

    def process_item(self, item, spider):
        if spider.name == 'comment_spider':
            comment_user_url = get_urls_by_id('user', item['comment_user_id'])
            self.add_ids('user_spider', comment_user_url)

        elif spider.name == 'fan_spider':
            fan_user_url = get_urls_by_id('user', item['fan_id'])
            self.add_ids('user_spider', fan_user_url)

        elif spider.name == 'follower_spider':
            follower_user_url = get_urls_by_id('user', item['followed_id'])
            self.add_ids('user_spider', follower_user_url)

        elif spider.name == 'user_spider':
            fan_url = get_urls_by_id('fan', item['_id'])
            follow_url = get_urls_by_id('follow', item['_id'])
            tweet_url = get_urls_by_id('user_tweet', item['_id'])
            self.add_ids('fan_spider', fan_url)
            self.add_ids('follower_spider', follow_url)
            self.add_ids('tweet_spider', tweet_url)

        elif spider.name == 'tweet_spider':
            comment_url = get_urls_by_id('comment', item['_id'])
            self.add_ids('comment_spider', comment_url)
            # todo: 使用 single_tweet_spider 根据 tweet id 进行单条微博的爬取
            #       有待测试，这里可能存在 BUG
            if len(item['origin_weibo']) > 0:
                origin_weibo_url = get_urls_by_id('single_tweet', item['origin_weibo'])
                self.add_ids('single_tweet_spider', origin_weibo_url)

        return item

    def add_ids(self, spider_name, item_id):
        self.r.lpush(f'{spider_name}:start_urls', item_id)


class MongoDBPipeline(object):
    def __init__(self):
        client = pymongo.MongoClient(MONGO_HOST, MONGO_PORT)
        db = client['weibo']
        self.Users = db["Users"] #创建集合Users
        self.Tweets = db["Tweets"]
        self.Comments = db["Comments"]
        self.Relationships = db["Relationships"]

    def process_item(self, item, spider):
        if spider.name == 'comment_spider':
            self.insert_item(self.Comments, item)
        elif spider.name == 'fan_spider':
            self.insert_item(self.Relationships, item)
        elif spider.name == 'follower_spider':
            self.insert_item(self.Relationships, item)
        elif spider.name == 'user_spider':
            self.insert_item(self.Users, item)
        elif spider.name == 'tweet_spider':
            self.insert_item(self.Tweets, item)
        elif spider.name == 'single_tweet_spider':
            # fixme: 有待测试
            self.insert_item(self.Tweets, item)
        else:
            return DropItem('Missing Item!')
        return item

    @staticmethod
    def insert_item(collection, item):
        try:
            collection.insert(dict(item))
        except DuplicateKeyError:
            pass
