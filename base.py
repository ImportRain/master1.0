from scrapy_redis.spiders import RedisSpider
from scrapy import signals, item
import pymongo
import time

from settings import MONGO_HOST, MONGO_PORT
import redis
from utils import get_urls_by_id


class BaseRedisSpider(RedisSpider):
    """
    用于对所有的基础 spider 进行一些统一配置或操作
    """
    def __init__(self):
        super(BaseRedisSpider, self).__init__()

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(BaseRedisSpider, cls).from_crawler(crawler, *args, **kwargs)
        # 连接 spider_close 信号，用于爬虫结束运行时对 redis 中待处理 url 进行持久化，实现断点续爬
        crawler.signals.connect(spider.spider_closed, signal=signals.spider_closed)
        return spider

    def spider_close(self, spider):
        """
        对爬虫提前结束运行时对 redis 中待爬取 url 进行保存
        """
        # 数据库连接
        client = pymongo.MongoClient(MONGO_HOST, MONGO_PORT)
        db = client['weibo']
        pending_url_collection = db['pending_urls']
        r = redis.Redis(host="redis")

        # todo: 通过 spider.name 将 redis 中对应的 url 队列持久化保存到 MongoDB pending_urls 集合中
        #       需要将所有 spider 继承自该类才会生效
        if spider.name == 'comment_spider':
            pending_url_collection.save({'_id' : get_urls_by_id('user', item['comment_user_id']),
                                          'persisted_time' : int(time.time()),
                                          'spider_name' : spider.name})

        elif spider.name == 'fan_spider':
            pending_url_collection.save({'_id': get_urls_by_id('user', item['fan_id']),
                                         'persisted_time': int(time.time()),
                                         'spider_name': spider.name})

        elif spider.name == 'follower_spider':
            pending_url_collection.save({'_id': get_urls_by_id('user', item['followed_id']),
                                         'persisted_time': int(time.time()),
                                         'spider_name': spider.name})

        elif spider.name == 'user_spider':
            pending_url_collection.save({'_id': get_urls_by_id('fan', item['_id']),
                                         'persisted_time': int(time.time()),
                                         'spider_name': spider.name})
            pending_url_collection.save({'_id': get_urls_by_id('follow', item['_id']),
                                         'persisted_time': int(time.time()),
                                         'spider_name': spider.name})
            pending_url_collection.save({'_id': get_urls_by_id('user_tweet', item['_id']),
                                         'persisted_time': int(time.time()),
                                         'spider_name': spider.name})

        elif spider.name == 'tweet_spider':
            pending_url_collection.save({'_id': get_urls_by_id('comment', item['_id']),
                                         'persisted_time': int(time.time()),
                                         'spider_name': spider.name})
            if len(item['origin_weibo']) > 0:
                pending_url_collection.save({'_id': get_urls_by_id('single_tweet', item['origin_weibo']),
                                             'persisted_time': int(time.time()),
                                             'spider_name': spider.name})
        pass



