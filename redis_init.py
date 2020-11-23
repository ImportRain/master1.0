#!/usr/bin/env python
# encoding: utf-8

import datetime
import redis
import sys
import pymongo
from settings import MONGO_PORT, MONGO_HOST

client = pymongo.MongoClient(MONGO_HOST, MONGO_PORT)

def redis_init(spider_name, urls, use_error_urls=True, append=False):
    r = redis.Redis(host='redis')
    if not append: #添加新的url时，判断是否要清空现有的urls集里面的内容
        print(f'Remove all keys match {spider_name}* ')
        for key in r.scan_iter(f"{spider_name}*"):
            r.delete(key)
    print(f'Add urls to {spider_name}:start_urls')

    error_urls_collection = client['weibo']['error_urls']
    error_urls_count = error_urls_collection.find({"spider_name": spider_name}).count()

    if error_urls_count != 0 and use_error_urls:
        urls = [u['_id'] for u in error_urls_collection.find({"spider_name": spider_name})]
        print(f'Use error urls cached from {spider_name} last run')

    # todo: 对 pending_urls 进行计数并加入到 urls 中
    #       参考 error_urls_collection、error_urls_count 的相关代码
    pending_urls_collection = client['weibo']['pending_urls']
    pending_urls_count = pending_urls_collection.find({"spider_name": spider_name}).count()

    if pending_urls_count != 0:
        urls = [u['_id'] for u in pending_urls_collection.find({"spider_name": spider_name})]
        print(f'Add urls to {spider_name}: pending_urls')

    for url in urls:
        r.lpush(f'{spider_name}:start_urls', url)
        print(f'Added: ', url)

    if error_urls_count != 0 and use_error_urls:
        print(f'Clear cached error urls from {spider_name} last run')
        error_urls_collection.drop()
        pass

    if pending_urls_count != 0:
        for pending_url in pending_urls_collection:
            r.lpush(f'{spider_name}:pending_urls', pending_url)
            print(f'Added: ', pending_url)

#定义系列种子数据
def init_user_spider(**kwargs):
    # change the user ids
    user_ids = ['1087770692', '1699432410', '1266321801']
    urls = [f"https://weibo.cn/{user_id}/info" for user_id in user_ids]
    redis_init('user_spider', urls, **kwargs)


def init_fan_spider(**kwargs):
    # change the user ids
    user_ids = ['1087770692', '1699432410', '1266321801']
    urls = [f"https://weibo.cn/{user_id}/fans?page=1" for user_id in user_ids]
    redis_init('fan_spider', urls, **kwargs)


def init_follow_spider(**kwargs):
    # change the user ids
    user_ids = ['1087770692', '1699432410', '1266321801']
    urls = [f"https://weibo.cn/{user_id}/follow?page=1" for user_id in user_ids]
    redis_init('follower_spider', urls, **kwargs)


def init_comment_spider(**kwargs):
    # change the tweet ids
    tweet_ids = ['IDl56i8av', 'IDkNerVCG', 'IDkJ83QaY']
    urls = [f"https://weibo.cn/comment/hot/{tweet_id}?rl=1&page=1" for tweet_id in tweet_ids]
    redis_init('comment_spider', urls, **kwargs)


def init_user_tweets_spider(**kwargs):
    # crawl tweets post by users
    user_ids = ['1087770692', '1699432410', '1266321801']
    urls = [f'https://weibo.cn/{user_id}/profile?page=1' for user_id in user_ids]
    redis_init('tweet_spider', urls, **kwargs)


def init_single_tweet_spider(**kwargs):
    tweet_ids = ['IDl56i8av', 'IDkNerVCG', 'IDkJ83QaY']
    urls = [f"https://weibo.cn/comment/{tweet_id}?page=1" for tweet_id in tweet_ids]
    redis_init('single_tweet_spider', urls, **kwargs)


def init_keyword_tweets_spider(**kwargs):
    # crawl tweets include keywords in a period, you can change the following keywords and date
    keywords = ['转基因']
    date_start = datetime.datetime.strptime("2017-07-30", '%Y-%m-%d')
    date_end = datetime.datetime.strptime("2018-07-30", '%Y-%m-%d')
    time_spread = datetime.timedelta(days=1)
    urls = []
    url_format = "https://weibo.cn/search/mblog?hideSearchFrame=&keyword={}" \
                 "&advancedfilter=1&starttime={}&endtime={}&sort=time&page=1"
    while date_start < date_end:
        next_time = date_start + time_spread
        urls.extend(
            [url_format.format(keyword, date_start.strftime("%Y%m%d"), next_time.strftime("%Y%m%d"))
             for keyword in keywords]
        )
        date_start = next_time
    redis_init('tweet_spider', urls, **kwargs)


if __name__ == '__main__':
    '''
    -a append，在原有的redis队列中追加url，否则清空所有相关数据。
    -e use_error_urls，从MongoDB中读取之前错误的url，否则使用指定的url。
    '''
    mode = sys.argv[1]
    append = False
    use_error_urls = False
    if len(sys.argv) > 2 and '-a' in sys.argv:
        append = True
    if len(sys.argv) > 2 and '-e' in sys.argv:
        use_error_urls = True
    kwargs = {'append': append, 'use_error_urls': use_error_urls}
    mode_to_fun = {
        'user': init_user_spider,
        'comment': init_comment_spider,
        'fan': init_fan_spider,
        'follow': init_follow_spider,
        'single_tweet': init_single_tweet_spider,
        'tweet_by_user_id': init_user_tweets_spider,
        'tweet_by_keyword': init_keyword_tweets_spider,
    }
    if mode != 'all':
        mode_to_fun[mode](**kwargs)
    else:
        print('Initialize all in redis')
        for f in mode_to_fun.values():
            f(**kwargs)
