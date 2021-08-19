# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

# useful for handling different item types with a single interface

from scrapy.exceptions import DropItem
from pymongo import MongoClient
from pymysql import connect


class Duplicate:
    """
    数据去重
    """
    # 去重合集
    urls_seen = set()

    def process_item(self, item, spider):
        if item['url'] in self.urls_seen:
            raise DropItem('url %s exists' % item['url'])
        self.urls_seen.add(item['url'])
        return item


class MongodbPipeline:
    """
    Mongodb插入数据
    """
    # 表名
    collection_name = 'article'

    def __init__(self, mongo_url, mongo_db):
        self.client = MongoClient(mongo_url)
        self.db = self.client[mongo_db][self.collection_name]

    # 获取settings文件中Mongodb配置
    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            mongo_url=crawler.settings.get('MONGO_URL'),
            mongo_db=crawler.settings.get('MONGO_DATABASE', 'items')
        )

    def process_item(self, item, spider):
        self.db.insert_one(item)
        return item

    def close_process(self, spider):
        self.client.close()


class MysqlPipeline:
    """
    Mysql插入数据
    """

    sql = 'insert into article(title) values("%s——%s")'

    def __init__(self, host, port, dbname, user, password):
        self.db = connect(
            host=host,
            port=port,
            db=dbname,
            user=user,
            password=password
        )
        # 创建游标
        self.cursor = self.db.cursor()

    # 获取settings文件中的Mysql配置
    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            host=crawler.settings.get('MYSQL_HOST'),
            port=crawler.settings.get('MYSQL_PORT'),
            dbname=crawler.settings.get('MYSQL_DB'),
            user=crawler.settings.get('MYSQL_USER'),
            password=crawler.settings.get('MYSQL_PASSWORD')
        )

    # 插入文件到数据库
    def process_item(self, item, spider):
        self.cursor.execute(
            self.sql % (item['title'], item['dates'])
        )
        return item

    # 关闭爬虫时调用接口
    def close_process(self, spider):
        self.db.close()

