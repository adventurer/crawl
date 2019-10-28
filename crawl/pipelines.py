# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

import pymysql.cursors
from crawl.items import BeikeItem,BeikeItemLocation,BeikeItemAlbum,BeikeItemAlbumUp,BeikeItemBasic,BeikeItemProgramme,BeikeItemSupport,BeikeItemEvent

class MySQLPipeline(object):
    def __init__(self):
        # 连接数据库
        self.connect = pymysql.connect(
            host='192.168.1.202',  # 数据库地址
            port=3306,  # 数据库端口
            db='beike',  # 数据库名
            user='root',  # 数据库用户名
            passwd='112215334',  # 数据库密码
            charset='utf8',  # 编码方式
            use_unicode=True)
        # 通过cursor执行增删查改
        self.cursor = self.connect.cursor()

    def process_item(self, item, spider):
        values = []
        if isinstance(item,BeikeItem):
            sql = """ select id from house_house where href = %s """
            self.cursor.execute(sql,(item['href'],))
            result = self.cursor.fetchone()
            if result is None:
                sql = """insert into house_house(city,city_en, title, address, area, priceavg, priceall, href, img,tags,sale,type)
                    VALUES (%s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""" 
                values.append((item['city'],item['city_en'], item['title'], item['address'], item['area'], item['priceavg'], item['priceall'], item['href'], item['img'], item['tags'], item['sale'], item['type']))
            else:
                sql = """ update house_house set priceavg = %s,priceall = %s,sale = %s where id = %s """ 
                values.append((item['priceavg'],item['priceall'],item['sale'],result[0]))
        elif isinstance(item,BeikeItemLocation):
            sql = """update house_house set longitude = %s,latitude = %s where href = %s """ 
            values.append((item['longitude'],item['latitude'],item['href']))
        elif isinstance(item,BeikeItemAlbum):
            pass
            # sql = """select id from houses_album where href = %s """ 
            # value = (item['href'][0],)
            # self.cursor.execute(sql,value)
            # result = self.cursor.fetchone()
            # if result is None:
            #     sql = """ insert into houses_album(type,type_en,href,locate) values (%s,%s,%s,%s) """
            #     for image_url in item['image_urls']:
            #         values.append((item['type'],item['type_en'],item['href'][0],image_url,))
        elif isinstance(item,BeikeItemAlbumUp):
            print("ssss")
        elif isinstance(item,BeikeItemBasic):
            sql = """ insert into house_info_basic(href,type,feature,address,sale_address,merchant,price,location) 
                VALUES (%s,%s, %s, %s,%s,%s, %s, %s)
                """ 
            values.append((item['href'],item['type'],item['feture'],item['address'],item['sale_address'],item['merchant'],item['price'],item['location']))
        elif isinstance(item,BeikeItemProgramme):
            sql = """ insert into house_info_programme(href,type,area_floor,area_building,house_cnt,years,r_green,r_volume) 
            VALUES (%s,%s, %s, %s,%s,%s, %s, %s)
            """ 
            values.append((item['href'],item['type'],item['area_floor'],item['area_building'],item['house_cnt'],item['years'],item['r_green'],item['r_volume']))
        elif isinstance(item,BeikeItemSupport):
            sql = """ insert into house_info_support(href,manager,fee,water,electric,warm,parking,r_parking,support,subway,study,hospital,shop) 
            VALUES (%s,%s, %s, %s,%s,%s, %s, %s,%s,%s, %s, %s,%s)
            """ 
            values.append((item['href'],item['manager'],item['fee'],item['water'],item['electric'],
            item['warm'],item['parking'],item['r_parking'],item['support'],item['subway'],item['study'],item['hospital'],item['shop']))
        elif isinstance(item,BeikeItemEvent):
            sql = """ insert into house_info_event(href,time,event,target) values(%s,%s,%s,%s) """ 
            values.append((item['href'],item['time'],item['event'],item['target']))


        try:
            for v in values:
                self.cursor.execute(sql,v)
                self.connect.commit()
        except pymysql.err.InternalError as identifier:
            print(sql)
            print(values)
            print(identifier)
        
        return item

class MysqlTwistedPipeline(object):
    
    def __init__(self,dbpool):
        self.dbpool=dbpool

    @classmethod
    def from_settings(cls,setting):
        dbparams = dict(
            host=setting["MYSQL_HOST"],
            db=setting["MYSQL_DBNAME"],
            user=setting["MYSQL_USER"],
            password=setting["MYSQL_PASSWORD"],
            charset='utf8',
            cursorclass=pymysql.cursors.DictCursor,
            use_unicode=True
        )

        dbpool = pymysql.ConnectionPool("MySQLdb", **dbparams)

        return cls(dbpool)

    def process_item(self, item, spider):
        # 使用twisted将mysql插入变成异步执行
        query = self.dbpool.runInteraction(self.do_insert, item)
        query.addErrback(self.handle_error, item, spider)  # 处理异常

    def handle_error(self, failure, item, spider):
        # 处理异步插入的异常
        print(failure)

    def do_insert(self, cursor, item):
        # 执行具体的插入
        # 根据不同的item 构建不同的sql语句并插入到mysql中
        insert_sql, params = item.get_insert_sql()
        cursor.execute(insert_sql, params)