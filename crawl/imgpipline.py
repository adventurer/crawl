import scrapy
from scrapy.pipelines.images import ImagesPipeline
from scrapy.exceptions import DropItem
from crawl.items import BeikeAlbumItemLoader,BeikeItemAlbum,BeikeItemAlbumHouseType

import pymysql.cursors
import re



class MyImagesPipeline(ImagesPipeline):
    # 图片下载完成时保存item信息和图片信息
    def item_completed(self, results, item, info):

        connection = pymysql.connect(host='192.168.1.202',
                             user='root',
                             password='112215334',
                             db='beike',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)

        # 补充item信息到数据库
        values = []
        sql = ''
        if isinstance(item,BeikeItemAlbum):
            sql = """ insert into houses_album(type,type_en,href,locate,checksum,path,isdown) values (%s,%s,%s,%s,%s,%s,%s) """
            for image_url in item['image_urls']:
                for result in results:
                    if image_url == result[1]['url']:
                        values.append((item['type'],item['type_en'],item['href'][0],image_url,result[1]['checksum'],result[1]['path'],result[0]))
        elif isinstance(item,BeikeItemAlbumHouseType):
            sql = """ insert into houses_type(type,type_en,href,locate,ht,area,price,checksum,path,isdown) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) """
            for image_url in item['image_urls']:
                for result in results:
                    if image_url == result[1]['url']:
                        values.append((item['type'],item['type_en'],item['href'][0],image_url,self.extractNumberAndConnect(item['ht']),self.getNum(item['area'][0]),item['price'],result[1]['checksum'],result[1]['path'],result[0]))

        for v in values:
                connection.cursor().execute(sql,v)
                connection.commit()

        connection.close()


    def extractNumberAndConnect(self,value):
        string = ''
        if value!='':
            arr = re.findall(r"[0-9]",value[0])
            return '-'.join(arr)
        else:
            return string


    def getNum(self,value):
        if value!='':
            num = re.findall(r"(\d+)",value)
            return num
        else:
            return value