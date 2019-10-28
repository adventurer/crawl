# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy
from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose, TakeFirst, Join, Identity
import re


def noneToEmpty(value):
    if value=='None':
        return ''
    else:
        return value

def getNum(value):
    if value!='':
        num = re.findall(r"(\d+)",value)
        print("find number:",value)
        print("replace number:","".join(num))
        return "".join(num)
    else:
        return value

def intOrEmpty(value):
    try:
        if isinstance(int(value),int):
            return value
        else:
            return ''
    except ValueError:
        return ''


def extractPNG(value):
    if value!='':
        png = re.findall(r"(\w.*\.png)",value)
        return png
    else:
        return value

class ConnectItem(object):
    
    def __call__(self, values):
        return ','.join(values)


class OriginItem(object):
    
    def __call__(self, values):
        return values

class CrawlItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    pass

class BeikeJobItemLoader(ItemLoader):
    default_output_processor = ConnectItem()

class BeikeAlbumItemLoader(ItemLoader):
    default_output_processor = OriginItem()

class BeikeItem(scrapy.Item):
    city = scrapy.Field()  # 房源名称
    city_en = scrapy.Field()
    sale = scrapy.Field()  # 房源名称
    type = scrapy.Field()  # 房源名称
    title = scrapy.Field()  # 房源名称
    address = scrapy.Field()  # 房源名称
    area = scrapy.Field(input_processor=MapCompose(getNum))  # 房源名称
    priceavg = scrapy.Field(input_processor=MapCompose(getNum))  # 房源名称
    priceall = scrapy.Field(input_processor=MapCompose(getNum))  # 房源名称
    href = scrapy.Field()  # 房源名称
    img = scrapy.Field(input_processor=MapCompose(extractPNG))  # 房源名称
    tags = scrapy.Field()  # 房源名称

class BeikeItemLocation(scrapy.Item):
    href = scrapy.Field()
    latitude = scrapy.Field()
    longitude = scrapy.Field()


class BeikeItemAlbum(scrapy.Item):
    image_urls = scrapy.Field()
    href = scrapy.Field()
    type = scrapy.Field()
    type_en = scrapy.Field()

class BeikeItemAlbumUp(scrapy.Item):
    locate = scrapy.Field()
    checksum = scrapy.Field()
    isdown = scrapy.Field()

class BeikeItemAlbumHouseType(scrapy.Item):
    image_urls = scrapy.Field()
    href = scrapy.Field()
    type = scrapy.Field()
    type_en = scrapy.Field()
    locate = scrapy.Field()
    ht = scrapy.Field()
    area = scrapy.Field()
    price = scrapy.Field()
    checksum = scrapy.Field()
    path = scrapy.Field()

class BeikeItemBasic(scrapy.Item):
    href = scrapy.Field()
    type = scrapy.Field()
    feture = scrapy.Field()
    address = scrapy.Field()
    sale_address = scrapy.Field()
    merchant = scrapy.Field()
    price = scrapy.Field()
    location = scrapy.Field()

class BeikeItemProgramme(scrapy.Item):
    href = scrapy.Field()
    type = scrapy.Field()
    area_floor = scrapy.Field(input_processor=MapCompose(getNum))
    area_building = scrapy.Field(input_processor=MapCompose(getNum))
    house_cnt = scrapy.Field()
    years = scrapy.Field()
    r_green = scrapy.Field()
    r_volume = scrapy.Field()

class BeikeItemSupport(scrapy.Item):
    href = scrapy.Field()
    manager = scrapy.Field()
    fee = scrapy.Field()
    water = scrapy.Field()
    electric = scrapy.Field()
    warm = scrapy.Field()
    parking = scrapy.Field()
    r_parking = scrapy.Field()
    support = scrapy.Field()
    subway = scrapy.Field()
    study = scrapy.Field()
    hospital = scrapy.Field()
    shop = scrapy.Field()

class BeikeItemEvent(scrapy.Item):
    href = scrapy.Field()
    event = scrapy.Field()
    time = scrapy.Field()
    target = scrapy.Field()
