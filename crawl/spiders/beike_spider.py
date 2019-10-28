import scrapy
import logging
import math
from crawl.items import BeikeItem, BeikeJobItemLoader, BeikeAlbumItemLoader, BeikeItemLocation, BeikeItemAlbum, \
    BeikeItemAlbumHouseType,BeikeItemBasic,BeikeItemProgramme,BeikeItemSupport,BeikeItemEvent
from bs4 import BeautifulSoup
from xpinyin import Pinyin
import re


# from scrapy.utils.response import open_in_browser


class beikeSpider(scrapy.Spider):
    name = "beike"
    allowed_domains = ['sz.fang.ke.com']
    start_urls = [
        'https://sz.fang.ke.com/loupan'
    ]

    def parse(self, response):
        for href in response.css('.fc-main .fl a::attr(href)').extract():
            # self.start_urls.append(href)
            yield response.follow(href, self.parse)

        for count in response.xpath("/html/body/div[5]/div[2]/span[2]//text()").extract():
            print("发现rul：" + response.url + "条目：", count)

            if int(count) > 0:
                maxPage = int(count) / 10
                if maxPage > 10:
                    for x in range(1, 10):
                        print("跟进rul-1：", response.url + '/pg' + str(x))
                        yield response.follow(response.url + '/pg' + str(x), self.parse_house)
                else:
                    for x in range(1, math.ceil(int(count) / 10)):
                        print("跟进rul-2：", response.url + '/pg' + str(x))
                        yield response.follow(response.url + '/pg' + str(x), self.parse_house)

    def parse_house(self, response):
        print("进入列表页：", response.url)
        data = response.body
        soup = BeautifulSoup(data, "html5lib")
        pin = Pinyin()

        city = response.xpath('/html/body/div[1]/div[1]/div[1]/a[2]/text()').extract_first()
        houses = soup.find_all('li', 'resblock-list')
        for house in houses:
            title = house.find('div', class_='resblock-name').find('a', class_='name').get_text()
            sale = house.find('div', class_='resblock-name').find('a', class_='name').find_next('span').get_text()
            _type = house.find('a', class_='name').find_next('span').find_next('span').get_text()
            address = house.find('a', class_='resblock-location').get_text("|", strip=True)
            area = ''
            obj_area = house.find(class_='area')
            if obj_area:
                area = obj_area.get_text("|", strip=True)

            priceavg = house.find('span', class_='number').get_text("|", strip=True)

            priceall_obj = house.find(class_='second')

            priceall = ''
            if priceall_obj is not None:
                priceall = priceall_obj.get_text("|", strip=True)

            href = response.urljoin(house.find('a', class_='name').get('href'))
            img = house.find('img', class_='lj-lazy').get('src')

            tagsarr = []
            tags_obj = house.find('div', class_='resblock-tag').find_all('span')

            if tags_obj is not None:
                for tag_index in tags_obj:
                    tagsarr.append(tag_index.get_text())
                tags = ','.join(tagsarr)
            else:
                tags = ''

            item_loader = BeikeJobItemLoader(item=BeikeItem(), response=response)
            item_loader.add_value('city', city)
            item_loader.add_value('city_en', pin.get_pinyin(city))
            item_loader.add_value('title', title)
            item_loader.add_value('sale', sale)
            item_loader.add_value('type', _type)
            item_loader.add_value('address', address)
            item_loader.add_value('area', area)
            item_loader.add_value('priceavg', priceavg)
            item_loader.add_value('priceall', priceall)
            item_loader.add_value('href', href)
            item_loader.add_value('img', img)
            item_loader.add_value('tags', tags)

            item = item_loader.load_item()
            yield item
            yield response.follow(item['href'], self.parse_detail)
            yield response.follow(item['href']+"xiangce", self.parse_album)
            yield response.follow(item['href'] + "huxingtu", self.parse_huxingtu)
            yield response.follow(item['href'] + "xiangqing", self.parse_xiangqing)

    def parse_detail(self, response):
        print("进入楼盘页面：" + response.css("title::text").extract_first())
        data = response.body
        soup = BeautifulSoup(data, "html5lib")

        location = re.findall(r"point:.*(\d{2}\.\d{4,6}).*(\d{3}\.\d{4,6}).*\]", soup.get_text())
        print(location)
        item_loader = BeikeJobItemLoader(item=BeikeItemLocation(), response=response)
        item_loader.add_value('href', response.url)
        if len(location):
            item_loader.add_value('latitude', location[0][0])
            item_loader.add_value('longitude', location[0][1])
        else:
            item_loader.add_value('latitude', "0")
            item_loader.add_value('longitude', "0")
        
        item = item_loader.load_item()
        yield item

    def parse_album(self, response):
        print("进入相册页面：" + response.css("title::text").extract_first())
        pin = Pinyin()
        data = response.body
        soup = BeautifulSoup(data, "html5lib")

        items = soup.find_all('div', class_='tab-group')
        # print(items)
        for itemv in items:
            item_loader = BeikeAlbumItemLoader(item=BeikeItemAlbum(), response=response)

            type_ = itemv.find('h4').find('a').get_text()
            images = itemv.find_all('img')
            imgs = []
            for img in images:
                img_clena = self.get_img_url(img['src'])
                if img_clena is not None:
                    imgs.append(img_clena)
            item_loader.add_value('type', type_)
            item_loader.add_value('type_en', pin.get_pinyin(self.chinese_to_en(type_)))
            item_loader.add_value('image_urls', imgs)
            item_loader.add_value('href', response.url.rstrip('xiangce'))
            item = item_loader.load_item()
            yield item

    def parse_huxingtu(self, response):
        print("进入户型图页面：" + response.css("title::text").extract_first())
        pin = Pinyin()
        data = response.body
        soup = BeautifulSoup(data, "html5lib")

        huxings = soup.find_all('li', class_='huxing-item')

        for huxing in huxings:
            type_ = '户型图'
            imgs = []
            images = huxing.find_all('img')
            for img in images:
                img_clena = self.get_img_url(img['src'])
                if img_clena is not None:
                    imgs.append(img_clena)

            item_loader = BeikeAlbumItemLoader(item=BeikeItemAlbumHouseType(), response=response)
            item_loader.add_value('type', type_)
            item_loader.add_value('type_en', pin.get_pinyin(self.chinese_to_en(type_)))
            item_loader.add_value('image_urls', imgs)
            item_loader.add_value('href', response.url.rstrip('huxingtu'))

            info = huxing.find('div',class_='info').find('ul').find_all('li')

            item_loader.add_value('locate', 'locate')
            item_loader.add_value('ht', info[0].get_text())
            item_loader.add_value('area', info[1].get_text())

            if huxing.find('div',class_='info').find('span',class_='price').find('i') is not None:
                price = huxing.find('div',class_='info').find('span',class_='price').find('i').get_text()
            else:
                price = '0'
            item_loader.add_value('price', price)
            item_loader.add_value('checksum', 'checksum')
            item_loader.add_value('path', 'path')

            item = item_loader.load_item()
            yield item

    def parse_xiangqing(self, response):
        print("进入详情页："+response.url)    
        data = response.body
        soup = BeautifulSoup(data, "html5lib")

        infos = soup.find_all('ul', class_='x-box')

        # 基本信息
        basics = infos[0].find_all('li')
        baisc_item = []
        for basic in basics:
            if basic.find('span',class_='label-val') is not None:
                baisc_item.append(basic.find('span',class_='label-val').get_text().strip())
            else:
                baisc_item.append('')
        item_loader = BeikeJobItemLoader(item=BeikeItemBasic(), response=response)
        item_loader.add_value('href', response.url.rstrip('xiangqing'))
        item_loader.add_value('type', baisc_item[0])
        item_loader.add_value('feture', baisc_item[2])
        item_loader.add_value('address', baisc_item[4])
        item_loader.add_value('sale_address', baisc_item[5])
        item_loader.add_value('merchant', baisc_item[6])
        item_loader.add_value('price', baisc_item[1])
        item_loader.add_value('location', baisc_item[3])
        item = item_loader.load_item()
        yield item

        # programme
        programmes = infos[1].find_all('li')
        programme_item = []
        for programme in programmes:
            if programme.find('span',class_='label-val') is not None:
                programme_item.append(programme.find('span',class_='label-val').get_text().strip())
            else:
                programme_item.append('')
        item_loader1 = BeikeJobItemLoader(item=BeikeItemProgramme(), response=response)
        item_loader1.add_value('href', response.url.rstrip('xiangqing'))
        item_loader1.add_value('type', programme_item[0])
        item_loader1.add_value('area_floor', programme_item[2])
        item_loader1.add_value('area_building', programme_item[4])
        item_loader1.add_value('house_cnt', programme_item[6])
        item_loader1.add_value('years', programme_item[7])
        item_loader1.add_value('r_green', programme_item[1])
        item_loader1.add_value('r_volume', programme_item[3])
        item1 = item_loader1.load_item()
        yield item1

        # support
        supports = infos[2].find_all('li')
        support_item = []
        for support in supports:
            if support.find('span',class_='label-val') is not None:
                support_item.append(support.find('span',class_='label-val').get_text().strip())
            else:
                support_item.append('')
        item_loader2 = BeikeJobItemLoader(item=BeikeItemSupport(), response=response)
        item_loader2.add_value('href', response.url.rstrip('xiangqing'))
        item_loader2.add_value('manager', support_item[0])
        item_loader2.add_value('fee', support_item[2])
        item_loader2.add_value('water', support_item[4])
        item_loader2.add_value('electric', support_item[5])
        item_loader2.add_value('warm', support_item[3])
        item_loader2.add_value('parking', support_item[6])
        item_loader2.add_value('r_parking', support_item[1])

        item_loader2.add_value('support', '')
        item_loader2.add_value('subway', '')
        item_loader2.add_value('study', '')
        item_loader2.add_value('hospital', '')
        item_loader2.add_value('shop', '')
        item2 = item_loader2.load_item()
        yield item2

        # events = []
        # event_list = soup.find_all('li',class_='fq-nbd')
        # for event_unit in event_list:
        #     event = event_unit.find_all('span');
        #     if len(event)==6 and len(re.findall(r"(\d+)-(\d+)-(\d+)",event[0].get_text())) > 0:
        #         x = {'href':response.url.rstrip('xiangqing'),'time':event[0].get_text().strip(),'event':event[2].get_text().strip(),'target':event[4].get_text().strip()}
        #         events.append(x)

        # item_loader3 = BeikeJobItemLoader(item=BeikeItemEvent(), response=response)
        # for event_i in events:
        #     item_loader3.add_value('href', event_i['href'])
        #     item_loader3.add_value('time', event_i['time'])
        #     item_loader3.add_value('event', event_i['event'])
        #     item_loader3.add_value('target', event_i['target'])
        #     item3 = item_loader3.load_item()
        # yield item3

        
                


            

    def get_img_url(self, url):
        return url
        src = re.findall(r"(\w.*\.jpg)", url)
        if len(src) > 0:
            return src[0]
        else:
            return None

    def chinese_to_en(self, value):
        en = re.findall(r"[\u4e00-\u9fa5]+", value)
        if len(en) > 0:
            return en[0]
        else:
            return value
