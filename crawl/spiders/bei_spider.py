import scrapy
import logging
import math
from crawl.items import BeikeItem,BeikeJobItemLoader,BeikeAlbumItemLoader,BeikeItemLocation,BeikeItemAlbum
from bs4 import BeautifulSoup
from xpinyin import Pinyin
import re

from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor

# from scrapy.utils.response import open_in_browser


class beikeSpider(CrawlSpider):
    name = "bei"
    # allowed_domains = ['sz.fang.ke.com']
    start_urls = [
        'https://sz.fang.ke.com/'
    ]

    rules = [
        Rule(LinkExtractor(allow=(r'loupan/\w+',)),callback='parse_house',follow=True),
        # Rule(LinkExtractor(restrict_xpaths=('//div[@class="city-enum"]')),callback='getDetail',follow=False)
    ]

    def getDetail(self, response):
        print("访问页面：",response.url)


    # def parse(self, response):
    #     for href in response.css('.fc-main .fl a::attr(href)').extract():
    #         # self.start_urls.append(href)
    #         yield response.follow(href, self.parse)

    #     for count in response.xpath("/html/body/div[5]/div[2]/span[2]//text()").extract():
    #         print("发现rul："+response.url+"条目：",count)

    #         if int(count) > 0:
    #                 maxPage = int(count)/10
    #                 if maxPage > 100:
    #                     for x in range(1,100):
    #                         print("跟进rul：",response.url+'pg'+str(x))
    #                         yield response.follow(response.url+'/pg'+str(x), self.parse_house)
    #                 else:
    #                     for x in range(1,math.ceil(int(count)/10)):
    #                         print("跟进rul：",response.url+'pg'+str(x))
    #                         yield response.follow(response.url+'/pg'+str(x), self.parse_house)

    def parse_house(self, response):
        print("访问列表页：",response.url)
        data = response.body
        soup = BeautifulSoup(data, "html5lib")
        pin = Pinyin()

        city = response.xpath('/html/body/div[1]/div[1]/div[1]/a[2]/text()').extract_first()
        houses = soup.find_all('li','resblock-list')
        for house in houses:
            title = house.find('div',class_='resblock-name').find('a',class_='name').get_text()
            sale = house.find('div',class_='resblock-name').find('a',class_='name').find_next('span').get_text()
            _type = house.find('a',class_='name').find_next('span').find_next('span').get_text()
            address = house.find('a',class_='resblock-location').get_text("|", strip=True)
            area = ''
            obj_area = house.find(class_='area')
            if obj_area:
                area = obj_area.get_text("|", strip=True)

            priceavg = house.find('span',class_='number').get_text("|", strip=True)

            priceall_obj = house.find(class_='second')

            priceall = ''
            if priceall_obj is not None:
                priceall = priceall_obj.get_text("|", strip=True)


            href = response.urljoin(house.find('a',class_='name').get('href'))
            img = house.find('img',class_='lj-lazy').get('src')

            tagsarr = []
            tags_obj = house.find('div',class_='resblock-tag').find_all('span')

            if tags_obj is not None:
                for tag_index in tags_obj:
                    tagsarr.append(tag_index.get_text())
                tags = ','.join(tagsarr)
            else:
                tags = ''

            item_loader = BeikeJobItemLoader(item=BeikeItem(),response=response)
            item_loader.add_value('city',city)
            item_loader.add_value('city_en',pin.get_pinyin(city))
            item_loader.add_value('title',title)
            item_loader.add_value('sale',sale)
            item_loader.add_value('type',_type)
            item_loader.add_value('address',address)
            item_loader.add_value('area',area)
            item_loader.add_value('priceavg',priceavg)
            item_loader.add_value('priceall',priceall)
            item_loader.add_value('href',href)
            item_loader.add_value('img',img)
            item_loader.add_value('tags',tags)
            item = item_loader.load_item()
            yield item
            # yield response.follow(item['href'], self.parse_detail)
            # yield response.follow(item['href']+"xiangce", self.parse_album)



    def parse_detail(self,response):
        print("进入详情页面："+response.css("title::text").extract_first())
        data = response.body
        soup = BeautifulSoup(data, "html5lib")

        location = re.findall(r"point:.*(\d{2}\.\d{4,6}).*(\d{3}\.\d{4,6}).*\]",soup.get_text())
        print(location)
        item_loader = BeikeJobItemLoader(item=BeikeItemLocation(),response=response)
        item_loader.add_value('href',response.url)
        item_loader.add_value('latitude',location[0][0])
        item_loader.add_value('longitude',location[0][1])
        item = item_loader.load_item()
        yield item

    def parse_album(self,response):
        print("进入相册页面："+response.css("title::text").extract_first())
        pin = Pinyin()
        data = response.body
        soup = BeautifulSoup(data, "html5lib")

        items = soup.find_all('div',class_='tab-group')
        # print(items)
        for itemv in items:
            item_loader = BeikeAlbumItemLoader(item=BeikeItemAlbum(),response=response)

            type_ = itemv.find('h4').find('a').get_text()
            images = itemv.find_all('img')
            imgs = []
            for img in images:
                img_clena = self.get_img_url(img['src'])
                if img_clena is not None:
                    imgs.append(img_clena)
            item_loader.add_value('type',type_)
            item_loader.add_value('type_en',pin.get_pinyin(self.chinese_to_en(type_)))
            item_loader.add_value('image_urls',imgs)
            item_loader.add_value('href',response.url.strip('xiangce'))
            item = item_loader.load_item()
            yield item
        
    def get_img_url(self,url):
        return url
        src = re.findall(r"(\w.*\.jpg)",url)
        if len(src) > 0:
            return src[0]
        else:
            return None

    def chinese_to_en(self,value):
        en = re.findall(r"[\u4e00-\u9fa5]+",value)
        if len(en) > 0:
            return en[0]
        else:
            return value