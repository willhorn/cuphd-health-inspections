# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class Facility(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    facility_id = scrapy.Field()
    facility_name = scrapy.Field()
    facility_address = scrapy.Field()


class Inspection(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    facility_id = scrapy.Field()
    inspection_id = scrapy.Field()
    inspection_date = scrapy.Field()
    critical_violations = scrapy.Field()
    non_critical_violations = scrapy.Field()
