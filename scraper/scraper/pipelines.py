# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

import re
from scrapy.exceptions import DropItem


class Error(Exception):

    pass


class UnknownFieldError(Error):

    pass


class CleaningError(Error):

    def __init__(self, drop=False):
        self.drop = drop


# TODO: CleanUpPipeline
class CleanUpPipeline(object):

    def __init__(self):
        self._cleaners = {
            'facility_id': self._id_cleaner,
            'facility_name': self._string_cleaner,
            'facility_address': self._address_cleaner,
            'inspection_id': self._id_cleaner,
            'inspection_date': self._date_cleaner,
            'critical_violations': self._violations_cleaner,
            'non_critical_violations': self._violations_cleaner
        }
        self.logger = None

    def process_item(self, item, spider):
        self.logger = spider.logger
        for field, value in item.items():
            self.logger.debug("Cleaning field `%s` with value `%s`.", field, value)
            try:
                cleaner = self._cleaners.get(field, self._unknown_field)
                item[field] = cleaner(value)
            except CleaningError as e:
                msg = "Failed to parse value `{}` for field `{}` using `{}`.".format(value, field, cleaner)
                if e.drop:
                    raise DropItem(msg)
                else:
                    self.logger.error(msg)
                    item[field] = None
            except UnknownFieldError:
                self.logger.error("Unknown field `%s` encountered with value `%s`.", field, value)
                del item[field]
        return item


    def _id_cleaner(self, value):
        try:
            return int(value.strip())
        except ValueError:
            # an id is required for the record
            raise CleaningError(True)

    def _string_cleaner(self, value):
        return value.strip()

    def _address_cleaner(self, value):
        if not isinstance(value, list):
            # it's not essential that we get this field on the first attempt
            raise CleaningError()
        address_parts = [s.strip() for s in value if s.strip()]
        address = ', '.join(address_parts)
        address = re.sub(r'\s+', ' ', address)
        return address

    def _date_cleaner(self, value):
        # TODO: parse into a date object
        return self._string_cleaner(value)

    def _violations_cleaner(self, value):
        # TODO: actually make entries for violations
        if not isinstance(value, list):
            raise CleaningError(True)
        return len(value)

    def _unknown_field(self, value):
        raise UnknownFieldError()


# TODO: DuplicatesPipeline
