# Run with `scrapy crawl il.healthinspections.us` from top scraper directory
#  with optional options: `scrapy crawl il.healthinspections.us -a start_date=YYYY-MM-DD -a end_date=YYYY-MM-DD`
# As detailed in my blog post, this date range functionality does not seem to work properly in the inspections
#  database. However, I feel the work is still valuable and worth archiving.
# Test scraping with e.g.
#  `scrapy shell 'http://il.healthinspections.us/champaign/estab.cfm?facilityID=800' --set="ROBOTSTXT_OBEY=False"`

import datetime
import logging
import re
import urllib.parse

import scrapy.exceptions
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule

from ..items import Facility, Inspection


class ReportsSpider(CrawlSpider):
    name = 'il.healthinspections.us'
    logger = logging.getLogger('il.healthinspections.us')
    allowed_domains = ['il.healthinspections.us']
    rules = (
        # search result pages
        Rule(LinkExtractor(allow='search\.cfm\?start=.+')),
        # facility pages
        Rule(LinkExtractor(allow='estab\.cfm\?facilityID=.+'), callback='parse_facility_page', follow=True),
        # inspection reports
        Rule(
            LinkExtractor(allow='_templates/90/Food_Establishment_Inspection/_report_full\.cfm\?inspectionID=.+'),
            callback='parse_inspection_report'
        ),
    )
    xpaths = {
        'facility_name': '//div[@id="demographic"]/strong/text()',
        'facility_address': '//div[@id="demographic"]/i/text()',
        'inspection_date': '//table/tr/td/div[text()="Date:"]/parent::td/following-sibling::td/text()',
        'critical_violations': ('//tr[preceding-sibling::tr/td[normalize-space(text())="Critical Violations:"] and '
                                'following-sibling::tr/td[normalize-space(text())="Non-critical Violations:"]]/td[1]'
                                '[text() != "Item"]/text()'),
        'non_critical_violations': ('//tr[preceding-sibling::tr/td[normalize-space(text())="Non-critical Violations:"] '
                                    'and following-sibling::tr/td[normalize-space(text())="Inspector Comments:"]]/td[1]'
                                    '[text() != "Item"]/text()')
    }

    def __init__(self, start_date=None, end_date=None,  *args, **kwargs):
        super(ReportsSpider, self).__init__(*args, **kwargs)
        self.start_date = self._parse_date_parameter('start_date', start_date)
        self.end_date = self._parse_date_parameter('end_date', end_date)
        # first search result page
        start_url = ('http://il.healthinspections.us/champaign/search.cfm?1=1&sd={}&ed={}&kw1=&kw2=&kw3='
                     '&rel1=A.organization_facility&rel2=A.organization_facility&rel3=A.organization_facility&zc='
                     '&dtRng=YES&pre=similar&lhd=all&riskCategory=all&asrTo=&asrFrom=&ncv=any').format(
            datetime.date.strftime(self.start_date, '%m/%d/%Y'),
            datetime.date.strftime(self.end_date, '%m/%d/%Y')
        )
        self.start_urls = [start_url]
        self.ref_url = None

    def _parse_date_parameter(self, field, value):
        if value is None:
            if field == 'start_date':
                return datetime.date(2008, 1, 1)
            if field == 'end_date':
                return datetime.date.today()
            self.logger.error("No default value for available for '{}' parameter.".format(field))
            exit(1)
        if not re.fullmatch(r'\d{4}-\d\d-\d\d', value):
            self.logger.error("'{}' parameter value '{}' does not match the format YYYY-MM-DD.".format(field, value))
            exit(1)
        try:
            date_parts = [int(p) for p in value.split('-')]
            return datetime.date(*date_parts)
        except ValueError:
            self.logger.error("'{}' parameter value '{}' is not a proper date.".format(field, value))
            exit(1)

    # override to add the ref url in the meta data
    # I'll need this to link an inspection report to a facility id
    # could also use scrapy.utils.request.referer_str
    def _requests_to_follow(self, response):
        for request_or_item in super(ReportsSpider, self)._requests_to_follow(response):
            if isinstance(request_or_item, scrapy.Request):
                request_or_item.meta['ref_url'] = response.url
            yield request_or_item

    def parse_facility_page(self, response):
        yield Facility(
            facility_id=self._get_parameter_value(response.url, 'facilityID'),
            facility_name=response.xpath(self.xpaths['facility_name']).extract_first(),
            facility_address=response.xpath(self.xpaths['facility_address']).extract()
        )
        return

    def parse_inspection_report(self, response):
        yield Inspection(
            facility_id=self._get_parameter_value(response.meta['ref_url'], 'facilityID'),
            inspection_id=self._get_parameter_value(response.url, 'inspectionID'),
            inspection_date=response.xpath(self.xpaths['inspection_date']).extract_first(),
            critical_violations=response.xpath(self.xpaths['critical_violations']).extract(),
            non_critical_violations=response.xpath(self.xpaths['non_critical_violations']).extract()
        )
        # kill the crawl early for now
        raise scrapy.exceptions.CloseSpider('done')

    def _get_parameter_value(self, url, key):
        parsed_url = urllib.parse.urlparse(url)
        qdict = urllib.parse.parse_qs(parsed_url.query)
        value = qdict.get(key)
        if value:
            return value[0]
        return None
