# run with `scrapy crawl il.healthinspections.us` from top scraper directory

import urllib.parse
import scrapy.exceptions
import logging
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor


class ReportsSpider(CrawlSpider):
    logger = logging.getLogger('il.healthinspections.us')
    name = 'il.healthinspections.us'
    allowed_domains = ['il.healthinspections.us']
    # first search result page
    # TODO: parameterize the crawler to take an arbitrary start date
    # TODO: set the end date to today
    start_urls = ['http://il.healthinspections.us/champaign/search.cfm?1=1&sd=01/01/2008&ed=06/04/2017&kw1=&kw2=&kw3=&rel1=A.organization_facility&rel2=A.organization_facility&rel3=A.organization_facility&zc=&dtRng=YES&pre=similar&lhd=all&riskCategory=all&asrTo=&asrFrom=&ncv=any']
    ref_url = None

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

    # override to add the ref url in the meta data
    # I'll need this to link an inspection report to a facility id
    # could also use scrapy.utils.request.referer_str
    def _requests_to_follow(self, response):
        for request_or_item in super(ReportsSpider, self)._requests_to_follow(response):
            if isinstance(request_or_item, scrapy.Request):
                request_or_item.meta['ref_url'] = response.url
            yield request_or_item

    def parse_facility_page(self, response):
        # TODO: get the facility name and location
        return

    def parse_inspection_report(self, response):
        facility_id = self._get_parameter_value(response.meta['ref_url'], 'facilityID')
        inspection_id = self._get_parameter_value(response.url, 'inspectionID')
        self.logger.info('facility: {}; inspection: {}'.format(facility_id, inspection_id))
        # kill the crawl early for now
        raise scrapy.exceptions.CloseSpider('done')

    def _get_parameter_value(self, url, key):
        parsed_url = urllib.parse.urlparse(url)
        qdict = urllib.parse.parse_qs(parsed_url.query)
        value = qdict.get(key)
        if value:
            return value[0]
        return None
