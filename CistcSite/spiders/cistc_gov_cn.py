import json

import scrapy
import re


class CistcGovCnSpider(scrapy.Spider):
    name = 'cistc_gov_cn'
    page_now = 1        # 起始页数
    page_end = 10       # 结束页数
    allowed_domains = ['www.cistc.gov.cn']
    start_urls = [
        f'http://www.cistc.gov.cn/handlers/cistcMenuInfoList.ashx?columnid=222&isall=1&keyword=&year=&pagenum={page_now}'
    ]
    regex = re.compile(r'infoDetail\.html\?id=(\d+)')

    def parse(self, response, **kwargs):
        # 正则获取文章ID
        reg_result = self.regex.findall(response.text)
        # 如果获取不到则数据爬取完毕
        if reg_result is None:
            return
        for post_id in reg_result:
            yield scrapy.Request(
                f'http://www.cistc.gov.cn/handlers/cistcInfo.ashx?infoid={post_id}&contentLenth=&column=222',
                callback=self.pares_content
            )
        # 爬取页数满足条件跳出
        if self.page_now == self.page_end:
            return
        # 下一页
        self.page_now += 1
        yield scrapy.Request(
            f'http://www.cistc.gov.cn/handlers/cistcMenuInfoList.ashx?columnid=222&isall=1&keyword=&year=&pagenum={self.page_now}'
        )

    @staticmethod
    def pares_content(response):
        json_data = json.loads(response.text)
        return {
            'url': response.url,
            'title': json_data.get("InfoTitle"),
            'dates': json_data.get('InfoPublicDate'),
            'content': json_data.get('InfoContent')
        }
