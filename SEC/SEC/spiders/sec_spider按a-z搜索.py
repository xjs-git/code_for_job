# -*- coding: utf-8 -*-
import scrapy
from SEC.items import CompanySearchItem,NoticeListItem,NoticeDetailItem
import re
import datetime
import pymysql
import pandas as pd
import os

class SecSpiderSpider(scrapy.Spider):
    name = 'old_sec_spider'
    allowed_domains = ['sec.gov']

    #从csv读取cik,返回一个列表
    def get_cik_fromcsv(self):
        file_path = 'C:/Users/熊京生/SEC/SEC/spiders'
        file_name = 'cik.csv'

        os.chdir(file_path)  # 切换到csv所在路径，方便读取数据
        # 将csv文件数据转化为list
        data = pd.read_csv(file_name)
        list = data.values.tolist()
        ciks = []  # 存储的是要提取的数据
        for i in range(len(list)):
            ciks.append(list[i][0])
        return ciks

    # 构造公司搜索的url列表，'https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&company={a-z}&&start=&count=100'
    def start_requests(self):
        #通过构造a-z搜索公司
        urls_companyname = []
        #91
        for i in range(65,91):
            company = chr(i)
            url = 'https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&company={}&start=0&count=100'.format(
                company)
            urls_companyname.append(url)
        for url in urls_companyname:
            yield scrapy.Request(url=url, callback=self.parse)

        #通过cik搜索公司
        #从csv导入1-0001788300 的cik
        #构造0001788301-9999999999 的url
        ciks_fromcsv=self.get_cik_fromcsv()
        for cik in ciks_fromcsv:
            url='https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={}&start=0&count=100'.format(cik)
            yield scrapy.Request(url=url,callback=self.parse)
        for i in range(1788301,9999999999):
            url='https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={}&start=0&count=100'.format(i)
            yield scrapy.Request(url=url, callback=self.parse)


    # 解析公司搜索页数据
    def parse(self, response):
        trlist = response.xpath('//table[@class="tableFile2"]/tr')
        for tr in trlist:
            cik_url = tr.xpath('./td[1]/a/@href').extract_first()
            # cik_url不为空，请求cik_url
            if cik_url:
                com_search = CompanySearchItem()
                com_search["cik"] = tr.xpath('./td[1]/a/text()').extract_first()
                com_search["cik_url"] = "https://www.sec.gov/" + cik_url
                company_name = "".join(tr.xpath('./td[2]//text()').extract())
                com_search["company_name"] = company_name.replace("\'", "\'\'")
                com_search["state"] = tr.xpath('./td[3]/a/text()').extract_first()
                com_search["add_time"] = datetime.datetime.now()
                com_search["update_time"] = datetime.datetime.now()
                yield scrapy.Request(url=com_search["cik_url"], callback=self.parse_notice,
                                         meta={'com_search': com_search})

        #请求下一页url
        next = response.xpath('//input[@value="Next 100"]').extract()
        #能找到next标签，请求下一页，否则结束该url
        if len(next):
            result = re.findall(r'start=(.*)&', response.url)
            number = result[0]
            number = str(int(number) + 100)
            next_url = re.sub(result[0], number, response.url, 1)
            yield scrapy.Request(url=next_url, callback=self.parse)

    # 解析公告页数据
    def parse_notice(self, response):
        com_search = response.meta['com_search']

        #连接数据库查询cik是否存在，若存在，则该公司已被爬取完毕，否则爬取该公司
        conn = pymysql.connect(host="10.1.18.72", port=3306,
                               user="root", passwd="123456", database='sec_spider', charset="utf8", )
        cursor = conn.cursor()
        result=cursor.execute('SELECT * FROM company WHERE cik="{}"'.format(com_search["cik"]))
        #result=0 未查询到cik
        if result == 0:
            trlist = response.xpath('//table[@class="tableFile2"]/tr')
            com_search["sic"] = response.xpath(
                '//p[@class="identInfo"]/acronym[contains(text(),"SIC")]/following-sibling::a[1]/text()').extract_first()
            com_search["business_address"] = "|".join(
                [i.strip() for i in response.xpath('//div[contains(text(),"Business Address")]//text()').extract()])
            mailing_address = "|".join(
                [i.strip() for i in response.xpath('//div[contains(text(),"Mailing Address")]//text()').extract()])
            com_search["mailing_address"] = mailing_address.replace("'", "''")
            texts = response.xpath('//p[@class="identInfo"]//text()').extract()
            result = False
            for text in texts:
                if re.findall(r'formerly:(.+)', text):
                    com_search["formerly"] = text.replace("'", "''")
                    result = True
            if result == False:
                com_search["formerly"] = "None"
            yield com_search

            for tr in trlist:
                notice_url = tr.xpath('./td[2]/a/@href').extract_first()
                if notice_url:
                    # 解析公告页数据
                    notice_list=NoticeListItem()
                    notice_list["filings"] = tr.xpath('./td[1]/text()').extract_first()
                    notice_list["notice_url"] = "https://www.sec.gov/" + notice_url
                    format = tr.xpath('./td[2]/a/text()').extract()
                    notice_list["notice_format"] = "|".join([i.strip() for i in format])
                    des_list = tr.xpath('./td[3]//text()').extract()
                    # 去除换行符
                    des_list = [des.strip() for des in des_list]
                    notice_list["notice_description"] = "".join(des_list)
                    notice_list["publice_date"] = tr.xpath('./td[4]/text()').extract_first()
                    notice_list["file_film_number"] = "|".join([i.strip() for i in tr.xpath('./td[5]//text()').extract()])
                    notice_list["company_cik"] = com_search["cik"]
                    yield notice_list
                    # 请求公告详情页
                    yield scrapy.Request(url=notice_list["notice_url"], callback=self.parse_notice_detail,
                                         meta={'notice_list': notice_list,'com_search':com_search})

            # ==============================================================================================
            #请求公告页下一页url
            next = response.xpath('//input[@value="Next 100"]').extract()
            # 能找到next标签，请求下一页，否则结束该url
            if len(next):
                # 获取下一页的url start=ka-1+40
                ka = re.findall(r'Items (.+) -', response.xpath('//td[contains(text(),"Items")]/text()').extract_first())
                next_notice_url = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={}&start={}&count=100".format(
                    com_search["cik"], int(ka[0]) - 1 + 100)
                yield scrapy.Request(url=next_notice_url, callback=self.parse_notice,meta={'com_search': com_search})

        else:
            print("{}对应的公司已被爬取".format(com_search["cik"]))

    # 解析公告详情页数据
    def parse_notice_detail(self, response):
        notice_list = response.meta['notice_list']
        com_search = response.meta['com_search']
        notice_detail=NoticeDetailItem()
        # 解析公告详情页数据
        notice_detail["filing_date"] = response.xpath(
            '//div[contains(text(),"Filing Date")]//following-sibling::div[1]/text()').extract_first()
        notice_detail["accepted"] = response.xpath(
            '//div[contains(text(),"Accepted")]//following-sibling::div[1]/text()').extract_first()
        notice_detail["period_of_report"] = response.xpath(
            '//div[contains(text(),"Period of Report")]//following-sibling::div[1]/text()').extract_first()
        notice_detail["effectiveness_date"] = response.xpath(
            '//div[contains(text(),"Effectiveness Date")]//following-sibling::div[1]/text()').extract_first()
        # items若是带 ' 插入mysql数据库会报错，需要将 ' 替换成 ''
        items_name = response.xpath('//div[contains(text(),"Items")]//following-sibling::div[1]/text()').extract_first()
        if items_name:
            notice_detail["items"] = items_name.replace("'", "''")
        else:
            notice_detail["items"] = "None"
        trlist = response.xpath('//table[@class="tableFile"]/tr')
        for tr in trlist:
            notice_detail["doc_files"] = tr.xpath('.//../@summary').extract_first()
            detail_description = "".join(tr.xpath('./td[2]/text()').extract())
            notice_detail["detail_description"] = detail_description.replace("'", "''")
            doc_url = tr.xpath('./td[3]/a/@href').extract_first()
            notice_detail["doc_url"] = "https://www.sec.gov/" + doc_url if doc_url else doc_url
            notice_detail["doc_size"] = tr.xpath('./td[5]/text()').extract_first()
            notice_detail["doc_name"] = tr.xpath('./td[3]/a/text()').extract_first()
            notice_detail["doc_tpye"] = tr.xpath('./td[4]/text()').extract_first()
            notice_detail["from_notice"] = notice_list["notice_url"]
            notice_detail["from_cik"] = com_search["cik_url"]
            # 不是空数据，返回数据
            if (notice_detail["detail_description"] or notice_detail["doc_name"] or notice_detail["doc_tpye"] or notice_detail["doc_size"] ):
                yield notice_detail