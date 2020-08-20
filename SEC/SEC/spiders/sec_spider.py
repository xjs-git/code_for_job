# -*- coding: utf-8 -*-
import scrapy
from SEC.items import CompanySearchItem,NoticeListItem,NoticeDetailItem
import re
import datetime
import pymysql
import pandas as pd
import os

class SecSpiderSpider(scrapy.Spider):
    name = 'sec_spider'
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


    # 解析公司公告页数据
    def parse(self, response):
        print("response_url:",response.url)
        #1.查看请求的页面是否有公司/cik等数据存在 有的话解析数据 没有直接结束

        #xpath提取到center 说明没有数据
        data=response.xpath("//center")
        #len(data)=0 说明有数据
        if len(data) == 0:
            com_search=CompanySearchItem()

            com_search["cik"] = response.xpath('//span[@class="companyName"]/a/text()').extract_first()[:10]
            com_search["cik_url"] = "https://www.sec.gov/" + response.xpath('//span[@class="companyName"]/a/@href').extract_first()
            company_name = response.xpath('//span[@class="companyName"]/text()').extract_first()
            com_search["company_name"] = company_name.replace("\'", "\'\'")
            com_search["state"] = response.xpath('//p[@class="identInfo"]/a[contains(@href,"/cgi-bin/browse-edgar?action=getcompany&State")]/text()').extract_first()
            com_search["add_time"] = datetime.datetime.now()
            com_search["update_time"] = datetime.datetime.now()
            sic_data = response.xpath(
                '//*[text()="SIC"]/following-sibling::text()[preceding-sibling::node()[1][local-name()="a"]][following-sibling::node()[local-name()="br"]][1]').extract_first()
            com_search["sic"] = response.xpath('//a[contains(@href,"/cgi-bin/browse-edgar?action=getcompany&SIC")]/text()').extract_first()+sic_data
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
            #返回company数据
            yield com_search

            trlist = response.xpath('//table[@class="tableFile2"]/tr')
            for tr in trlist:
                notice_url = tr.xpath('./td[2]/a/@href').extract_first()
                if notice_url:
                    # 解析公告页数据
                    notice_list = NoticeListItem()
                    notice_list["filings"] = tr.xpath('./td[1]/text()').extract_first()
                    notice_list["notice_url"] = "https://www.sec.gov/" + notice_url
                    format = tr.xpath('./td[2]/a/text()').extract()
                    notice_list["notice_format"] = "|".join([i.strip() for i in format])
                    des_list = tr.xpath('./td[3]//text()').extract()
                    # 去除换行符
                    des_list = [des.strip() for des in des_list]
                    notice_description="".join(des_list)
                    notice_list["notice_description"] = notice_description
                    notice_list["description"] = notice_description.split("Acc-no:")[0]
                    notice_list["acc_no"] = notice_description.split("Acc-no:")[1].split("Size:")[0]
                    notice_list["sizes"] = notice_description.split("Size:")[1]
                    notice_list["publice_date"] = tr.xpath('./td[4]/text()').extract_first()
                    notice_list["file_film_number"] = "|".join(
                        [i.strip() for i in tr.xpath('./td[5]//text()').extract()])
                    notice_list["company_cik"] = com_search["cik"]

                    #返回notice的数据
                    yield notice_list
                    # 请求公告详情页
                    yield scrapy.Request(url=notice_list["notice_url"], callback=self.parse_detail,
                                         meta={'notice_list': notice_list, 'com_search': com_search})

            # ==============================================================================================
            # 请求公告页下一页url
            next = response.xpath('//input[@value="Next 100"]').extract()
            # 能找到next标签，请求下一页，否则结束该url
            if len(next):
                # 获取下一页的url start=ka-1+40
                ka = re.findall(r'Items (.+) -',
                                response.xpath('//td[contains(text(),"Items")]/text()').extract_first())
                next_notice_url = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={}&start={}&count=100".format(
                    com_search["cik"], int(ka[0]) - 1 + 100)
                yield scrapy.Request(url=next_notice_url, callback=self.parse)

        else:
            print("该cik没有对应的公司")

    # 解析详情页
    def parse_detail(self, response):
        notice_list = response.meta['notice_list']
        com_search = response.meta['com_search']
        notice_detail = NoticeDetailItem()
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
            if (notice_detail["detail_description"] or notice_detail["doc_name"] or notice_detail["doc_tpye"] or
                    notice_detail["doc_size"]):
                yield notice_detail

