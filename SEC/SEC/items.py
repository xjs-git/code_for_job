# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class CompanySearchItem(scrapy.Item):
    #公告搜索页数据
    cik=scrapy.Field() #cik
    cik_url=scrapy.Field() #cik链接
    company_name=scrapy.Field() #公司名称
    state=scrapy.Field() #位置
    sic = scrapy.Field()
    business_address = scrapy.Field()
    mailing_address = scrapy.Field()
    formerly = scrapy.Field()
    add_time = scrapy.Field()
    update_time = scrapy.Field()

class NoticeListItem(scrapy.Item):
    #公告列表页数据
    filings=scrapy.Field()
    notice_format=scrapy.Field()
    notice_url=scrapy.Field() #公告的url
    notice_description=scrapy.Field() #公告的描述
    publice_date=scrapy.Field() #公告发布日期
    file_film_number=scrapy.Field()
    company_cik=scrapy.Field() #关联公司搜索表
    description=scrapy.Field()
    acc_no=scrapy.Field()
    sizes=scrapy.Field()


class NoticeDetailItem(scrapy.Item):
    #公告详情页的数据
    detail_description=scrapy.Field() #公告详情描述
    doc_name=scrapy.Field() #公告详情名称
    doc_url=scrapy.Field() #公告文件url
    doc_size=scrapy.Field() #公告文件大小
    doc_tpye=scrapy.Field() #公告详情类型
    filing_date=scrapy.Field() #公告归档时间
    accepted=scrapy.Field() #公告接受时间
    period_of_report=scrapy.Field() #公告定期报告时间
    effectiveness_date=scrapy.Field() #公告效力
    doc_files=scrapy.Field() #公告属性
    items=scrapy.Field() #项目
    from_notice=scrapy.Field()

    from_cik=scrapy.Field()