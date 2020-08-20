# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

import json
import pymysql
from SEC.items import CompanySearchItem
from SEC.items import NoticeListItem
from SEC.items import NoticeDetailItem
import copy


class SecPipeline:
    def __init__(self):
        # 连接数据库
        self.conn = pymysql.connect(host="10.1.18.72", port=3306,
                                    user="root", passwd="123456", database='sec_spider', charset="utf8", )
        self.cursor = self.conn.cursor()

    def open_spider(self, spider):
        print("爬虫启动！")

    def reConnect(self):
        try:
            self.conn.ping()
        except:
            self.conn()
            self.cursor = self.conn.cursor()

    def process_item(self, item, spider):
        # mysql断开连接则重连
        self.reConnect()
        copy_item=copy.deepcopy(item)
        # company表中添加数据
        if isinstance(item,CompanySearchItem):
            self.cursor.execute(
                'REPLACE INTO company(cik,cik_url,company_name,state,sic,business_address,mailing_address,formerly,add_time,update_time) VALUES ("{}","{}","{}","{}","{}","{}","{}","{}","{}","{}")'.format(copy_item['cik'],copy_item['cik_url'],copy_item['company_name'],copy_item['state'],copy_item['sic'],copy_item['business_address'],copy_item['mailing_address'],copy_item['formerly'],item["add_time"],item["update_time"]))
        # notice表中添加数据
        elif isinstance(item,NoticeListItem):
            self.cursor.execute(
                'REPLACE INTO notice(notice_description,notice_url,publish_date,company_cik,filings,notice_format,file_film_number,description,acc_no,sizes) VALUES ("{}","{}","{}","{}","{}","{}","{}","{}","{}","{}")'.format(copy_item['notice_description'],copy_item['notice_url'],copy_item['publice_date'],copy_item['company_cik'],copy_item['filings'],copy_item['notice_format'],copy_item['file_film_number'],copy_item['description'],item['acc_no'],item['sizes']))
        # notice_detail表中添加数据
        elif isinstance(item, NoticeDetailItem):
            self.cursor.execute(
                'REPLACE INTO notice_detail(detail_description,doc_url,doc_size,doc_name,doc_tpye,filing_date,accepted,period_of_report,effectiveness_date,doc_files,items,from_notice,from_cik) VALUES ("{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}")'.format(copy_item['detail_description'], copy_item['doc_url'], copy_item['doc_size'],copy_item['doc_name'],copy_item['doc_tpye'],copy_item['filing_date'],copy_item['accepted'],copy_item['period_of_report'],copy_item['effectiveness_date'],copy_item['doc_files'],copy_item['items'],copy_item['from_notice'],copy_item["from_cik"]))
        self.conn.commit()
        return item

    def close_spider(self, spider):
        self.cursor.close()
        self.conn.close()
        print("爬虫终止！")
