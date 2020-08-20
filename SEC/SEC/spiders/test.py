import requests
from lxml import etree
import re
url="https://www.sec.gov/cgi-bin/browse-edgar?company=docusign&match=&filenum=&State=&Country=&SIC=&myowner=exclude&action=getcompany"
response=requests.get(url=url)

res=requests.get(url=url).content.decode("utf-8")

data=etree.HTML(res)

#提取SIC的a标签和br标签之间的文本
result=data.xpath('//*[text()="SIC"]/following-sibling::text()[preceding-sibling::node()[1][local-name()="a"]][following-sibling::node()[local-name()="br"]][1]')
print(result)