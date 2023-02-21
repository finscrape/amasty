import scrapy
import json
from amasty import utils

import pandas as pd

#automatic update
df = pd.read_excel('products/input_file1.xlsx')
df_ean = list(df['Product EAN'])
df_name = list(df['Product name'])
df_sku = list(df['sku'])


dff = pd.read_excel('sites_archive/input_file2_topzwembad.xlsx')
dff_ean = list(dff['ean'])
dff_sku = list(dff['sku'])
dff_name = list(dff['name'])

new_sku = []
new_eann = []
new_name = []
for a,b,c in zip(df_ean,df_sku,df_name):
    if a not in dff_ean or b not in dff_sku or c not in dff_name:
        new_sku.append(b)
        new_eann.append(a)
        new_name.append(c)
        # print(f'new_sku:{b}')
        # print(f'new_ean:{a}')
        # print(f'new_name:{c}')


data = pd.DataFrame({'sku':new_sku,'ean':new_eann,'name':new_name})
#end automatic update

#mysql connection
import mysql.connector as mc

mydb = mc.connect(host=utils.host,user=utils.user,password=utils.password)

mydb_cursor = mydb.cursor()

mydb_cursor.execute("CREATE DATABASE IF NOT EXISTS product_search")

mydb = mc.connect(host=utils.host,user=utils.user,password=utils.password,database='product_search')


mydb_cursor = mydb.cursor()

mydb_cursor.execute('CREATE TABLE IF NOT EXISTS pd_checkers (id INT AUTO_INCREMENT PRIMARY KEY,Company varchar(255),Barcode varchar(255),Product_name TEXT,Sku varchar(255),Price TEXT,Brand TEXT, UNIQUE (Company,Barcode,Sku))')

#end connection

class TopZwembadSpider(scrapy.Spider):
    name = 'top_zwembad'
    # allowed_domains = ['a.com']
    # start_urls = ['http://a.com/']
    def start_requests(self):
        data0 = pd.DataFrame({'sku':dff_sku,'ean':dff_ean,'name':dff_name})

        data0 =data0.append(data,ignore_index=True)

        data0.to_excel('sites_archive/input_file2_topzwembad.xlsx',index=False)

        for i in new_name:
            x = i.split()
            xx = '+'.join(x)
            yield scrapy.Request(url=f'https://www.top-zwembadshop.nl/catalogsearch/result/index/?product_list_limit=36&q={xx}')
    def parse(self, response):
        links = response.xpath("//li[@class='item product product-item']/div/a/@href").getall()
        if links:
            for i in links:
                yield scrapy.Request(url=i,callback=self.next)

            next = response.xpath("(//a[@title='Volgende'])[1]/@href").get()
            if next:
                yield scrapy.Request(url=next,callback=self.parse)

    def next(self,response):
        
        pd_name = response.xpath("//h1[@class='page-title']/span/text()").get()
        prc =response.xpath("(//span[@class='price'])[1]/text()").get()
        urlc = response.url
        site = 'https://www.top-zwembadshop'
        ean = response.xpath("//th[text()='EAN-code']/following-sibling::td/text()").get()
        sku = response.xpath("//th[text()='Leverancierscode']/following-sibling::td/text()").get()
        Brand = response.xpath("//th[text()='Merk']/following-sibling::td/text()").get()
        if ean in new_eann or sku in new_sku:
            #INSERT IGNORE into mysql database
            sitex = site.replace('https://www.','')
                        
            sql = "INSERT IGNORE INTO pd_checkers(Company,Barcode,Product_name,Sku,Price,Brand) VALUES (%s,%s,%s,%s,%s,%s)"
            val = (sitex,ean,pd_name,sku,prc,Brand)
            mydb_cursor.execute(sql,val)

            mydb.commit()

            #end mysql

        
            yield {
                    'Ean':ean,
                    'Product_name':pd_name,
                    'sku':sku,
                    'price':prc,
                    'url':urlc,
                    'site':site
                }

