import scrapy

import scrapy
import scrapy

from scrapy.spiders import XMLFeedSpider
import scrapy
import json
from amasty import utils

import pandas as pd
import json
from scrapy.spiders import XMLFeedSpider


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
for ao,bo,co in zip(df_ean,df_sku,df_name):
    if ao not in dff_ean or bo not in dff_sku or co not in dff_name:
        if ao or len(str(ao)) > 1:
            a = str(ao)
        else :
            a= 'Nothing'

        if bo or len(str(bo)) > 1:
            b = str(bo)
        else:
            b= 'Nothing'

        if '-' in b:
            b1 = b.split('-')
            b2 = b1[0]
            if len(b2) <= 3:
                b2 = b1[1]
        else:
            b2 = b

        new_sku.append(b)
        new_eann.append(a)
        new_name.append(co)
        
        # print(f'new_sku:{b2}')
        # print(f'new_ean:{a}')
        # print(f'new_name:{co}')


new_skiv = [a for a in new_sku if a != None ]
new_eanv = [b for b in new_eann if b != None ]

data = pd.DataFrame({'sku':new_sku,'ean':new_eann,'name':new_name})
#end automatic update

#mysql connection
import mysql.connector as mc

class OutdoorSpider(XMLFeedSpider):
    name = 'outdoor'
    #allowed_domains = ['a.com']

    def __init__(self):
        #mysql connection
        
        mydb = mc.connect(host=utils.host,user=utils.user,password=utils.password)

        mydb_cursor = mydb.cursor()

        mydb_cursor.execute("CREATE DATABASE IF NOT EXISTS product_search1")

        mydb = mc.connect(host=utils.host,user=utils.user,password=utils.password,database='product_search1')


        mydb_cursor = mydb.cursor()

        mydb_cursor.execute('CREATE TABLE IF NOT EXISTS pd_checkerz (id INT AUTO_INCREMENT PRIMARY KEY,Company varchar(255),Barcode varchar(255),Product_name TEXT,Sku varchar(255),Price TEXT,Brand TEXT,Sku_match TEXT,Barcode_match TEXT, UNIQUE (Company,Barcode_input,Sku_input))')

        self.mydb = mydb
        self.cur = mydb_cursor

        #end connection


    start_urls = ['https://www.outdoorxl.nl/sitemap.xml']
    #data0 = pd.DataFrame({'sku':dff_sku,'ean':dff_ean,'name':dff_name})
    #data0 =data0.append(data,ignore_index=True)
    #data0.to_excel('sites_archive/input_file2_topzwembad.xlsx',index=False)

    namespaces = [('n', 'http://www.sitemaps.org/schemas/sitemap/0.9')]
    itertag = 'n:loc'
    iterator = 'xml'

    

    def parse_node(self, response, node):

        urls = node.xpath("./text()").extract()
        for i in urls:
        
            furl = f'{i}'
            print(furl)
            
            
    #         yield scrapy.Request(url=furl,callback=self.next,dont_filter=False)
    

    # def next(self,response):
    #     check = response.xpath("//div[@class='product-name-ctr']").get()
    #     if check:
    