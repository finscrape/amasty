import scrapy
import json


import pandas as pd

#automatic update
df = pd.read_excel('products/input_file1.xlsx')
df_ean = list(df['Product EAN'])
df_name = list(df['Product name'])
df_sku = list(df['sku'])


dff = pd.read_excel('sites_archive/input_file2_dewit.xlsx')
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
from amasty import utils

mydb = mc.connect(host=utils.host,user=utils.user,password=utils.password)

mydb_cursor = mydb.cursor()

mydb_cursor.execute('CREATE DATABASE IF NOT EXISTS product_search1')

mydb = mc.connect(host=utils.host,user=utils.user,password=utils.password,database='product_search1')


mydb_cursor = mydb.cursor()


mydb_cursor.execute('CREATE TABLE IF NOT EXISTS pd_checkerz (id INT AUTO_INCREMENT PRIMARY KEY,Company varchar(255),Barcode_input varchar(255),Product_name TEXT,Sku_input varchar(255),Price TEXT,Brand TEXT,Sku_match TEXT,Barcode_match TEXT, UNIQUE (Company,Barcode_input,Sku_input))')

#end connection

class DewitSpider(scrapy.Spider):
    name = 'dewit'
    # allowed_domains = ['a.com']
    # start_urls = ['http://a.com/']
    def start_requests(self):
        data0 = pd.DataFrame({'sku':dff_sku,'ean':dff_ean,'name':dff_name})

        data0 =data0.append(data,ignore_index=True)

        #data0.to_excel('sites_archive/input_file2_dewit.xlsx',index=False)

        for i in new_name:
            ii = i.split()
            ix = "-".join(ii)
            yield scrapy.Request(url=f'https://www.dewitschijndel.nl/catalogsearch/result/?q={ix}')
    def parse(self, response):
        links = response.xpath("//a[@class='product-item__title-link']/@href").getall()
        for i in links:
            yield scrapy.Request(url=i,callback=self.next)

        next = response.xpath("(//span[text()='Volgende'])[1]/ancestor::span/@data-href").get()
        if next:
            yield scrapy.Request(url=next,callback=self.parse)

    def next(self,response):
        sku = response.xpath("//td[@data-th='Artikelnummer fabrikant']/span/descendant::text()").get()
        plu = response.xpath("//td[@data-th='PLU']/span/descendant::text()").get()
        brand = response.xpath("//td[@data-th='Merk']/span/descendant::text()").get()
        ean = ''
        if sku in new_sku or plu in new_sku:
            urlc = response.url
            site = 'https://www.dewitschijndel.nl/ - Dewitschijndel'
            pd_name = response.xpath("//h1[@class='page-title page-title--']/descendant::text()").getall()
            pd_namex = " ".join(pd_name)
            price = response.xpath("//span[@class='price']/descendant::text()").get()

            #INSERT IGNORE into mysql database
            sitex = site.replace('https://www.','')
                        
            sql = "INSERT IGNORE INTO pd_checkers(Company,Barcode,Product_name,Sku,Price,Brand) VALUES (%s,%s,%s,%s,%s,%s)"
            val = (sitex,ean,pd_namex,sku,price,brand)
            mydb_cursor.execute(sql,val)

            mydb.commit()

            #end mysql


            yield {
                'Ean':ean,
                'Product_name':pd_namex,
                'sku':sku,
                'price':price,
                'url':urlc,
                'site':site
            }


