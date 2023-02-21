import scrapy
import json


import pandas as pd

#automatic update
df = pd.read_excel('products/input_file1.xlsx')
df_ean = list(df['Product EAN'])
df_name = list(df['Product name'])
df_sku = list(df['sku'])


dff = pd.read_excel('sites_archive/input_file2_zwembad.xlsx')
dff_ean = list(dff['ean'])
dff_sku = list(dff['sku'])
dff_name = list(dff['name'])

global new_eann
global new_sku
global new_name
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

mydb_cursor.execute('CREATE DATABASE IF NOT EXISTS product_search1');

mydb = mc.connect(host=utils.host,user=utils.user,password=utils.password,database='product_search1')


mydb_cursor = mydb.cursor()

mydb_cursor.execute('CREATE TABLE IF NOT EXISTS pd_checkerz (id INT AUTO_INCREMENT PRIMARY KEY,Company varchar(255),Barcode varchar(255),Product_name TEXT,Sku varchar(255),Price TEXT,Brand TEXT, UNIQUE (Company,Barcode,Sku))')



#end connection


class ZwembadSpider(scrapy.Spider):
    name = 'zwembad'
    # allowed_domains = ['a.com']
    # start_urls = ['http://a.com/']


    
    
    def start_requests(self):
        data0 = pd.DataFrame({'sku':dff_sku,'ean':dff_ean,'name':dff_name})

        data0 =data0.append(data,ignore_index=True)

        data0.to_excel('sites_archive/input_file2_zwembad.xlsx',index=False)

        urlx = 'https://www.zwembadstore.com/'
        yield scrapy.Request(url=urlx)

    def parse(self, response):
        link = response.xpath("//li[@class='item']/a/@href").getall()
        for i in link:
            if i:
            
                abs_i = f'{i}page1.html?format=json&limit=100'
                yield scrapy.Request(url = abs_i,callback=self.next,meta={'base':i})
    def next(self,response):
        html = json.loads(response.body)
        pdx = html.get('collection')
        if pdx:
            pdi = pdx.get('products')
            if pdi:
                k = list(pdi.values())
                for i in k:
                    ean = i.get('ean')
                    bran = i.get('brand')
                    if bran:
                        brand = bran.get('title')
                    else:
                        brand = ''
                    u = i.get('url')
                    sku = i.get('sku')
                    pd_name = i.get('title')
                    price = i.get('price').get('price')
                    site= 'https://www.zwembadstore.com/ - Zwembadstore'
                    urlx = f'https://www.zwembadstore.com/{u}'
                    if ean in new_eann or sku in new_sku:
                        #insert into mysql database
                        sitex = site.replace('https://www.','')
                        sql = "INSERT IGNORE INTO pd_checkerz(Company,Barcode,Product_name,Sku,Price,Brand) VALUES (%s,%s,%s,%s,%s,%s)"
                        val = (sitex,ean,pd_name,sku,price,brand)
                        mydb_cursor.execute(sql,val)

                        mydb.commit()

                        #end mysql




                

                        yield {
                            'Ean':ean,
                            'Product_name':pd_name,
                            'sku':sku,
                            'price':price,
                            'url':f'https://www.zwembadstore.com/{u}',
                            'site':'https://www.zwembadstore.com/'
                        }

                next = html.get('collection').get('page_next')

                if next != False :
                    base = response.meta['base']
                    urln = f'{base}page{next}.html?format=json&limit=100'
                    yield scrapy.Request(url=urln,callback=self.next,meta={'base':base})
                else:
                    pass

    