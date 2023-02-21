import scrapy
import json
from scrapy.spiders import XMLFeedSpider


import pandas as pd

#automatic update
df = pd.read_excel('products/input_file1.xlsx')
df_ean = list(df['Product EAN'])
df_name = list(df['Product name'])
df_sku = list(df['sku'])


dff = pd.read_excel('sites_archive/input_file2_bol.xlsx')
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
            if len(b2) <= 3 or b2.isalpha():
                b2 = b1[1]
                if len(b2) <= 3 or b2.isalpha():
                    b2 = b

                
        elif '/' in b:
            b1 = b.split('/')
            b2 = b1[0]
            if len(b2) <= 3 or b2.isalpha():
                b2 = b1[1]
                if len(b2) <= 3 or b2.isalpha():
                    b2 = b
        else:
            b2 = b

        new_sku.append(b2)
        new_eann.append(a)
        new_name.append(co)
        # print(f'new_sku:{b2}')
        # print(f'new_ean:{a}')
        # print(f'new_name:{co}')


new_skiv = [a for a in new_sku if a != None ]
new_eanv = [b for b in new_eann if b != None ]

data = pd.DataFrame({'sku':new_sku,'ean':new_eann,'name':new_name})

#end automatic update

import mysql.connector as mc
from amasty import utils


#end connection
class BolSpider(scrapy.Spider):
    name = 'bol'
    # allowed_domains = ['a.com']
    # start_urls = ['http://a.com/']
    
    def __init__(self):
        self.cur = None
        self.mydb = None
    def start_requests(self):
        #connect mysql

        mydb = mc.connect(host=utils.host,user=utils.user,password=utils.password)

        mydb_cursor = mydb.cursor()

        mydb_cursor.execute('CREATE DATABASE IF NOT EXISTS product_search1')

        mydb = mc.connect(host=utils.host,user=utils.user,password=utils.password,database='product_search1')


        mydb_cursor = mydb.cursor()

        mydb_cursor.execute('CREATE TABLE IF NOT EXISTS pd_checkerz (id INT AUTO_INCREMENT PRIMARY KEY,Company varchar(255),Barcode_input varchar(255),Product_name TEXT,Sku_input varchar(255),Price TEXT,Brand TEXT,Sku_match TEXT,Barcode_match TEXT, UNIQUE (Company,Barcode_input,Sku_input))')

        self.mydb = mydb
        self.cur = mydb_cursor

        #end connect

        data0 = pd.DataFrame({'sku':dff_sku,'ean':dff_ean,'name':dff_name})

        data0 =data0.append(data,ignore_index=True)

        #data0.to_excel('sites_archive/input_file2_bol.xlsx',index=False)
        
        for i in new_skiv:
            if i:
            
                yield scrapy.Request(url=f'https://www.bol.com/nl/nl/s/?searchtext={i}')
    def parse(self, response):
            
        link = response.xpath("//ul[@data-test='products']/li/descendant::wsp-analytics-tracking-event[1]/a/@href").getall()
        for i in link:
            abs_i = f'https://www.bol.com{i}'
            yield scrapy.Request(url=abs_i,callback=self.next)

        next = response.xpath("//li[contains(@class,'next')]/a/@href").get()
        if next:
            ab_next = f'https://www.bol.com{next}'
            yield scrapy.Request(url=ab_next,callback=self.parse)
    def next(self,response):
        
        
        ean = response.xpath("normalize-space(//dt[contains(text(),'EAN')]/following-sibling::dd/text())").get()
        sku = response.xpath("normalize-space(//dt[contains(text(),'MPN')]/following-sibling::dd/text())").get()
        brand = response.xpath("normalize-space(//dt[contains(text(),'Merk')]/following-sibling::dd/a/text())").get()
        
        global check
        check = None
        
        if ean:
            eanu = str(ean)
        else:
            eanu = 'Empty'

        if '|' in eanu:
            all_ean = eanu.split('|')
            
            check =  any(item in df_ean for item in all_ean)
 
        else:
            all_ean = eanu
        
        
        if sku:
            skuv = str(sku)
            if len(skuv) < 4:
                skuv = 'Invalid'

        else:
            skuv = 'Empty'

        if '-' in skuv:
            s = skuv.split('-')
            s1 = s[0]
            if len(s1) <= 4 or s1.isalpha():
                s1 = s[1]
                if len(s1) <= 4 or s1.isalpha():
                    s1 = skuv

        elif '/' in skuv:
            s = skuv.split('/')
            s1 = s[-1]
            if len(s1) <= 4 or s1.isalpha():
                s1 = s[0]
                if len(s1) <= 4 or s1.isalpha():
                    s1 = skuv
        
        else:
            word = ''
            if skuv.isalnum() or skuv.isalpha():
                word  = skuv
            else:

                for i in skuv:
                    if i.isdigit():
                        word = word + i

            if word == '':
                s1 = 'No value'
            else:
                if len(word) <= 4:
                    s1 = skuv
                else:
                    s1 = word


        aa = [a for a in new_skiv if s1 in a]
        bb = [b for b in new_eanv if all_ean in b]

            
        
        
        if len(aa) > 0 or len(bb) > 0 or check is True:

            aaa = ','.join(aa)
            bbb = ','.join(bb)

            pd_name = response.xpath("(//span[@data-test='title'])[1]/descendant::text()").get()
            price = response.xpath("(//span[@data-test='price'])[1]/descendant::text()").getall()
            pricex = price[0:2]
            new_price = []
            for a in pricex:
                new_i = a.strip()
                new_price.append(new_i)
            prc = '.'.join(new_price)
            prcx = prc.strip()
            
            urlc = response.url
            site = 'Bol'

            #INSERT IGNORE into mysql database
            sitex = site.replace('https://www.','')
                        
            sql = "INSERT IGNORE INTO pd_checkerz(Company,Barcode_input,Product_name,Sku_input,Price,Brand,Sku_match,Barcode_match) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"
            val = (site,all_ean,pd_name,skuv,prcx,brand,aaa,bbb)
            self.cur.execute(sql,val)

            self.mydb.commit()


            
                

            yield {
                'Ean':all_ean,
                'Product_name':pd_name,
                'sku':sku,
                'price':prcx,
                'url':urlc,
                'site':site
            }