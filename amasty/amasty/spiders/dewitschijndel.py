import scrapy
import json

import scrapy
import json
import scrapy
import json
from scrapy.spiders import XMLFeedSpider


import pandas as pd


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
new_names = [c for c in new_name if c != None ]

data = pd.DataFrame({'sku':new_sku,'ean':new_eann,'name':new_name})
#end automatic update

#mysql connection
import mysql.connector as mc
from amasty import utils


class DewitschijndelSpider(scrapy.Spider):
    name = 'dewitschijndel'
    # allowed_domains = ['a.com']
    def __init__(self):
        #mysql connection
        self.all = []
        
        mydb = mc.connect(host=utils.host,user=utils.user,password=utils.password)

        mydb_cursor = mydb.cursor()

        mydb_cursor.execute("CREATE DATABASE IF NOT EXISTS product_search1")

        mydb = mc.connect(host=utils.host,user=utils.user,password=utils.password,database='product_search1')


        mydb_cursor = mydb.cursor()

        mydb_cursor.execute('CREATE TABLE IF NOT EXISTS pd_checkerz (id INT AUTO_INCREMENT PRIMARY KEY,Company varchar(255),Barcode varchar(255),Product_name TEXT,Sku varchar(255),Price TEXT,Brand TEXT,Sku_match TEXT,Barcode_match TEXT, UNIQUE (Company,Barcode_input,Sku_input))')

        self.mydb = mydb
        self.cur = mydb_cursor

        #end connection




    def start_requests(self):
        data0 = pd.DataFrame({'sku':dff_sku,'ean':dff_ean,'name':dff_name})

        data0 =data0.append(data,ignore_index=True)

        #data0.to_excel('sites_archive/input_file2_bol.xlsx',index=False)
        tot  = new_names



        
        for i in tot:

            if i or len(str(i)) > 1:
                ii = i.split()
                ix = "-".join(ii)
                yield scrapy.Request(url=f'https://www.dewitschijndel.nl/catalogsearch/result/?q={ix}')
    
    def parse(self,response):
        l  = response.xpath("//div[contains(@class,'products wrapper')]/descendant::*/a/@href").getall()
        lv = response.url
        if len(l) > 0:
            for i in l:
                
                yield scrapy.Request(url=i,callback=self.next,meta={'lv':lv},dont_filter=True)

        next = response.xpath("(//span[text()='Volgende'])[1]/ancestor::a/@href").get()
        if next:
            yield scrapy.Request(url=next,callback=self.parse,dont_filter=True)


    
    def next(self,response):
        lv = response.meta['lv']
        ea = response.xpath("//td[@data-th='Artikelnummer fabrikant']/descendant::text()").get()
        pl = response.xpath("//button[@form='product_addtocart_form']/@data-id").get()
        brand = response.xpath("//button[@form='product_addtocart_form']/@data-brand").get()
        print(f'ean-{ea}-sku-{pl}-source{lv}')
        #ean = ''
        if ea:
            ean = str(ea)
        else:
            ean = 'Empty!'
        aa = [a for a in new_eanv if ean in a]
        cc = [a for a in new_skiv if ean in a]

        if pl:
            plu = str(pl)
            if len(plu) < 4:
                plu = 'Invalid'

        else:
            plu = 'Empty!'

        if '-' in plu:
            s = plu.split('-')
            s1 = s[0]
            if len(s1) <= 4 or s1.isalpha():
                s1 = s[1]
                if len(s1) <= 4 or s1.isalpha():
                    s1 = plu
        elif '/' in plu:
            s = plu.split('/')
            s1 = s[-1]
            if len(s1) <= 4 or s1.isalpha():
                s1 = s[0]
                if len(s1) <= 4 or s1.isalpha():
                    s1 = plu
        
        else:
            word = ''
            if plu.isalnum() or plu.isalpha():
                word  = plu
            else:
                for i in plu:
                    if i.isdigit():
                        word = word + i

            if word == '':
                s1 = 'No value'
            else:
                if len(word) <= 4:
                    s1 = plu
                else:
                    s1 = word


        print(f'ean_edit-{ean}-sku_edit-{s1}')
        
        bb = [b for b in new_skiv if s1 in b]

        print(f'sku_match-{bb}')
        print(f'sku_match-{bb}')


        if len(aa) > 0 or len(bb) > 0 or len(cc) > 0:
            aaa = ','.join(aa)
            bbb = ','.join(bb)
            ccc = ','.join(cc)
            if bb is None:
                bbb = ccc

            urlc = response.url
            site = 'https://www.dewitschijndel.nl/ - Dewitschijndel'
            pd_name = response.xpath("//meta[@property='og:title']/@content").getall()
            pd_namex = pd_name
            price = response.xpath("//meta[@property='product:price:amount']/@content").get()
            com = 'Dewitschijndel store'
            #INSERT IGNORE into mysql database
            sitex = site.replace('https://www.','')
                        
            sql = "INSERT IGNORE INTO pd_checkerz(Company,Barcode_input,Product_name,Sku_input,Price,Brand,Sku_match,Barcode_match) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"
            val = (com,ean,pd_namex,plu,price,brand,bbb,aaa)
            self.cur.execute(sql,val)

            self.mydb.commit()

            #end mysql


            yield {
                'Ean':ean,
                'Product_name':pd_namex,
                'sku':plu,
                'price':price,
                'url':urlc,
                'site':site,
                'si':bbb,
                'bi':aaa
            }


