import scrapy
import scrapy
import mysql.connector as mc

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



class CampzSpider(XMLFeedSpider):
    name = 'campz'
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

        

    start_urls = ['https://www.campz.nl/sitemap_0-category.xml','https://www.campz.nl/sitemap_1-content.xml','https://www.campz.nl/sitemap_2.xml',
    'https://www.campz.nl/sitemap-0-product.xml','https://www.campz.nl/sitemap-2-product.xml','https://www.campz.nl/sitemap-3-product.xml',
    'https://www.campz.nl/sitemap-1-product.xml']
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
            
            
            yield scrapy.Request(url=furl,callback=self.next,dont_filter=False)
    

    def next(self,response):
        check = response.xpath("//div[@class='product-name-ctr']").get()
        if check:
        
            pd_name = response.xpath("//h1[@itemprop='name']/text()").get()
            prc =response.xpath("//span[@itemprop='price']/@content").get()
            urlc = response.url
            site = 'https://www.campz.nl/sitemap_index.xml'
            ean = response.xpath("//div[contains(text(),'EAN')]/following-sibling::div/text()").get()
            sku = response.xpath("//div[contains(text(),'Artikelnummer')]/following-sibling::div/text()").get()
            Brand = 'None'
            com = 'Campz'


            print(f'ean:{ean}')
            print(f'sku:{sku}')
            print(f'ean:{ean},{sku}')


            if ean:
                eanv = str(ean)
            else:
                eanv = 'Empty!'
            aa = [a for a in new_eanv if eanv in a]
            
            if sku:

                skuv = str(sku)
                if len(skuv) < 4:
                    skuv = 'Invalid'

            else:
                skuv = 'Empty!'


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

            

            
            bb = [b for b in new_skiv if s1 in b]

            
            if len(aa) > 0 or len(bb) > 0:
                aaa = ','.join(aa)
                bbb = ','.join(bb)
                #INSERT IGNORE into mysql database
                sitex = site.replace('https://www.','')
                            
                sql = "INSERT IGNORE INTO pd_checkerz(Company,Barcode_input,Product_name,Sku_input,Price,Brand,Sku_match,Barcode_match) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"
                val = (com,eanv,pd_name,skuv,prc,Brand,bbb,aaa)
                self.cur.execute(sql,val)

                self.mydb.commit()

                #end mysql

            
                yield {
                        'Ean':ean,
                        'Product_name':pd_name,
                        'sku':sku,
                        'price':prc,
                        'url':urlc,
                        'site':site
                    }


    
