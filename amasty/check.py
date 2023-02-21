import pandas as pd


# df = pd.read_excel('input_file1.xlsx')
# df_ean = list(df['Product EAN'])
# df_name = list(df['Product name'])
# df_sku = list(df['sku'])


# dff = pd.read_excel('sites_archive/input_file2_zwembad.xlsx')
# dff_ean = list(dff['ean'])
# dff_sku = list(dff['sku'])
# dff_name = list(dff['name'])

# new_sku = []
# new_eann = []
# new_name = []
# for a,b,c in zip(df_ean,df_sku,df_name):
#     if a not in dff_ean or b not in dff_sku or c not in dff_name:
#         new_sku.append(b)
#         new_eann.append(a)
#         new_name.append(c)
#         print(f'new_sku:{b}')
#         print(f'new_ean:{a}')
#         print(f'new_name:{c}')


# data = pd.DataFrame({'sku':new_sku,'ean':new_eann,'name':new_name})
# data0 = pd.DataFrame({'sku':dff_sku,'ean':dff_ean,'name':dff_name})

# data0 =data0.append(data,ignore_index=True)

# data0.to_excel('sites_archive/input_file2_zwembad.xlsx',index=False)
# print(len(new_sku))

import mysql.connector as mc
df = pd.read_excel('bol1.xlsx')
Ean	= list(df['Ean'])
Product_name = list(df['Product_name'])
sku	= list(df['sku'])
price	= list(df['price'])
url	= list(df['url'])
site = list(df['site'])


mydb = mc.connect(host="localhost",user="root",password="8991",database="products")

mydb_cursor = mydb.cursor()

sql = "INSERT INTO pd_checker(Company,Ean,product_name,sku,price,url) VALUES (%s,%s,%s,%s,%s,%s)"
for a,b,c,d,e,f in zip(Ean,Product_name,sku,price,url,site):
    val = (f,a,b,c,d,e)
    mydb_cursor.execute(sql,val)

    mydb.commit()
print(mydb)