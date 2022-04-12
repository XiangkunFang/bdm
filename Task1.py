import re
from pyspark.sql.functions import col, split
#key_food_products spark dataframe
df1 = spark.read.load ('keyfood_products.csv', format ='csv', header = True, inferSchema = False)
df1 = df1.na.drop()
df_kp = df1.select ("store","department",F.split (df1.upc, "-").getItem(1).alias('upc_code'),F.regexp_extract(df1.price, "\d+\.\d+",0).alias('price'))
#keyfood_sample_items spark dataframe
df2 = spark.read.load ('keyfood_sample_items.csv', format = 'csv', header = True, inferSchema = False)
df2 = df2.na.drop()
df_ksi = df2.select(F.split(df2['UPC code'],"-").getItem(1).alias('upc_code'),df2['Item Name'].alias('itemName'))
#Merge
df_kp_ksi = df_ksi. join (df_kp, ['upc_code'], how = 'left')
#read json file
with open ('keyfood_nyc_stores.json') as js:
  stores = json.load (js)
  js.close()
#Creat store dataframe
store_list = []
for store in stores:
  store_list.append([stores[store]['name'], stores[store]['foodInsecurity'],stores[store]['communityDistrict']]) 
df_store = pd.DataFrame(store_list,columns=['store_name','foodInsecurity','communityDistrict'])
df_store_spark = spark.createDataFrame (df_store)
#Merge
Task1 = df_kp_ksi.join(df_store_spark,df_kp_ksi.store ==df_store_spark.store_name, how='left')
#output task
outputTask1 = Task1.select(Task1['itemName'].alias('Item Name'),
                Task1['price'].alias('Price ($)').cast('float'),
                (Task1['foodInsecurity']*100).alias ('Food Insecurity').cast('int'))
# DO NOT EDIT BELOW
outputTask1 = outputTask1.cache()
outputTask1.count()
