#!/usr/bin/env python
# coding: utf-8

# In[1]:


import findspark
findspark.init()
findspark.find()

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession,SQLContext,functions as F
from datetime import datetime
from matplotlib import pyplot as plt
import pandas as pd

conf =SparkConf().setAppName('SalesAnalysis').setMaster('local')
sc =SparkContext(conf=conf)
spark = SparkSession(sc)
sqlcontext=SQLContext(sc)


# ## Database connection using jdbc and pyspark

# In[2]:


hostname="localhost"
dbname="python_utility"
jdbcPort="3306"
username="root"
password="qwerty@7890"
jdbcURL=f"jdbc:mysql://{hostname}:{jdbcPort}/{dbname}?user={username}&password={password}"
print(jdbcURL)


sales_df = sqlcontext.read.format('jdbc').option("url",jdbcURL).option("dbtable","sales").option("driver","com.mysql.jdbc.Driver").load()
sales_df.show(n=5)


# ## Verifying type

# In[3]:


type(sales_df)


# ## Schema check

# In[4]:


sales_df.printSchema()


# ## Total countries

# In[5]:


sales_df.select("Country").distinct().count() #distinct countries


# ## Total continents

# In[6]:


sales_df.select('Region').distinct().count() #7 continents


# ## Create new column calculating total days to deliver any product

# In[7]:


days = F.datediff(sales_df['ShipDate'],sales_df['OrderDate']) #days to deliver 


# In[8]:


sales_df=sales_df.withColumn('DaysToDeliver',days) # add new col DaysToDeliver


# In[9]:


sales_df.printSchema()


# In[10]:


sales_df.select('DaysToDeliver').show(5)


# ## Continents and its respective countries

# In[11]:


continentsAndCountries=sales_df.groupBy('Region').agg(F.collect_set('Country'))#continents and its contries 
continentsAndCountries.show()


# In[12]:


ccp = continentsAndCountries.toPandas()
ccpDict = ccp.set_index('Region').to_dict()  #convert above df to dict 'Region' as Key and list as value
ccpDict=ccpDict['collect_set(Country)']
ccpDict


# ## Slowest delivery of products

# In[13]:


countriesAvgDays=sales_df.groupBy('Country').avg('DaysToDeliver').sort(F.col("avg(DaysToDeliver)").desc())

#5 slowest deliviery
countriesAvgDays.show(5)


# ## Mapping countries with its average delivery days

# In[14]:


cadp = countriesAvgDays.toPandas()
cadDict = cadp.set_index('Country').to_dict()  #convert above df to dict 'Country' as Key and avg as value
cadDict=cadDict['avg(DaysToDeliver)']
cadDict


# ## Continent wise countries and its average delivery days

# In[15]:


#plotting graph based on continents
#Looking at it we can understand which country has slowest and fastest delivery

for continent in ccpDict.keys():
    x=ccpDict[continent]
    y=[]
    for country in x:
        y.append(cadDict[country])
    
    plt.bar(x,y)
    plt.title(continent)
    plt.xticks(rotation=90)
    plt.xlabel('COUNTRIES')
    plt.ylabel('AVG DELIVERY DAYS')
    plt.figure()
    


# In[16]:


sales_df.groupBy('SalesChannel').count().show()


# ## Year wise total sales of specific product type 

# In[17]:


salesOnProducst=sales_df.sort('OrderDate')
salesOnProducst=salesOnProducst.groupBy('ItemType').agg(F.collect_list(F.date_format('OrderDate','yyyy')),F.collect_list( 'UnitsSold'))
salesOnProducst.show()


# In[18]:


sopp=salesOnProducst.toPandas()
sopDict = sopp.set_index('ItemType').to_dict()

# sopDict['collect_list(UnitsSold)']['Baby Food']

newDict=dict()

from collections import defaultdict
for x in range(len(sopp)):
    product=sopp['ItemType'][x]
    newDict[product] =defaultdict(lambda :0)
    
    for (index,value) in enumerate(sopDict['collect_list(date_format(OrderDate, yyyy))'][sopp['ItemType'][x]]):
        newDict[product][value]+=sopDict['collect_list(UnitsSold)'][product][index]

# print(newDict)

#sales of units of products in each year
for product in newDict.keys():
    print(product)
    x=list(newDict[product].keys())
    y=list(newDict[product].values())
    print(x)
    print(y)
    print('\n')
    


# ## Graph showing  product type and its year wise sales

# In[20]:


for product in newDict.keys():
    x=list(newDict[product].keys())
    y=list(newDict[product].values())
    plt.plot(x,y,label=product)


plt.xlabel('YEARS')
plt.ylabel('UNITS SOLD')
plt.title('PRODUCT COMPARISON')
plt.legend()
plt.figure(figsize=(10,10))
plt.show()


# In[ ]:




