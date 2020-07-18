#!/usr/bin/env python
# coding: utf-8

# # Pyspark operations on a json file

# In[1]:


import findspark
findspark.init('C:\\Spark\\spark-3.0.0-bin-hadoop2.7')
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import isnan, when, count, col, translate
spark = SparkSession.builder.appName("Manipulate_JSON").getOrCreate()
sc = spark.sparkContext
# the '4' in the argument. It denotes 4 cores to be used for this SparkContext object
# sc=SparkContext(master="local[4]")

path = "Data/tv-shows.json"
tvshowDF = spark.read.json(path)

# The inferred schema can be visualized using the printSchema() method
tvshowDF.printSchema()


# ## Print the schema

# In[2]:


tvshowDF.show() #to show all data in data frame


# ## Data Cleaning
# 
# ### Find null vaules column wise in dataframe

# In[3]:


tvshowDF.select([count(when(col(c).isNull(), c)).alias(c) for c in tvshowDF.columns]).show()
len(tvshowDF.columns)
#The code above check for the existence of null value for every columns and count its frequency and then display it in a tabulated format as below.


# ## drop columns which are not required

# In[4]:



tvshowDF = tvshowDF.drop('webChannel')
tvshowDF = tvshowDF.drop('officialSite')


# In[5]:


tvshowDF.select([count(when(col(c).isNull(), c)).alias(c) for c in tvshowDF.columns]).show()
len(tvshowDF.columns)


# In[6]:


# drop rows with missing or null values
tvshowDF = tvshowDF.dropna(how='any')


#  ## to get details of currently running or ended shows

# In[7]:


tvshowDF.groupBy("status").count().show()


# ## fetching all shows with average rating more than 9

# In[8]:


high_ratedshowDF=tvshowDF.filter(tvshowDF['rating.average']>9) 


# In[9]:


high_ratedshowDF.show() # printing all shows with rating >9


# In[10]:


high_ratedshowDF.count() # counting high rated shows


# ## listing name of all high rated shows

# In[11]:


high_ratedshowDF.select('name').show() 


# ## performing sql queries directly on files

# In[14]:



querydf = spark.sql("SELECT * FROM json.`Data/tv-shows.json` where rating.average>8.0 ")


# In[15]:


querydf.show()


# In[16]:


querydf.count() 


# In[17]:


tvshowDF.count()


#  ## groupby query based on networks in dataset
# 
#  ### Max
#  ### Min
#  ### Mean

# In[18]:


tvshowDF.groupby('network.name').mean().show()


# In[19]:


tvshowDF.groupby('network.name').max().show()


# In[20]:


tvshowDF.groupby('network.name').min().show()


# In[21]:


# find tv show with max runtime by aggregation function
tvshowDF.agg({'runtime':'max'}).show() 


# ## Get the show with highest runtime

# In[23]:


querydf = spark.sql("SELECT name FROM json.`Data/tv-shows.json` where runtime = 120 limit 1 ")


# In[24]:


querydf.show()


# In[25]:


#Using group by and aggregate together
group_data = tvshowDF.groupby('name')
group_data.agg({'runtime':'max'}).show()


# ## get shows which premiered in a specific year

# In[26]:


#get all shows aired in 2011
spark.sql("SELECT premiered FROM json.`Data/tv-shows.json` where premiered like '%2011%'").show()


# ## get shows which have rating above 9 along with network name and plot the graph

# In[27]:


querydf = spark.sql("SELECT network.name, count(name) from json.`Data/tv-shows.json` where rating.average>9.0 group by network.name")
querydf = querydf.dropna(how= 'any')
querydf.show()


# In[28]:


import matplotlib.pyplot as plt
x = [row['name'] for row in querydf.collect()]
y = [row['count(name)'] for row in querydf.collect()]


# In[29]:


x


# In[30]:


plt.bar(x, y)
plt.xlabel('network name')
plt.ylabel('no of shows with rating greater than 9')
plt.title('Visual representation')
plt.legend()
plt.show()


# In[ ]:





# In[ ]:




