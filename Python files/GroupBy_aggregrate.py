#!/usr/bin/env python
# coding: utf-8

# # Analysis functions on a file

# In[11]:


import findspark
findspark.init('C:\\Spark\\spark-3.0.0-bin-hadoop2.7')
from pyspark.sql import SparkSession
import matplotlib.pyplot as plt


# In[12]:


spark1 = SparkSession.builder.appName('aggs').getOrCreate()


# ### Read a dataset from a CSV file into a dataframe

# In[13]:


df = spark1.read.csv('Data/sales_info.csv',inferSchema=True,header=True)


# ### See how the data looks like and print the schema

# In[14]:


df.show()


# In[15]:


df.printSchema()


# ### groupBy

# In[16]:


df.groupby('Company')


# #### We can perform a variety of operations on this pyspark.sql.group.GroupedData object. 
# For example, taking a mean (average).

# In[17]:


df.groupby('Company').mean()


# In[18]:


temp_frame = df.groupby('Company').mean()
temp_frame.show()


# In[19]:


x = [row['Company'] for row in temp_frame.collect()]
y = [row['avg(Sales)'] for row in temp_frame.collect()]

plt.bar(x, y)
plt.xlabel('company')
plt.ylabel('Average sales')
plt.title('Visual representation')
plt.legend()
plt.show()


# In[20]:


df.groupby('Company').max().show()


# In[21]:


df.groupby('Company').min().show()


# In[22]:


df.groupby('Company').count().show()


# In[23]:


df.agg({'Sales':'sum'}).show()


# ### Suppose we want the maximum of all sales

# In[24]:


df.agg({'Sales':'max'}).show()


# In[25]:


group_data = df.groupBy('Company')


# In[26]:


group_data.agg({'Sales':'max'}).show()


# ### Import and use other functions

# In[27]:


from pyspark.sql.functions import stddev,countDistinct,count,avg


# In[28]:


df.agg({'Sales':'stddev'}).show()


# #### We could compute the same quantity using simple select too

# In[29]:


df.select(stddev('Sales')).show()


# #### We can count how many total sales and how many of them are distinct in value

# In[30]:


df.select(count('Sales')).show()


# In[31]:


df.select(countDistinct('Sales')).show()


# ### Formatting results
# 

# In[32]:


from pyspark.sql.functions import format_number


# In[33]:


sales_std = df.select(stddev('Sales').alias('std'))


# In[34]:


sales_std


# In[35]:


sales_std=sales_std.select(format_number('std',2).alias('final'))


# In[36]:


sales_std


# In[37]:


sales_std.show()


# In[38]:


# df.show()
temp_frame = df
temp_frame.show()


# In[39]:


x = [row['Person'] for row in temp_frame.collect()]
y = [row['Sales'] for row in temp_frame.collect()]
z = [row['Company'] for row in temp_frame.collect()]

print(set(z))


# In[40]:




plt.bar(x, y)
plt.xlabel('Person')
plt.ylabel('Sales')
plt.title('Visual representation')
plt.legend()
plt.show()


# In[41]:


df.orderBy('Sales').show()


# In[42]:


df.orderBy(df['Sales'].desc()).show()


# In[ ]:





# In[ ]:




