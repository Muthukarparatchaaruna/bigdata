# Databricks notebook source
diction = {
      "Items": [
        {
            "AlbumTitle": {
                "S": "Updated Album Title"
            },
            "Awards": {
                "N": 10
            },
            "Artist": {
                "N": "Acme Band"
            },
            "SongTitle": {
                "S": 2
            }
        }, {
            "AlbumTitle": {
                "S": "Updated Title"
            },
            "Awards": {
                "N": 120
            },
            "Artist": {
                "N": " Band"
            },
            "SongTitle": {
                "S": 1
            }
        }
        
    ]}

import ast
import json
import decimal
from pyspark.sql.functions import col

li=[]
for k in diction['Items']:
  d={}
  for i,j in k.items():
    if 'S' in j.keys():
      d[i]=j['S']
    else:
      d[i]=j['N']
  li.append(d)
print(li)
df = spark.createDataFrame(li)
#print(d)
df.show()
df2=spark.createDataFrame([{'Awards':1}])
df2.show()
rws=df2.collect()[0]['Awards']
print(rws)
df3=df.withColumn('Awards',df.Awards-rws)
df3.show()


# COMMAND ----------

from pyspark.sql.functions import *
df=spark.createDataFrame([
  ['test',0,'03-APR-19'],
  ['test1',20,'03-APR-19'],
  ['test2',30,'03-APR-19']]
,('name','TRAN_TIME','TRAN_DATE'))
df.filter("TRAN_TIME" >0).show()

# COMMAND ----------

from pyspark.sql.functions import *
df=spark.createDataFrame([
  ['test',0,'03-APR-19'],
  ['test1',20,'03-APR-19'],
  ['test2',30,'03-APR-19']]
,('name','TRAN_TIME','TRAN_DATE'))

input_trans_date_format = "yyyyMMdd"
trandateformat= "yyyyMMdd"
trandatestrformat= "dd-MMM-yy"
trandatelongformat="dd-MMM-yyyy"
df=df.withColumn("TRAN_DATE", date_format(to_date(col("TRAN_DATE"), trandatestrformat), trandateformat))
df = df.withColumn("TXN_DATE", concat_ws(' ', df.TRAN_DATE, df.TRAN_TIME))
df.show()
