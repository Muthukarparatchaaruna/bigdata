# Databricks notebook source
from pyspark.sql.functions import col,concat,concat_ws,lit
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
config = {
   "rename_columns":{"dataframe":"df2","renameto":["rename_id","re_name"],"renamefrom":["id","name"]},
    "rename_columns":{"dataframe":"df2","RenameCols":{"id":"rename_id"}}
#    "Select_columns":["name","id","sex"],
      "reject_columns":["rename_id"],
   "join_type":"inner",
  "join_condition":"df1.id == df2.rename_id"  
 }
spark = SparkSession.builder.appName('aggs').getOrCreate()
#reading from config
rejectcol=[]
selectcol=[]
for col_name,col_value in config.items():
  if(col_name=='join_condition'):
    join_condition=col_value
  if(col_name=='join_type'):
    join_type=col_value
  if(col_name=='Select_columns'):
    selectcol=col_value
  if(col_name=='reject_columns'):
    rejectcol=col_value
dataframe=config['rename_columns']['dataframe']
renameto=config['rename_columns']['renameto']
renamefrom=config['rename_columns']['renamefrom']

print("join_condition:",join_condition,"\nrenamefrom:",renamefrom,"\nrenameto:",renameto,"\ndataframe:",dataframe)
print("selectcol:",selectcol,"\nrejectcol:",rejectcol)

   
#defining dataframe 
df1=spark.createDataFrame([['test',34],['A',23]],('name','id'))
df2=spark.createDataFrame([['M',34,'test'],['F',23,'A'],['F',25,'B']],('sex','id','name'))

#column renaming from an array of columns
df2=eval(dataframe)
if len(renameto)==len(renamefrom):
  print("length equal")
  for x in range(len(renameto)):
    renamet=renameto[x]
    renamef=renamefrom[x]
    df2=df2.withColumnRenamed(renamef,renamet)
df2.show()

#Joing the dataframes
df  = df1.join(df2, eval(join_condition), how=join_type)
df.show()

#selecting specific columns using reject/select column values
if not selectcol :
  cols = [c for c in df.columns if c not in rejectcol ]
else :
  cols = selectcol
print(cols)
final_df=df.select(*cols)
final_df.show()

                         

# COMMAND ----------

config = {
  "PAD_column" :"age",
  "PAD_type" :"lpad",
  "PAD_Value":"*",
  "PAD_length":"10"  
}

from pyspark.sql.functions import *
df=spark.createDataFrame([
  ['test',0,45],
  ['test1',20,34],
  ['test2',30,56]]
,('name','age','deptid'))

df.show()

for col_name,col_value in config.items():
  if(col_name=='PAD_column'):
    PAD_column=col_value
  if(col_name=='PAD_type'):
    PAD_type=col_value
  if(col_name=='PAD_Value'):
    PAD_Value=col_value
  if(col_name=='PAD_length'):
    PAD_length=col_value 
print("**************")
print("Column",PAD_column);
print("Type",PAD_type);
print("Length",PAD_length);
print("Value",PAD_Value);
value=PAD_type+"(col('"+PAD_column+"'),"+PAD_length+",'"+PAD_Value+"')"
print(value)
df1= df.withColumn(PAD_column,eval(value))
df1.show()


# COMMAND ----------

from pyspark.sql.functions import col,concat,concat_ws,lit
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
config = {
   "rename_columns":{"dataframe":"df2","renameto":["rename_id","re_name"],"renamefrom":["id","name"]},
#    "Select_columns":["name","id","sex"],
      "reject_columns":["rename_id"],
   "join_type":"inner",
  "join_condition":"df1.id == df2.rename_id"  
 }
spark = SparkSession.builder.appName('aggs').getOrCreate()
#reading from config
rejectcol=[]
selectcol=[]
for col_name,col_value in config.items():
  if(col_name=='join_condition'):
    join_condition=col_value
  if(col_name=='join_type'):
    join_type=col_value
  if(col_name=='Select_columns'):
    selectcol=col_value
  if(col_name=='reject_columns'):
    rejectcol=col_value
dataframe=config['rename_columns']['dataframe']
renameto=config['rename_columns']['renameto']
renamefrom=config['rename_columns']['renamefrom']

print("join_condition:",join_condition,"\nrenamefrom:",renamefrom,"\nrenameto:",renameto,"\ndataframe:",dataframe)
print("selectcol:",selectcol,"\nrejectcol:",rejectcol)

   
#defining dataframe 
df1=spark.createDataFrame([['test',34],['A',23]],('name','id'))
df2=spark.createDataFrame([['M',34,'test'],['F',23,None],['F',25,'B']],('sex','id','name'))

#column renaming from an array of columns
df2=eval(dataframe)
if len(renameto)==len(renamefrom):
  print("length equal")
  for x in range(len(renameto)):
    renamet=renameto[x]
    renamef=renamefrom[x]
    df2=df2.withColumnRenamed(renamef,renamet)
df2.show()

#Joing the dataframes
df  = df1.join(df2, eval(join_condition), how=join_type)
df.show()

#selecting specific columns using reject/select column values
if not selectcol :
  cols = [c for c in df.columns if c not in rejectcol ]
else :
  cols = selectcol
print(cols)
final_df=df.select(*cols)
final_df.show()

                         

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
config = {
   "output_columns":"output_valid",
   "input_Columns":["name","age"]
}
spark = SparkSession.builder.appName('aggs').getOrCreate()
# concat_exp=[]
df=spark.createDataFrame([['test',34],[None,23]],('name','age'))

df.show()
df=df.na.fill("")
for col_name,col_value in config.items():
  if(col_name!='output_columns'):
   concat_exp=col_value
  else :
    output_column=col_value

print(concat_exp)
print(output_column)

# df.createOrReplaceTempView("EMP")

# dfs=spark.sql("select CONCAT("+concat_exp+") as "+output_column+" from EMP")
def concat_cols(*list_cols):
  return ''.join(list([str(i) for i in list_cols]))

concat_cols = udf(concat_cols)
dfs = df.withColumn('column_join', concat_cols(*concat_exp))

dfs.show()

                         



# COMMAND ----------

from pyspark.sql.functions import col,concat,concat_ws,trim,when,expr 

config = {
   "output_columns":"output_valid",
   "reference_column":"name",
   "when_conditions":[{
     "ref_value":"test",
     "output_value":"a",
     "toDataType":"STRING"
   },
   {
     "ref_value":"test1",
     "output_value":"b",
     "toDataType":"STRING"
   },
   {
     "ref_value":"test2",
     "output_value":"c",
     "toDataType":"STRING"
   }]
 }

ref_column=''
df=spark.createDataFrame([['test',34],['test1',23],['test2',23]],('name','age'))
df.show()
for col_name,col_value in config.items():
  if(col_name=='reference_column'):
    ref_column=col_value
  elif (col_name=='output_columns') :
    output_column=col_value
  else :
    change_columns=col_value
    

print(ref_column)
print(output_column)
print(change_columns)
case_exp="CASE  "
for  item in change_columns:
   case_exp=case_exp+" WHEN "+ref_column+" == '"+item['ref_value']+"' THEN "+"'"+item['output_value']+"'" 
case_exp=case_exp+" ELSE 'NA' END AS "+output_column
print(case_exp)

dfs=df.withColumn(output_column,expr(case_exp))

dfs.show()

                         

# COMMAND ----------

from pyspark.sql.functions import col,concat,concat_ws,lit
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
config = {
   "output_columns":"output_valid",
   "input_Columns":"USA",
   "input_Columns2":"NORTH AMERICA"
 }
spark = SparkSession.builder.appName('aggs').getOrCreate()
concat_exp=[]
column_valeus=''
df=spark.createDataFrame([['test',34],['A',23]],('name','age'))
df.show()
for col_name,col_value in config.items():
  if(col_name!='output_columns'):
   concat_exp.append(col_value)
   column_valeus=column_valeus+col_value+'-'
  else :
    output_column=col_value
column_valeus=column_valeus[:-1]
print(concat_exp)
print(output_column)
print(column_valeus)
# df.createOrReplaceTempView("EMP")

# dfs=spark.sql("select CONCAT("+concat_exp+") as "+output_column+" from EMP")
def concat_cols(*list_cols):
    return ''.join(list([str(i) for i in list_cols]))

concat_cols = udf(concat_cols)
print(concat_cols)
# dfs = df.withColumn(output_column, concat_cols(*concat_exp))
dfs=df.withColumn(output_column,lit(column_valeus))

dfs.show()

                         

# COMMAND ----------

import pyspark.sql.functions as f
from pyspark.sql.types import *
data = spark.createDataFrame([('s1', 't1'), ('s2', 't2')], ['col1', 'col2'])
data = data.withColumn('test', f.lit(None).cast(StringType()))
val="f.concat('col1', 'col2', 'test')"
display(data.na.fill('').withColumn('test2', eval(val)))
