# Databricks notebook source

from pyspark.sql.functions import col,concat,concat_ws
from pyspark.sql.window import Window
config = {
   "output_columns":"output_valid",
   "input_Columns1":"name",
   "input_Columns2":"age"
 }
concat_exp=""
df=spark.createDataframe([['test',34],['A',23]],('name','age'))
df.show()
for col_name,col_value in config.items():
  if(col_name!='output_columns'):
    concat_exp=concat_exp+'"'+col_value+'",'
  else :
    output_column=col_value
concat_exp = concat_exp[:-1]
print(concat_exp)
print(output_column)
# dfs=dfs.withColumn(output_column,concat(concat_exp))
# dfs.show()
                         

# COMMAND ----------

from pyspark.sql.types import DecimalType,IntegerType
import logging
import decimal
def getlogger(name, level=logging.INFO):
  import logging
  import sys

  logger = logging.getLogger(name)
  logger.setLevel(level)
  logging.basicConfig(filename='/FileStore/spam.log',
                            filemode='a',
                            format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                            datefmt='%H:%M:%S',
                            level=logging.DEBUG)
  if logger.handlers:
        print(logger.handlers)
        pass
  else:
        ch = logging.StreamHandler(sys.stderr)
        ch.setLevel(level)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        ch.setFormatter(formatter)
        logger.addHandler(ch)
       
        
  return logger
def tst_log():
  logger = getlogger('my-worker')
  logger.debug('a')
  logger.info('b')
  logger.warning('c')
  logger.error('d')
  logger.critical('e')
tst_log()

# COMMAND ----------


from pyspark.sql.functions import col,concat,concat_ws
from pyspark.sql.window import Window
config = {
   "output_columns":"output_valid",
   "input_Columns1":"name",
   "input_Columns2":"age"
 }
concat_exp=""
df=spark.createDataframe([['test',34],['A',23]],('name','age'))
df.show()
for col_name,col_value in config.items():
  if(col_name!='output_columns'):
    concat_exp=concat_exp+'"'+col_value+'",'
  else :
    output_column=col_value
concat_exp = concat_exp[:-1]
print(concat_exp)
print(output_column)
# dfs=dfs.withColumn(output_column,concat(concat_exp))

# dfs.show()
                         
