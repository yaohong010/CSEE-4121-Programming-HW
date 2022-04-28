import re
import regex
from pyspark.sql.functions import lower, col, udf, explode, split,regexp_replace
from pyspark.sql.types import StringType,BooleanType,DateType,ArrayType


# In[ ]:


from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
df = spark.read.format('xml').options(rowTag='page').load('hdfs:/enwiki_whole.xml')
# df = spark.read.format('xml').options(rowTag='page').load('hdfs:/enwiki_small.xml')


# In[5]:


def findLink(str):
    if str == "" or str is None:
        return
    matches = regex.findall(r'\[\[((?:[^[\]]+|(?R))*+)\]\]', str)
    if len(matches) == 0:
        return
#     sub = matches[0].split('|')
#     print('sub=', sub)
    res = []
    for s in matches:
        s = s.split('|')[0]
        if '#' in s:
            continue
        elif not s.startswith('Category:') and (':' in s):
            continue  
        res.append(s.lower())    
    if len(res) == 0:
        return ''
    else:
        return list(set(res))


findLink_UDF = udf(lambda z: findLink(z), ArrayType(StringType()))
df1= df.select(lower(col("title")).alias("title"), findLink_UDF(col("revision.text._VALUE")).alias("links"))
# df1 = df1.withColumn("links", split(regexp_replace(col("links"), r"(^\[)|(\]$)|(')", ""), ", "))
df2 = df1.select(df1.title.alias("title"),explode(df1.links).alias("links")).sort(["title", "links"], ascending = True)
    
# findLink(txt)


# In[6]:


gcs_bucket = 'bucket-hero'  # change it to your bucket name
gcs_filepath = 'gs://bucket-hero/whole.csv'.format(gcs_bucket)
df2.coalesce(1).write.option("delimiter", "\t").option("header","true").csv(gcs_filepath)