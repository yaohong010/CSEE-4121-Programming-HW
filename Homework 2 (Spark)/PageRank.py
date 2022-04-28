#!/usr/bin/env python
# coding: utf-8

# In[45]:


import re
import regex
import pandas
from pyspark.sql.functions import lower, col, udf, explode, split,regexp_replace,lit,when
from pyspark.sql.types import StringType,BooleanType,DateType,ArrayType
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from operator import add

import timeit


# test on small
spark = SparkSession.builder.appName("PageRank").getOrCreate()
sc = spark._sc

csv_path = "gs://bucket-hero/task3/task3_small.csv"

df = spark.read.format("csv").option("header", "False").option("delimiter", "\t").load(csv_path).withColumnRenamed('_c1','target').withColumnRenamed('_c0','source')

links = df.distinct().rdd.groupByKey().cache()

ranks = links.map(lambda url_neighbors: (url_neighbors[0], 1.0))

def computeContribs(urls, rank: float):
    """Calculates URL contributions to the rank of other URLs."""
    num_urls = len(urls)
    for url in urls:
        yield (url, rank / num_urls)


for i in range(10):
    print(i)
    # Calculates URL contributions to the rank of other URLs.
    contribs = links.join(ranks).flatMap(lambda url_urls_rank: computeContribs(
        url_urls_rank[1][0], url_urls_rank[1][1] 
    ))

    # Re-calculates URL ranks based on neighbor contributions.
    ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.85 + 0.15)


# for (link, rank) in sorted(ranks.take(5)):
#     print("%s has rank: %s." % (link, rank))


out = spark.createDataFrame(sorted(ranks.take(5)))
out.show()

# gcs_bucket = 'bucket-hero'  # change it to your bucket name
gcs_filepath = 'gs://bucket-hero/task3_final_output' #.format(gcs_bucket)
out.coalesce(1).write.option("delimiter", "\t").csv(gcs_filepath)