from pyspark.sql import functions as F

def dummy_feature(df):
  return df.select(
    'entity_id',
    F.lit(1).alias('dummy'))