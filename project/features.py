from pyspark.sql import functions as F

def dummy_feature(df):
  return df.select(
    'entity_id',
    F.lit(9).alias('my_feature')  # This logic is incorrect accoding to the unit test. Change this line to make the unit test succeed.
  )