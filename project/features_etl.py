from project.features import dummy_feature
from databricks.feature_store import FeatureStoreClient
from pyspark.sql import SparkSession


def input_data(spark):
  return spark.createDataFrame(
    [['43hj34', 1, '@'],
     ['1kkfj3', 3, '_'],
     ['45jk2d', 2, '!'],
     ['432kjj', 6, '(']],
    ['entity_id', 'B', 'C'])

  
def update_features(spark, table_name):
  df = input_data(spark)
  df_features = dummy_feature(df)

  fs = FeatureStoreClient()
  fs.write_table(
    name=table_name,
    df=df_features,
    mode='merge')