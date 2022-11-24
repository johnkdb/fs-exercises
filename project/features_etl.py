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
  # Calculate features on df based on the feature engineering code in the module project.features
  df_features = None  # fill in

  # See the accompanying unit test to see how the feature store is expected to be used here.
  fs = FeatureStoreClient()
  fs.write_table(
    name=None,  # fill in
    df=None,  # fill in
    mode=None)  # fill in