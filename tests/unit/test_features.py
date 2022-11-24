from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from databricks.feature_store import FeatureStoreClient
from unittest.mock import MagicMock, patch
import unittest

from project.features import dummy_feature
from project.features_etl import update_features


spark = SparkSession.builder.appName('unittests').getOrCreate()


class FeatureTest(unittest.TestCase):
  
  def test_dummy_feature(self):
    rows = [['x', 9], ['y', 9], ['z', 9]]
    columns = ['entity_id', 'B']
    df = spark.createDataFrame(rows, columns)
    df_transformed = dummy_feature(df)
    assert 3 == df_transformed.agg(F.sum('dummy')).collect()[0][0]
    
  @patch.object(FeatureStoreClient, "write_table")
  @patch("project.features_etl.dummy_feature")
  def test_feature_update(self, dummy_feature__mock, write_table__mock):
    features_df = MagicMock()
    dummy_feature__mock.return_value = features_df

    update_features(spark, 'recommender_system.customer_features')  # Function being tested

    write_table__mock.assert_called_once_with(
      name='recommender_system.customer_features',
      df=features_df,
      mode='merge'
    )