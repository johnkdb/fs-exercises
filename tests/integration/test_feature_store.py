import uuid
import time
import unittest
from databricks.feature_store import FeatureStoreClient
from pyspark.sql.types import StructType, StringType, StructField, IntegerType
from pyspark.sql import SparkSession

from project.features_etl import update_features


class FeatureStoreIntegrationTest(unittest.TestCase):
  def setUp(self):
    self.spark = SparkSession.builder.appName('integrationtests').getOrCreate()
    self.fs = FeatureStoreClient()
    self.table_name = f'default.feature_store_integration_test_{str(uuid.uuid1())[:8]}'
    
    schema = StructType([
      StructField('entity_id', StringType(), True),
      StructField('dummy', IntegerType(), True)])
    
    # Create a feature store table with the name and schema defined above.
    # See the docs on the create_table function signature:
    # https://docs.databricks.com/dev-tools/api/python/latest/feature-store/client.html#databricks.feature_store.client.FeatureStoreClient.create_table
    print(f'Creating feature table {self.table_name}')
    self.fs.create_table(
      name=self.table_name,
      primary_keys='entity_id',
      schema=schema,
      description='Integration test features')

  def test_write(self):
    update_features(self.spark, self.table_name)
    # Query the underlying Delta table and verify that it has 3 rows.
    # You can use Spark to query self.table_name.
    count = self.spark.sql(f'select * from {self.table_name}').count()
    self.assertEqual(count, 4)

  def tearDown(self):
    # Uncomment this and search for "feature_store_integration_test" in
    # the Feature Store UI to verify that this test communicates with the service.
    # time.sleep(120)
    print(f'Dropping feature table {self.table_name}')
    self.fs.drop_table(name=self.table_name)


if __name__ == '__main__':
    loader = unittest.TestLoader()
    tests = loader.loadTestsFromTestCase(FeatureStoreIntegrationTest)
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(tests)
    if not result.wasSuccessful():
        raise RuntimeError('One or multiple tests failed. Please check job logs for additional information.')