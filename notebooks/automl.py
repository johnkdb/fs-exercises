# Databricks notebook source
# MAGIC %md # Prerequisites
# MAGIC 
# MAGIC This notebook reuses the feature table created by
# MAGIC [`databricks-academy` / `scalable-machine-learning-with-apache-spark-english` / ML 10 Feature Store](https://github.com/databricks-academy/scalable-machine-learning-with-apache-spark-english/blob/published/ML%2010%20-%20Feature%20Store.py).

# COMMAND ----------

# MAGIC %md # Input
# MAGIC 
# MAGIC Edit `feature_table_name` as needed.

# COMMAND ----------

feature_table_name = 'odl_instructor_798535_databrickslabs_com.airbnb_88328e'

# COMMAND ----------

# MAGIC %md # Setup

# COMMAND ----------

# DBTITLE 1,Feature Table
spark.sql(f'select * from {feature_table_name}').display()

# COMMAND ----------

# DBTITLE 1,Training data that needs to be enriched with features
from pyspark.sql.functions import monotonically_increasing_id, rand
file_path = f"dbfs:/mnt/dbacademy-datasets/scalable-machine-learning-with-apache-spark/v02/airbnb/sf-listings/sf-listings-2019-03-06-clean.delta/"
airbnb_df = spark.read.format("delta").load(file_path).coalesce(1).withColumn("index", monotonically_increasing_id())
training_df = airbnb_df.select("index", "price", (rand() * 0.5-0.25).alias("score_diff_from_last_month"))
display(training_df)

# COMMAND ----------

# MAGIC %md # Exercise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Train an AutoML Regression model
# MAGIC 
# MAGIC See the [documentation for running AutoML with Feature Store lookups](https://learn.microsoft.com/en-gb/azure/databricks/machine-learning/automl/train-ml-model-automl-api). Remember that the parameter that specifies the lookups *must* be a list, even though we only look up from a single Feature Store table.
# MAGIC 
# MAGIC For the solution to this problem you may look at the notebooks at the bottom of the documentation page.
# MAGIC 
# MAGIC ## 2. Score the best model on the training data
# MAGIC 
# MAGIC Here we use [`FeatureStoreClient.score_batch` (link to documentation)](https://docs.databricks.com/dev-tools/api/python/latest/feature-store/client.html#databricks.feature_store.client.FeatureStoreClient.score_batch) to reference the best model found by AutoML, as well as the other required parameters that you need to look up in the linked documentation.

# COMMAND ----------

# DBTITLE 1,1. Train an AutoML Regression model
from databricks import automl

# Use the documentation linked above to figure out the parameters needed for AutoML to use the Feature Store.

# The table name is defined at the top of this Notebook. Its key(s) can be found via the Feature Store UI or by FeatureStoreClient.get_table. You need to specify which keys from the training dataset that correspond to the FS Table key(s) in the same order.

# Hint: The AutoML parameter for Feature Store lookups follow a different syntax than what you've seen before with FeatureLookup objects. See the documentation for more details.

summary = automl.regress(
  training_df,
  timeout_minutes=10,
  target_col="price",
  # TODO: Both the parameter name and its value is missing here. See the documentation on what is needed.
)

# COMMAND ----------

# DBTITLE 1,2. Score the best model on the training data
from databricks.feature_store import FeatureStoreClient
fs = FeatureStoreClient()

model_uri = f'runs:/{summary.best_trial.mlflow_run_id}/model'

predictions_df = fs.score_batch(
  # Here we would like to score the best model from AutoML (referenced on line 4) on our training data (for simplicity).
  # Both the names and the values of the required parameters are missing. See the documentation on score_batch linked further up.
  result_type='double')

display(predictions_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Model Registry
# MAGIC For reference, the following code moves the model through the Model Registry into the Production stage:
# MAGIC 
# MAGIC ```
# MAGIC import mlflow
# MAGIC from mlflow.tracking.client import MlflowClient
# MAGIC client = MlflowClient()
# MAGIC ```
# MAGIC ```
# MAGIC model_details = mlflow.register_model(
# MAGIC   model_uri=model_uri,
# MAGIC   name=model_name)
# MAGIC ```
# MAGIC ```
# MAGIC client.transition_model_version_stage(
# MAGIC   name=model_details.name,
# MAGIC   version=model_details.version,
# MAGIC   stage='production',
# MAGIC )
# MAGIC ```
# MAGIC 
# MAGIC This is not required in this exercise and is only included as an example.
