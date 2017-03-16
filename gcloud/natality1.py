#!/usr/bin/python
"""Create a Google BigQuery linear regression input table by querying the
public "natality" dataset.

In the code below, the following actions are taken:
  * A new dataset is created “natality_regression.”
  * A new table “regression_input” is created to hold the inputs for our linear
    regression.
  * A query is run against the public dataset,
    bigquery-public-data.samples.natality, selecting only the data of interest
    to the regression, the output of which is stored in the “regression_input”
    table.
  * The output table is moved over the wire to the user's default project via
    the built-in BigQuery Connector for Spark that bridges BigQuery and Cloud
    Dataproc.
"""

from gcloud import bigquery
from gcloud.bigquery import job
from gcloud.bigquery.table import *

# Create a new Google BigQuery client using Google Cloud Platform project
# defaults.
bq = bigquery.Client()


# Create a new BigQuery dataset.
reg_dataset = bq.dataset("natality_regression")
reg_dataset.create()

# In the new BigQuery dataset, create a new table.
table = reg_dataset.table(name="regression_input")
# The table needs a schema before it can be created and accept data.
# We create an ordered list of the columns using SchemaField objects.
schema = []
schema.append(SchemaField("weight_pounds", "float"))
schema.append(SchemaField("mother_age", "integer"))
schema.append(SchemaField("father_age", "integer"))
schema.append(SchemaField("gestation_weeks", "integer"))
schema.append(SchemaField("weight_gain_pounds", "integer"))
schema.append(SchemaField("apgar_5min", "integer"))

# We assign the schema to the table and create the table in BigQuery.
table.schema = schema
table.create()

# Next, we issue a query in StandardSQL.
# The query selects the fields of interest.
query = """
SELECT weight_pounds, mother_age, father_age, gestation_weeks,
weight_gain_pounds, apgar_5min
from `bigquery-public-data.samples.natality`
where weight_pounds is not null
and mother_age is not null and father_age is not null
and gestation_weeks is not null
and weight_gain_pounds is not null
and apgar_5min is not null
"""

# We create a query job to save the data to a new table,
qj = job.QueryJob("natality-extract", query, bq)
# and set specific parameters:
#  * 'write_disposition' to overwrite an existing table with this name in the
#     dataset
#  * 'use_legacy_sql' to ensure we’re using Standard SQL
#  * 'destination' to set the output table to the table created above
qj.write_disposition="WRITE_TRUNCATE"
qj.use_legacy_sql = False
qj.destination = table

# 'qj.begin' submits the query.
qj.begin()
