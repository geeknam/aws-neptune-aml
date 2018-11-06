import sys

from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import lit

glueContext = GlueContext(SparkContext.getOrCreate())

job_args = getResolvedOptions(sys.argv, [
    'glue-db-name',
    'glue-table-name',
    's3-bucket-name'
])
# Data Catalog: database and table name
db_name = job_args['glue-db-name']
tbl_name = job_args['glue-table-name']


# Read data into a DynamicFrame using the Data Catalog metadata
dyf = glueContext.create_dynamic_frame.from_catalog(
    database=db_name, table_name=tbl_name
)
df = dyf.toDF()

orig_account_df = df.select('nameorig').distinct()
dest_account_df = df.select('namedest').distinct()
all_account_df = orig_account_df.union(dest_account_df).distinct()

accounts = all_account_df\
    .withColumnRenamed('nameOrig', '~id')\
    .withColumn('~label', lit('ACCOUNT'))

accounts_dyf = DynamicFrame.fromDF(accounts, glueContext, "accounts")

# S3 location for output
output_dir = "s3://%s/transformed/vertex-accounts" % job_args['s3-bucket-name']
glueContext.write_dynamic_frame.from_options(
    frame=accounts,
    connection_type="s3",
    connection_options={"path": output_dir},
    format="csv"
)
