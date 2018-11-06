from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import lit

glueContext = GlueContext(SparkContext.getOrCreate())

# Data Catalog: database and table name
db_name = "aml-db"
tbl_name = "raw_data"


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
output_dir = "s3://neptune-aws-aml-data/transformed"
glueContext.write_dynamic_frame.from_options(
    frame=accounts,
    connection_type="s3",
    connection_options={"path": output_dir},
    format="csv"
)
