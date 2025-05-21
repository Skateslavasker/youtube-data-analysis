import sys
from awsglue.transforms import *                         # Import built-in AWS Glue transforms
from awsglue.utils import getResolvedOptions             # Utility to fetch job arguments
from pyspark.context import SparkContext                 # Entry point for Spark
from awsglue.context import GlueContext                  # Entry point for Glue
from awsglue.job import Job                              # Class to handle Glue job lifecycle

from awsglue.dynamicframe import DynamicFrame            # AWS Glue abstraction over Spark DataFrame

# Retrieve job arguments; expecting a JOB_NAME parameter
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Initialize Spark and Glue contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Initialize the Glue job 
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Predicate to filter only specific regions (Canada, UK, US) during data read
predicate_pushdown = "region in ('ca', 'gb', 'us')"

# Step 1: Read raw data from AWS Glue Data Catalog (source: S3 via catalog table)
datasource0 = glueContext.create_dynamic_frame.from_catalog(
    database="data_youtube_raw",
    table_name="raw_statistics",
    transformation_ctx="datasource0",
    push_down_predicate=predicate_pushdown  # Pushdown filter applied at read time
)

# Step 2: Apply explicit schema mapping to the raw data fields
applymapping1 = ApplyMapping.apply(
    frame=datasource0,
    mappings=[
        ("video_id", "string", "video_id", "string"),
        ("trending_date", "string", "trending_date", "string"),
        ("title", "string", "title", "string"),
        ("channel_title", "string", "channel_title", "string"),
        ("category_id", "long", "category_id", "long"),
        ("publish_time", "string", "publish_time", "string"),
        ("tags", "string", "tags", "string"),
        ("views", "long", "views", "long"),
        ("likes", "long", "likes", "long"),
        ("dislikes", "long", "dislikes", "long"),
        ("comment_count", "long", "comment_count", "long"),
        ("thumbnail_link", "string", "thumbnail_link", "string"),
        ("comments_disabled", "boolean", "comments_disabled", "boolean"),
        ("ratings_disabled", "boolean", "ratings_disabled", "boolean"),
        ("video_error_or_removed", "boolean", "video_error_or_removed", "boolean"),
        ("description", "string", "description", "string"),
        ("region", "string", "region", "string")
    ],
    transformation_ctx="applymapping1"
)

# Step 3: Resolve ambiguous types (e.g., different types for the same field) by converting to a struct
resolvechoice2 = ResolveChoice.apply(
    frame=applymapping1,
    choice="make_struct",                     # Encapsulates ambiguous fields into structs
    transformation_ctx="resolvechoice2"
)

# Step 4: Drop records that have nulls in any fields
dropnullfields3 = DropNullFields.apply(
    frame=resolvechoice2,
    transformation_ctx="dropnullfields3"
)

# Step 5: Convert cleaned DynamicFrame to Spark DataFrame and reduce number of output files to 1
datasink1 = dropnullfields3.toDF().coalesce(1)

# Convert Spark DataFrame back to DynamicFrame for Glue writing
df_final_output = DynamicFrame.fromDF(
    datasink1,
    glueContext,
    "df_final_output"
)

# Step 6: Write the cleansed and partitioned data to S3 in Parquet format
datasink4 = glueContext.write_dynamic_frame.from_options(
    frame=df_final_output,
    connection_type="s3",
    connection_options={
        "path": "s3://data-youtube-raw-useast1-cleansed-version/youtube/raw_statistics/",
        "partitionKeys": ["region"]          # Partition output by region for efficient querying
    },
    format="parquet",                        # Efficient columnar storage format
    transformation_ctx="datasink4"
)


job.commit()
