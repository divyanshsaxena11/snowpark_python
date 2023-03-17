# %%
#Importing Librarires for notebook

#.. Snowflake Snowpark Session Library
from snowflake.snowpark import Session

#.. Snowflake Snowpark Datatypes
from snowflake.snowpark.types import StructType, StructField, IntegerType, StringType

#.. Snowflake Snowpark Transformation
from snowflake.snowpark.functions import col

#.. Snowflake Snowpark All Functions
import snowflake.snowpark.functions as f

# %%
# Constructing Dict for Connection Params

conn_config = {
    "account": "XXXXXXX.central-india.azure",
    "user": "XXXXXXX",
    "password": "XXXXXXX",
    "role" : "accountadmin",
    "warehouse" : "compute_wh",
    "database" : "snowflake_alert",
    "schema" : "edw"
}


