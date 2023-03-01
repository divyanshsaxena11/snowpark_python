# %% [markdown]
# # Snowflake Snowpark Using Python
# <blockquote>
#     Importing Snowflake Snowpark Libraries
#     <br>Establishing Connection To Snowflake 
#     <br>Understanding Snowpark Dataframe
#     <br>Reading Snowflake Data into Dataframe
# </blockquote>

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

#.. Snowflake Snowpark Window Function
from snowflake.snowpark import Window
from snowflake.snowpark.functions import row_number

# %%
# Constructing Dict for Connection Params

conn_config = {
    "account": "vt67141.central-india.azure",
    "user": "divyansh",
    "password": "Divyansh@123",
    "role" : "accountadmin",
    "warehouse" : "compute_wh",
    "database" : "snowflake_alert",
    "schema" : "edw"
}

# %%
#Invoking Snowpark Session for Establishing Connection

conn = Session.builder.configs(conn_config).create()

# %%
# Using Snowpark Session Sql to query data
# Follows lazily executed approach
#.. Will not query data for below on snowflake
query_1 = conn.sql("select * from tb_catalog_sales limit 10")

#.. Will query data for below on snowflake because of **collect()**
query_2 = conn.sql("select * from tb_catalog_sales limit 20").collect()

#.. Will query data for query_1 because of **show()**
#query_1.show()

# %% [markdown]
# # Examples on How to Use Snowpark Dataframes

# %%
#.. Creating Dataframe by directly reading a table

df_tbl_read = conn.table("edw.tb_catalog_sales")
#df_tbl_read.show()

#.. Creating Dataframe by reading data from Stage
#conn.sql("create stage @test_stage;").collect()
#conn.sql("put file://C:\\Users\\divya\\Downloads\\test_file.csv  @test_stage;").collect()
df_stg_read = conn.read.schema(StructType([StructField("name", StringType()),StructField("url", StringType()),StructField("username", StringType()),StructField("password", StringType())])).csv("@test_stage/test_file.csv")
#df_stg_read.show()

#.. Creating Dataframe by specifying range or sequence
df_create = conn.create_dataframe([(1,'one'),(2,"two")],schema = ["col_1","col_2"])
df_create_rng = conn.range(1,100,3).to_df("col_a")
#df_create.show()
#df_create_rng.show()

#.. Creating Dataframe by Joining two tables/dataframes
df_tbl2_read = conn.table("edw.tb_catalog_sales_logs")
df_join = df_tbl_read.join(df_tbl2_read, df_tbl_read["CS_ITEM_SK"] == df_tbl2_read["CS_ITEM_SK"])
#df_join.show()


# %%
#.. Performing Operations on Snowpark Dataframe

#.. Using Select Method to create new Dataframe with specific columns from existing DF
#.. https://docs.snowflake.com/ko/developer-guide/snowpark/reference/python/api/snowflake.snowpark.DataFrame.select.html#snowflake.snowpark.DataFrame.select
df_select1 = df_tbl_read.select(col("CS_SOLD_TIME_SK"))
df_select2 = df_tbl_read.select(col("CS_SOLD_TIME_SK").substr(0, 3).as_("column1"))
df_select3 = df_tbl_read.select(col("CS_SOLD_TIME_SK").as_("column1"),col("CS_BILL_CDEMO_SK"))
#df_select1.show()
#df_select2.show()
#df_select3.show()

#.. Using Filter Method to filter data (Similar to Where Clause)
#.. AND condition - &
#.. OR Condition - |
#.. https://docs.snowflake.com/ko/developer-guide/snowpark/reference/python/api/snowflake.snowpark.DataFrame.filter.html#snowflake.snowpark.DataFrame.filter
df_filter = df_tbl_read.filter((col("CS_SOLD_TIME_SK")==46616)&(col("CS_BILL_CDEMO_SK")==1642927)).select(col("CS_EXT_WHOLESALE_COST"))
#df_filter.show()

#.. Using SORT Method to Order the data
#.. https://docs.snowflake.com/ko/developer-guide/snowpark/reference/python/api/snowflake.snowpark.DataFrame.sort.html#snowflake.snowpark.DataFrame.sort

df_sort1 = df_tbl_read.filter((col("CS_QUANTITY").isNotNull()) & (col("CS_WHOLESALE_COST").isNotNull())).sort(col("CS_QUANTITY").asc(),col("CS_WHOLESALE_COST").desc()).select(col("CS_ORDER_NUMBER"),col("CS_QUANTITY"),col("CS_WHOLESALE_COST"),col("CS_LIST_PRICE"))
#df_sort1.show()
df_sort2 = df_tbl_read.filter((col("CS_QUANTITY").isNotNull()) & (col("CS_WHOLESALE_COST").isNotNull())).sort([col("CS_QUANTITY"),col("CS_WHOLESALE_COST").desc()],ascending=[1,1]).select(col("CS_ORDER_NUMBER"),col("CS_QUANTITY"),col("CS_WHOLESALE_COST"),col("CS_LIST_PRICE"))
#df_sort2.show()

#.. Using AGG Method to aggregate the data
#.. https://docs.snowflake.com/ko/developer-guide/snowpark/reference/python/api/snowflake.snowpark.DataFrame.agg.html#snowflake.snowpark.DataFrame.agg
df_sum1 = df_tbl_read.agg(f.sum("CS_QUANTITY"))
df_stddev1 = df_tbl_read.agg(f.stddev("CS_QUANTITY"))
df_count1 = df_tbl_read.agg(f.count("CS_QUANTITY"))
df_max_min1 = df_tbl_read.agg(("CS_QUANTITY","min"),("CS_WHOLESALE_COST","max"))
df_max_min2 = df_tbl_read.agg({"CS_QUANTITY":"min",
                              "CS_WHOLESALE_COST":"max"})
#df_sum1.show() -- using f.<>
#df_stddev1.show() -- using f.<>
#df_count1.show() -- using f.<>
#df_max_min1.show() -- using Tuple()
#df_max_min2.show() -- using Dict{}

#.. Using Group_by for grouping aggregate results
#.. https://docs.snowflake.com/ko/developer-guide/snowpark/reference/python/api/snowflake.snowpark.DataFrame.group_by.html#snowflake.snowpark.DataFrame.group_by
df_group = df_tbl_read.group_by("CS_SOLD_DATE_SK").agg((f.sum("CS_QUANTITY").alias("QTY_SUM")),f.stddev("CS_WHOLESALE_COST").alias("STD_Dev"))
#df_group.show()

#.. Using Window Method as Window Function
#.. https://docs.snowflake.com/ko/developer-guide/snowpark/reference/python/api/snowflake.snowpark.Window.html#snowflake.snowpark.Window
df_window = df_tbl_read.with_column("Rank", row_number().over(Window.order_by(col("CS_SOLD_DATE_SK").desc()))).select(col("Rank"),col("CS_ORDER_NUMBER"),col("CS_QUANTITY"),col("CS_WHOLESALE_COST"),col("CS_LIST_PRICE"))
#df_window.show()

#.. Using DataFrame NA Function for Handling Missing Values
#.. https://docs.snowflake.com/ko/developer-guide/snowpark/reference/python/api/snowflake.snowpark.DataFrameNaFunctions.html#snowflake.snowpark.DataFrameNaFunctions
df_null_handling = df_tbl_read.filter((col("CS_QUANTITY").isNull()) & (col("CS_WHOLESALE_COST").isNull())).sort([col("CS_QUANTITY"),col("CS_WHOLESALE_COST").desc()],ascending=[1,1]).select(col("CS_ORDER_NUMBER"),col("CS_QUANTITY"),col("CS_WHOLESALE_COST"),col("CS_LIST_PRICE"))
#df_null_handling.na.fill({"CS_QUANTITY":111}).show()

# %%
#.. Performing Actions on Snowpark Dataframe

#.. Using Collect() Method to generate array of rows from a query
#df_tbl_read.collect()

#.. Using Collect_NoWait() to Execute Query Asynchronously
df_async = df_tbl_read.collect_nowait()
#df_async.result()

#.. Using Show Method to print the result
#df_tbl_read.show(5)



# %%
#Closing the Established Connection
conn.close()


