# %% [markdown]
# # Snowflake Warehouse Reports Using Snowflake Snowpark

# %% [markdown]
# <blockquote>
#     Calling Configuration Notebook at #1
#     <br>Establishing Connection To Snowflake 
#     <br>Generating Warehouse Cost Matrix From Snowflake
#     <br>Sending an Customized Alert
# </blockquote>

# %%
# Importing Libraries for notebook
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns 
import matplotlib.ticker as ticker


# %%
#Calling Config notebook for Defining Snowpark Connection
%run ./sf_config.ipynb

# %%
#Establishing the snowflake snowpark connection
conn = Session.builder.configs(conn_config).create()

# %%
#Using Snowpark Session SQL To Query Warehouse Credits Consumption in last 30 Days

sp_warehouse_consumption = conn.sql("WITH CTE_DATE_WH AS(  \
  SELECT TO_DATE(START_TIME) AS START_DATE \
        ,WAREHOUSE_NAME \
        ,SUM(CREDITS_USED) AS CREDITS_USED_DATE_WH \
    FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY \
   GROUP BY START_DATE \
           ,WAREHOUSE_NAME \
) \
SELECT START_DATE \
      ,WAREHOUSE_NAME \
      ,round(CREDITS_USED_DATE_WH) AS CREDITS_USED_DATE_WH \
  FROM CTE_DATE_WH \
where CREDITS_USED_DATE_WH > 0  \
AND START_DATE > DATEADD(Month,-1,CURRENT_TIMESTAMP())")
sp_warehouse_consumption = conn.sql('select * from db_roles.public.wh_logs')
pd_warehouse_consumption = sp_warehouse_consumption.to_pandas()



# %%
sp_user_consumption = conn.sql("""WITH USER_HOUR_EXECUTION_CTE AS (
    SELECT  USER_NAME
    ,WAREHOUSE_NAME
    ,DATE_TRUNC('hour',START_TIME) as START_TIME_HOUR
    ,SUM(EXECUTION_TIME)  as USER_HOUR_EXECUTION_TIME
    FROM "SNOWFLAKE"."ACCOUNT_USAGE"."QUERY_HISTORY" 
    WHERE WAREHOUSE_NAME IS NOT NULL
    AND EXECUTION_TIME > 0
    AND START_TIME > DATEADD(Month,-1,CURRENT_TIMESTAMP())
    group by 1,2,3
    )
, HOUR_EXECUTION_CTE AS (
    SELECT  START_TIME_HOUR
    ,WAREHOUSE_NAME
    ,SUM(USER_HOUR_EXECUTION_TIME) AS HOUR_EXECUTION_TIME
    FROM USER_HOUR_EXECUTION_CTE
    group by 1,2
)
, APPROXIMATE_CREDITS AS (
    SELECT 
    A.USER_NAME
    ,C.WAREHOUSE_NAME
    ,(A.USER_HOUR_EXECUTION_TIME/B.HOUR_EXECUTION_TIME)*C.CREDITS_USED AS APPROXIMATE_CREDITS_USED

    FROM USER_HOUR_EXECUTION_CTE A
    JOIN HOUR_EXECUTION_CTE B  ON A.START_TIME_HOUR = B.START_TIME_HOUR and B.WAREHOUSE_NAME = A.WAREHOUSE_NAME
    JOIN "SNOWFLAKE"."ACCOUNT_USAGE"."WAREHOUSE_METERING_HISTORY" C ON C.WAREHOUSE_NAME = A.WAREHOUSE_NAME AND C.START_TIME = A.START_TIME_HOUR
)

SELECT 
 USER_NAME
,WAREHOUSE_NAME
,round(SUM(APPROXIMATE_CREDITS_USED),2) AS APPROXIMATE_CREDITS_USED
FROM APPROXIMATE_CREDITS
GROUP BY 1,2
ORDER BY 3 DESC;""")
pd_user_consumption = sp_user_consumption.to_pandas()
pd_user_consumption

# Generating HTML table for the Email Template from PD dataframe
html_user_consumption = ""
for index, row in pd_user_consumption.iterrows():
    html_user_consumption+="<tr><td>"+ str(row['USER_NAME']) + "</td><td>" + str(row['WAREHOUSE_NAME']) + "</td><td>" + str(row['APPROXIMATE_CREDITS_USED']) + "</td></tr>"
html_user_consumption

# %%
sp_queries_exec_time = conn.sql("""SELECT 
substr(QUERY_TEXT,0,25) as partial_query_text
,count(*) as number_of_queries
--,sum(TOTAL_ELAPSED_TIME)/1000 as execution_seconds
,sum(TOTAL_ELAPSED_TIME)/(1000*60) as execution_minutes
--,sum(TOTAL_ELAPSED_TIME)/(1000*60*60) as execution_hours

  from SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY Q
  where 1=1
  and TO_DATE(Q.START_TIME) >     DATEADD(month,-1,TO_DATE(CURRENT_TIMESTAMP())) 
 and TOTAL_ELAPSED_TIME > 0 --only get queries that actually used compute
  group by 1
  having count(*) >= 10 --configurable/minimal threshold
  order by 2 desc
  limit 20 --configurable upper bound threshold
  ;""")

pd_queries_exec_time = sp_queries_exec_time.to_pandas()
pd_queries_exec_time

# Generating HTML table for the Email Template from PD dataframe
html_queries_exec_time = ""
for index, row in pd_queries_exec_time.iterrows():
    html_queries_exec_time +="<tr><td>"+ str(row['PARTIAL_QUERY_TEXT']) + "</td><td>" + str(row['NUMBER_OF_QUERIES']) + "</td><td>" + str(row['EXECUTION_MINUTES']) + "</td></tr>"
html_queries_exec_time

# %%
df_pivot = pd.pivot_table(pd_warehouse_consumption
                          ,values='CREDITS_USED_DATE_WH'
                          ,index='WAREHOUSE_NAME'
                          ,columns='START_DATE')
df_pivot
plt.figure(figsize=(40,19))
plt.title('Warehouse Consumption Trend')
a = sns.heatmap(df_pivot,annot=True,cmap='RdYlBu_r',fmt='.4g',)
a.invert_xaxis()
a.invert_yaxis()
plt.xlabel('Date')
plt.ylabel('Warehouses')
plt.show()
a.figure.savefig('img.jpg',bbox_inches = 'tight')


# %%
#creating HTML Template for Email Alert

path1 = "img.jpg"

template_css_header = """
<html>
   <head>
         <style>
            table {
               border-collapse: collapse;
            }
            th, td {
               border: 1px solid black;
               padding: 5px;
               text-align: left;
            }
      </style>
   </head>
"""

template_end_footer = """
      </table>
      <br>
      <br>
      <hr/>
   </body>
</html>
"""

template_user_queries = """
<body>
      <H2>Snowpark Generated Snowflake Utilization Report</H2>
      <hr>
      <br>
      <h3>Approx. Users Credit Consumption Report</h3>
      <br>
      <table>
         <tr>
            <th>USER NAME</th>
            <th>WAREHOUSE NAME</th>
            <th>APPROX. CREDITS USED</th>
         </tr>
"""
dynamic_template_user_queries = template_user_queries + html_user_consumption

template_queries_exec_time = """
</table>
<br>
<h3>Queries by # Executed and their Execution Time</h3>
<br>
<table>
         <tr>
            <th>PARTIAL QUERY TEXT</th>
            <th>NUMBER OF QUERIES</th>
            <th>EXECUTION TIME IN MINS</th>
         </tr>
"""


dynamic_template_queries_exec_time = template_queries_exec_time + html_queries_exec_time

template_warehouse_usage = """
   </table>
   <br>
      <H3>Snowpark Generated Snowflake Utilization Report</H3>
      <IMG height=auto width=100% SRC = 'cid:{current_dupe_graph}' ></img>
      <br>
""".format(current_dupe_graph = path1 )


template = template_css_header + dynamic_template_user_queries + dynamic_template_queries_exec_time +template_warehouse_usage + template_end_footer



# %%
#Calling Config notebook for Defining Snowpark Connection
%run ./mail_config.ipynb

# %%
mail_subject = "Snowflake Utility Report"
to_list = "divyanshsaxenaofficial@gmail.com"
cc_list = "divyanshsaxenaofficial@gmail.com"
sender = "divyanshsaxenaofficial@gmail.com"
msg = email.message.EmailMessage()
msg = MIMEMultipart()
msg["To"] = to_list
msg["Cc"] = cc_list
msg["From"] = sender
msg["Subject"] = mail_subject
body = template
msgText = MIMEText(body, 'html')  
msg.attach(msgText)

#EMBEDDING THE PNG GRAPH IMAGES IN EMAIL
#**********************************************************************************************

with open(path1, 'rb') as fp:
    img = MIMEImage(fp.read())
img.add_header('Content-ID', '<{}>'.format(path1))
msg.attach(img)


#**********************************************************************************************

server = smtplib.SMTP('smtp.gmail.com', 587)
server.starttls()
server.SentOnBehalfOfName = sender_onBehalf
server.login(login_mail, login_credential)
server.send_message(msg)
server.quit()

# %%



