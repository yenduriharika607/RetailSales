from pyspark.sql.types import *
import random,uuid
import datetime
from pyspark.sql.functions import *

state_list=["vic","nsw","qld"]
city_list={"vic":["melbourne","geelong"],"nsw":["Wollongong","sydney"],"qld":["brisbane","perth"]}
region_list=["east","west","south","north"]
store_type_list=["retail","online","warehouse"]
store_name_list=["central","cbd","airport","mall"]

columns=["store_id","store_name","state","city","region","store_type","store_name","Country","OpenDate","ManagerID","IsActive"]
rows=[]

schema=StructType([
    StructField("store_id",StringType(),True),
    StructField("store_name",StringType(),True),
    StructField("state",StringType(),True),
    StructField("city",StringType(),True),
    StructField("region",StringType(),True),
    StructField("store_type",StringType(),True),
    StructField("Country",StringType(),True),
    StructField("OpenDate",DateType(),True),
    StructField("ManagerID",StringType(),True),
    StructField("IsActive",BooleanType(),True)
])

store_names = []
store_names_used=set()
store_names_deduplicated = []
for store_name in store_names:
    if store_name not in store_names_used:
        store_names_deduplicated.append(store_name)
        store_names_used.add(store_name)

for state in state_list:
    for city in city_list[state]:
        for location in store_name_list:
            store_names.append(city + " " + location)






for i in range(24):
    store_id=f"STR{i+1:03d}"
    state=random.choice(state_list)
    city=random.choice(city_list[state])
    region=random.choice(region_list)
    store_type=random.choice(store_type_list)
    Country="Australia"
    OpenDate=datetime.date(random.randint(2000,2020),random.randint(1,12),random.randint(1,28))
    ManagerID='Mgr'+str(store_id)
    IsActive=random.choice([True,False])
    store_name = random.choice(store_names)
    store_names_used.add(store_name)
    rows.append(
        (
        store_id,
        store_name,
        state,
        city,
        region,
        store_type,
        Country,
        OpenDate,
        ManagerID,
        IsActive
        )
    )
    
    
_sqldf=spark.createDataFrame(rows,schema)

    


nested_df=_sqldf.select("store_id",struct("state","city").alias("location"),"store_name",struct("ManagerID").alias("manager"),"Country","region","store_type","OpenDate","IsActive")
nested_df.write.format("json").mode("append").save("/Volumes/retailsales/bronze/data/stores/")
    
   





