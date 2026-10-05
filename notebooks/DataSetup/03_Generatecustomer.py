
%pip install faker
dbutils.library.restartPython()

from faker import Faker

import random
import datetime
from pyspark.sql.types import *

schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("date_of_birth", DateType(), True),
    StructField("email", StringType(), True),
    StructField("phone_number", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("country", StringType(), True),
    StructField("customer_segment", StringType(), True),
    StructField("registration_date", DateType(), True),
    StructField("is_active", BooleanType(), True),
    StructField("created_timestamp", TimestampType(), True)
])

fake = Faker("en_AU")

segments = [
    "Regular",
    "Premium",
    "VIP"
]

rows=[]

for i in range(1000):

    customer_id = f"CUS{i+1:06d}"

    first_name = fake.first_name()
    last_name = fake.last_name()

    email = fake.email()

    gender = random.choice(
        ["Male","Female"]
    )

    city = fake.city()

    state = random.choice(
        ["VIC","NSW","QLD"]
    )

    country = "Australia"

    customer_segment = random.choice(segments)

    registration_date = fake.date_between(
        start_date="-5y",
        end_date="today"
    )

    is_active = random.choice(
        [True,True,True,False]
    )

    created_timestamp = datetime.datetime.now()

    rows.append(
        (
            customer_id,
            first_name,
            last_name,
            gender,
            fake.date_of_birth(),
            email,
            fake.phone_number(),
            city,
            state,
            country,
            customer_segment,
            registration_date,
            is_active,
            created_timestamp
        )
    )

df = spark.createDataFrame(
    data=rows,
    schema=schema
)

df.write.format("csv").mode("overwrite").option("header",True).save("/Volumes/retailsales/bronze/data/customers/")
