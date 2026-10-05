%pip install faker

import random
import datetime
from decimal import Decimal

from faker import Faker
from pyspark.sql.types import *

fake = Faker()


sales_raw_schema = StructType([
    StructField("sales_id", StringType(), False),
    StructField("transaction_date", TimestampType(), False),
    StructField("store_id", StringType(), False),
    StructField("customer_id", StringType(), False),

    StructField(
        "items",
        ArrayType(
            StructType([
                StructField("product_id", StringType()),
                StructField("quantity", IntegerType()),
                StructField("unit_price", DecimalType(10,2)),
                StructField("discount_amount", DecimalType(10,2)),
                StructField("tax_amount", DecimalType(10,2)),
                StructField("total_amount", DecimalType(10,2))
            ])
        )
    ),

    StructField("payment_method", StringType(), False),
    StructField("created_timestamp", TimestampType(), False),
    StructField("flag", BooleanType(), False),
])


# Read dimension data

products_df = spark.read \
    .format("parquet") \
    .load("/Volumes/retailsales/bronze/data/products/")


stores_df = spark.read \
    .format("json") \
    .option("multiline", True) \
    .load("/Volumes/retailsales/bronze/data/stores/")


customers_df = spark.read \
    .format("csv") \
    .option("header", True) \
    .load("/Volumes/retailsales/bronze/data/customers/")


# Lookup containers

store_ids = []
customer_ids = []
product_lookup = {}


# Collect dimension values

for row in products_df.select(
    "product_id",
    "selling_price"
).collect():

    product_lookup[row.product_id] = {
        "selling_price": row.selling_price,
        "tax_rate": Decimal("0.05")
    }


for row in stores_df.select("store_id").collect():
    store_ids.append(row.store_id)


for row in customers_df.select("customer_id").collect():
    customer_ids.append(row.customer_id)


# Faster product selection

product_ids = list(product_lookup.keys())


# Batch generation

batch_size = 10
total_batches = 1


sales_path = "/Volumes/retailsales/bronze/data/Sales/"


for batch in range(total_batches):

    rows = []

    for i in range(batch_size):

        sales_id = f"SALE{batch * batch_size + i + 1:08d}"


        transaction_date = fake.date_time_between(
            start_date=datetime.datetime(2020,1,1),
            end_date=datetime.datetime(2025,12,31)
        )


        store_id = random.choice(store_ids)

        customer_id = random.choice(customer_ids)


        # reset for every transaction
        items = []


        number_of_items = random.randint(1,3)


        for j in range(number_of_items):

            product_id = random.choice(product_ids)


            quantity = random.randint(1,10)


            unit_price = product_lookup[product_id]["selling_price"]


            discount_percentage = random.choice(
                [
                    Decimal("0"),
                    Decimal("0.05"),
                    Decimal("0.10"),
                    Decimal("0.15")
                ]
            )


            discount_amount = (
                unit_price *
                discount_percentage
            )


            subtotal = (
                unit_price * quantity
            ) - discount_amount


            tax_amount = (
                subtotal *
                product_lookup[product_id]["tax_rate"]
            )


            total_amount = (
                subtotal +
                tax_amount
            )


            items.append(
                {
                    "product_id": product_id,
                    "quantity": quantity,
                    "unit_price": unit_price,
                    "discount_amount": discount_amount,
                    "tax_amount": tax_amount,
                    "total_amount": total_amount
                }
            )


        payment_method = random.choice(
            [
                "credit_card",
                "debit_card",
                "cash"
            ]
        )


        created_timestamp = datetime.datetime.now()
        flag=True


        rows.append(
            (
                sales_id,
                transaction_date,
                store_id,
                customer_id,
                items,
                payment_method,
                created_timestamp,flag
            )
        )


    # Create batch dataframe

    batch_df = spark.createDataFrame(
        rows,
        sales_raw_schema
    )


    # Write each batch separately
    # simulates new files arriving

    batch_df.write \
        .format("json") \
        .mode("overwrite") \
        .save(
            f"{sales_path}batch_{batch}"
        )


    print(f"Completed batch {batch}")