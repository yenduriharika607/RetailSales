import random
from pyspark.sql.types import *
import datetime
from decimal import Decimal

products_master = {
    "Electronics": {
        "brands": [
            "Apple",
            "Samsung",
            "Sony",
            "Dell",
            "HP",
            "Logitech"
        ],
        "subcategories": {
            "Laptop": {
                "products": [
                    "Laptop",
                    "Gaming Laptop",
                    "Business Laptop"
                ],
                "price_range": (700, 2500)
            },
            "Accessories": {
                "products": [
                    "Wireless Mouse",
                    "Mechanical Keyboard",
                    "USB-C Hub",
                    "Webcam"
                ],
                "price_range": (20, 250)
            },
            "Monitor": {
                "products": [
                    "24 Inch Monitor",
                    "27 Inch Monitor",
                    "Ultrawide Monitor"
                ],
                "price_range": (150, 800)
            }
        }
    },

    "Clothing": {
        "brands": [
            "Nike",
            "Adidas",
            "Puma"
        ],
        "subcategories": {
            "Men": {
                "products": [
                    "T-Shirt",
                    "Hoodie",
                    "Jeans"
                ],
                "price_range": (20, 150)
            },
            "Women": {
                "products": [
                    "Leggings",
                    "Jacket",
                    "Sneakers"
                ],
                "price_range": (25, 180)
            }
        }
    },

    "Home": {
        "brands": [
            "IKEA",
            "Dyson",
            "Philips"
        ],
        "subcategories": {
            "Kitchen": {
                "products": [
                    "Coffee Maker",
                    "Air Fryer",
                    "Blender"
                ],
                "price_range": (50, 400)
            },
            "Furniture": {
                "products": [
                    "Office Chair",
                    "Dining Table",
                    "Bookshelf"
                ],
                "price_range": (80, 1200)
            }
        }
    }
}

rows=[]

products_generated=set()


product_schema = StructType([
    StructField("product_id", StringType(), False),
    StructField("sku", StringType(), False),
    StructField("product_name", StringType(), False),
    StructField("category", StringType(), False),
    StructField("subcategory", StringType(), False),
    StructField("brand", StringType(), False),
  #  StructField("supplier", StringType(), False),
    StructField("cost_price", DecimalType(10,2), False),
    StructField("selling_price", DecimalType(10,2), False),
    StructField("tax_rate", DecimalType(5,2), False),
    StructField("weight_kg", DecimalType(6,2), True),
    StructField("is_active", BooleanType(), False),
    StructField("created_date", DateType(), False),
    StructField("last_updated", TimestampType(), False)
])



for i in range(90):
    productid=f"PRD{i+1:04d}"
    sku=f"SKU{i+1:04d}"
    category= random.choice(list(products_master.keys()))
    subcategory=random.choice(list(products_master[category]["subcategories"].keys()))
    product=random.choice(products_master[category]["subcategories"][subcategory]
    ["products"])
    brand=random.choice(products_master[category]["brands"])
    product=brand+" "+product
    while product in products_generated:
        category= random.choice(list(products_master.keys()))
        subcategory=random.choice(list(products_master[category]["subcategories"].keys()))
        product=random.choice(products_master[category]["subcategories"][subcategory]
    ["products"])
        brand=random.choice(products_master[category]["brands"])
        product=brand+" "+product

    products_generated.add(product)
    min_price, max_price = products_master[category]["subcategories"][subcategory]["price_range"]
    cost_price = Decimal(str(random.randint(min_price, max_price)))
    selling_price=Decimal(cost_price+50)
    weight_kg=Decimal(str(round(random.uniform(0.1,10), 2)))
    is_active=True
    created_date=datetime.date.today()
    last_updated=datetime.datetime.now()
    row=(productid,sku,product,category,subcategory,brand,cost_price,selling_price,Decimal("0.05"),weight_kg,is_active,created_date,last_updated)
    rows.append(row)

df=spark.createDataFrame(rows,schema=product_schema)

df.coalesce(1).write.format("parquet").mode("append").save("/Volumes/retailsales/bronze/data/products/")




   
   

 
