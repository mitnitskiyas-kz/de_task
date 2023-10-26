"""Module defining structures for the task."""


from pyspark.sql.types import StructType, StructField, \
    StringType, IntegerType, TimestampType, DoubleType

olist_customers_dataset_schema = StructType([
    StructField('customer_id', StringType(), True),
    StructField('customer_unique_id', StringType(), True),
    StructField('customer_zip_code_prefix', StringType(), True),
    StructField('customer_city', StringType(), True),
    StructField('customer_state', StringType(), True)
    ])
olist_geolocation_dataset_schema = StructType([
    StructField('geolocation_zip_code_prefix', StringType(), True),
    StructField('geolocation_lat', DoubleType(), True),
    StructField('geolocation_lng', DoubleType(), True),
    StructField('geolocation_city', StringType(), True),
    StructField('geolocation_state', StringType(), True)
    ])

olist_order_items_dataset_schema \
    = StructType([StructField('order_id', StringType(), True),
                  StructField('order_item_id', IntegerType(), True),
                  StructField('product_id', DoubleType(), True),
                  StructField('seller_id', StringType(), True),
                  StructField('shipping_limit_date', TimestampType(), True),
                  StructField('price', DoubleType(), True),
                  StructField('freight_value', DoubleType(), True)
                  ])

olist_order_payments_dataset_schema \
    = StructType([StructField('order_id', StringType(), True),
                  StructField('payment_sequential', IntegerType(), True),
                  StructField('payment_type', StringType(), True),
                  StructField('payment_installments', IntegerType(), True),
                  StructField('payment_value', DoubleType(), True)
                  ])

olist_order_reviews_dataset_schema \
    = StructType([StructField('review_id', StringType(), True),
                  StructField('order_id', StringType(), True),
                  StructField('review_score', IntegerType(), True),
                  StructField('review_comment_title', StringType(), True),
                  StructField('review_comment_message', StringType(), True),
                  StructField('review_creation_date', TimestampType(), True),
                  StructField('review_answer_timestamp', TimestampType(), True)
                  ])


olist_orders_dataset_schema = StructType([
    StructField('order_id', StringType(), True),
    StructField('customer_id', StringType(), True),
    StructField('order_status', StringType(), True),
    StructField('order_purchase_timestamp', TimestampType(), True),
    StructField('order_approved_at', TimestampType(), True),
    StructField('order_delivered_carrier_date', TimestampType(), True),
    StructField('order_delivered_customer_date', TimestampType(), True),
    StructField('order_estimated_delivery_date', TimestampType(), True)
    ])

olist_products_dataset_schema = StructType([
    StructField('product_id', StringType(), True),
    StructField('product_category_name', StringType(), True),
    StructField('product_name_lenght', IntegerType(), True),
    StructField('product_description_lenght', IntegerType(), True),
    StructField('product_weight_g', IntegerType(), True),
    StructField('product_length_cm', IntegerType(), True),
    StructField('product_height_cm', IntegerType(), True)
    ])

olist_sellers_dataset_schema = StructType([
    StructField('seller_id', StringType(), True),
    StructField('seller_zip_code_prefix', StringType(), True),
    StructField('seller_city', StringType(), True),
    StructField('seller_state', StringType(), True)
    ])

product_category_name_translation_schema = StructType([
    StructField('product_category_name', StringType(), True),
    StructField('product_category_name_english', StringType(), True)
    ])
