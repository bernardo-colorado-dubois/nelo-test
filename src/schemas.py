from pyspark.sql.types import (
  ArrayType,
  DoubleType,
  LongType,
  StringType,
  StructField,
  StructType,
)

ITEM_PARAM_VALUE_SCHEMA = StructType([
  StructField("string_value", StringType(), True),
  StructField("int_value", LongType(), True),
  StructField("float_value", DoubleType(), True),
  StructField("double_value", DoubleType(), True),
])

ITEM_PARAM_SCHEMA = StructType([
  StructField("key", StringType(), True),
  StructField("value", ITEM_PARAM_VALUE_SCHEMA, True),
])

ITEM_SCHEMA = StructType([
  StructField("item_id", StringType(), True),
  StructField("item_name", StringType(), True),
  StructField("item_brand", StringType(), True),
  StructField("item_variant", StringType(), True),
  StructField("item_category", StringType(), True),
  StructField("item_category2", StringType(), True),
  StructField("item_category3", StringType(), True),
  StructField("item_category4", StringType(), True),
  StructField("item_category5", StringType(), True),
  StructField("price_in_usd", DoubleType(), True),
  StructField("price", DoubleType(), True),
  StructField("quantity", LongType(), True),
  StructField("item_revenue_in_usd", DoubleType(), True),
  StructField("item_revenue", DoubleType(), True),
  StructField("item_refund_in_usd", DoubleType(), True),
  StructField("item_refund", DoubleType(), True),
  StructField("coupon", StringType(), True),
  StructField("affiliation", StringType(), True),
  StructField("location_id", StringType(), True),
  StructField("item_list_id", StringType(), True),
  StructField("item_list_name", StringType(), True),
  StructField("item_list_index", StringType(), True),
  StructField("promotion_id", StringType(), True),
  StructField("promotion_name", StringType(), True),
  StructField("creative_name", StringType(), True),
  StructField("creative_slot", StringType(), True),
  StructField("item_params", ArrayType(ITEM_PARAM_SCHEMA), True),
])

BODY_SCHEMA = StructType([
  StructField("event_timestamp", LongType(), True),
  StructField("user_id", StringType(), True),
  StructField("event_name", StringType(), True),
  StructField("platform", StringType(), True),
  StructField("replay_timestamp", StringType(), True),
  StructField("items", ArrayType(ITEM_SCHEMA), True),
])

RECORD_SCHEMA = StructType([
  StructField("message_id", StringType(), True),
  StructField("received_at", StringType(), True),
  StructField("body", BODY_SCHEMA, True),
])

EVENT_FIELDS = ["event_timestamp", "user_id", "event_name", "platform", "replay_timestamp"]

ITEM_FIELDS = [f.name for f in ITEM_SCHEMA.fields if f.name != "item_params"]

CATEGORY_FIELD_CANDIDATES = ["item_category", "item_category2", "item_list_name"]
