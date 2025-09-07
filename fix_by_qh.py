import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import udf, when, col, trim, lower
from pyspark.sql.types import StringType
from dateutil.parser import parse
import pandas as pd
from datetime import datetime

# 1. Khởi tạo SparkSession
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("test") \
    .getOrCreate()
df = spark.read \
    .option('header','true') \
    .csv('day_1_raw.csv')
# ----------------- XỬ LÝ customer_gender -----------------
def fix_col_gender(s):
    if s is None:
        return 'U'
    else:
        return str(s)[0].upper()

fix_col_gender_udf = udf(fix_col_gender, StringType())
df = df.withColumn("customer_gender", fix_col_gender_udf(col("customer_gender")))
# df = df.fillna({"customer_gender": "U"})

df.select("customer_gender").show(10)
# ----------------- XỬ LÝ customer_region -----------------
cac_tinh = { 
    "hcmc": "TP Hồ Chí Minh",
    "can tho": "Cần Thơ",
    "hai phong": "Hải Phòng",
    "ha noi": "Hà Nội",
    "hanoi": "Hà Nội",
    "hà nội": "Hà Nội",
    "đà nẵng": "Đà Nẵng"
}

def fix_col_region(s):
    if s is None:
        return "Hà Nội"
    a = str(s).strip().lower()
    if a in ["", "na","nan"]:
        return "Hà Nội"
    return cac_tinh.get(a, a.title())
fix_col_region_udf = udf(fix_col_region, StringType())
df = df.withColumn("customer_region", fix_col_region_udf(col("customer_region")))
df = df.fillna({"customer_region": "Hà Nội"})
df.select("customer_region").show(10, truncate=False)
# ----------------- XỬ LÝ customer_signup_date -----------------
def fix_col_signup_date(s):
    try:
        s1 = float(s)
        return (pd.to_datetime('1899-12-30') + pd.to_timedelta(int(s1), unit='D')).strftime('%Y/%m/%d')
    except:
        try:
            return parse(s).strftime("%Y/%m/%d")
        except:
            return None

fix_col_signup_date_udf = udf(fix_col_signup_date, StringType())
df = df.withColumn("customer_signup_date", fix_col_signup_date_udf(col("customer_signup_date")))
df.select("customer_signup_date").show(10, truncate=False)
# ----------------- XỬ LÝ store_name -----------------
def fix_col_store_name(s):
    return str(s).strip().title() if s is not None else "U"

fix_col_store_name_udf = udf(fix_col_store_name, StringType())
df = df.withColumn("store_name", fix_col_store_name_udf(col("store_name")))
df.select("store_name").show(10, truncate=False)
# ----------------- XỬ LÝ store_id -----------------
def fix_col_store_id(s):
    return str(s).strip().title() if s is not None else "U"

fix_col_store_id_udf = udf(fix_col_store_id, StringType())
df = df.withColumn("store_id", fix_col_store_id_udf(col("store_id")))
df.select("store_id").show(10, truncate=False)
df_qh = df.select("customer_gender", "customer_region", "customer_signup_date", "store_id", "store_name").toPandas()

df_qh.to_excel("fix_columns_by_qh.xlsx", index=False, engine="openpyxl")

# # xuat file
# print("Xuất file thành công:\n", df_qh)
# from IPython.display import FileLink
# FileLink("fix_columns_by_qh.xlsx")
