import pyspark
from pyspark.sql import SparkSession, functions as F, types
from word2number import w2n # automatically turn words into number
import re

spark = SparkSession.builder \
        .master("local[*]") \
        .appName('test') \
        .getOrCreate()

df = spark.read \
      .option("header", True) \
      .option("quote", '"') \
      .option("escape", '"') \
      .csv('day_7_raw.csv')

# coalesce parse the first non-null column of a row
ts1 = F.coalesce(
    F.to_timestamp("order_datetime", "yyyy-MM-dd"),
    F.to_timestamp("order_datetime", "yyyy/MM/dd"),
    F.to_timestamp("order_datetime", "dd/MM/yyyy"),
    F.to_timestamp("order_datetime", "MM-dd-yyyy"),
    F.to_timestamp("order_datetime", "dd-MMM-yyyy"),
)

ts2 = F.coalesce(
    F.to_timestamp("customer_signup_date", "yyyy-MM-dd"),
    F.to_timestamp("customer_signup_date", "yyyy/MM/dd"),
    F.to_timestamp("customer_signup_date", "dd/MM/yyyy"),
    F.to_timestamp("customer_signup_date", "MM-dd-yyyy"),
    F.to_timestamp("customer_signup_date", "dd-MMM-yyyy"),
)

@F.udf(types.StringType())
def customer_email_udf(id):
    return f'user{id[1:]}@example.com'

@F.udf(types.FloatType())
def discount_udf(value):
    if value is None:
        return float(0)
    value = value.strip()
    if '%' in value:
        return float(int(value[:-1])/100)
    elif value=='None':
        return float(0)
    elif value.isnumeric():
        return float(int(value)/100)
    return float(value)

@F.udf(types.IntegerType())
def qty_udf(value):
    try:
        value = int(value)
        if value<0:
            return None
        return value
    except:
        if value is None:
            return None
        return w2n.word_to_num(value.strip().lower())

@F.udf(types.StringType())
def order_status_udf(value):
    if value is None:
        return None
    return value.title()

@F.udf(types.StringType())
def get_color_udf(attributes):
    if attributes is None:
        return None

    attributes = attributes.replace(':', ' ') \
            .replace('{', ' ') \
            .replace(r'"', ' ') \
            .replace(':', ' ') \
            .replace(',', ' ') \
            .replace('}', ' ')
    color_regex = r'(?P<color>\bcolor\s*\w*\s*weight\s*)'
    l = re.search(color_regex, attributes).group('color').split()
    if len(l)>2:
        return l[1].title()
    return None

@F.udf(types.StringType())
def get_weight_g_udf(attributes):
    if attributes is None:
        return None
    attributes = attributes.replace(':', ' ') \
                .replace('{', ' ') \
                .replace(r'"', ' ') \
                .replace(':', ' ') \
                .replace(',', ' ') \
                .replace('}', ' ')

    num_match = re.search(r'(?P<num>\d+([.]?)\d+)', attributes) # whole and decimal number
    kg_match = re.search(r'(\bkg\b|\d+kg\b)', attributes, flags=re.IGNORECASE)
    if num_match:
        if kg_match:
            return float(num_match.group('num')) * 1000
        return float(num_match.group('num'))
        
    else: # number as words
        # naw_regex = r'(?P<naw>\bweight\w*\s*\w*\s*kg\b)'
        # naw_match = re.search(naw_regex, attributes)
        # if naw_match:
        #     if kg_match:
        #         try:
        #             return w2n.word_to_num(naw_match.group('naw')) * 1000
        #         except:
        #             return None
        #     return w2n.word_to_num(l[1])
        try:
            if kg_match:
                return w2n.word_to_num(attributes) * 1000
            return w2n.word_to_num(attributes)
        except:
            return None

aliases = {
    "home decor": "decor",
    "boardgames": "board games",
    "board game": "board games",
    "team sport": "team sports",
    "hair care": "haircare",
    "wearable": "wearables",
    "phone": "phones",
    "laptop": "laptops",
    "audio & video": "audio",
    "rc car": "rc",
    "rc cars": "rc",
    "rc toys": "rc",
    "sport": "team sports",
}

def normalize_token(x: str) -> str:
    try:
        if x is None:
            return None
        s = str(x).lower().strip()
        s = re.sub(r"[_\-\/]+", " ", s)
        s = re.sub(r"[^a-z0-9 ]+", "", s)
        s = re.sub(r"\s+", " ", s).strip()
        return s
    except:
        return None

@F.udf(types.StringType())
def normalize_category_udf(value: str) -> str:
    if value is None:
        return None
    norm = normalize_token(value)
    # Áp dụng alias trước
    if norm in aliases:
        norm = aliases[norm]
    return norm.capitalize()

@F.udf(returnType = types.DoubleType())
def fix_price_udf(s):
    if s is None:
        return None

    st = str(s).strip().lower()
    if st in ("", "-", "_", "null", "none", "n/a"):
        return None

    neg = st.startswith("(") and st.endswith(")")
    if neg:
        st = st[1:-1]

    mult = 1

    if st.endswith("k"):
        mult = 1000
        st = st[:-1]
    elif st.endswith("m"):
        mult = 1_000_000
        st = st[:-1]
    elif st.endswith("b"):
        mult = 1_000_000_000
        st = st[:-1]

    # Nếu có dấu $, đổi sang VNĐ
    usd_to_vnd = 25000
    if "$" in st:
        mult = mult * usd_to_vnd
        st = st.replace("$", "")

    st = re.sub(r"[^\d,\.-]", "", st)

    if "," in st and "." in st:
        # Nếu dấu ',' xuất hiện sau '.' → ',' là phần thập phân
        dec = "," if st.rfind(",") > st.rfind(".") else "."
        other = "." if dec == "," else ","
        st = st.replace(other, "").replace(dec, ".")
    elif " " in st:
        parts = st.split(" ")
        is_thou = (len(parts[-1]) == 3 and all(len(p) == 3 for p in parts[1:-1])
                   and 1 <= len(parts[0]) <= 3)
        st = "".join(parts) if is_thou else "".join(parts[:-1]) + "." + parts[-1]
    elif "." in st:
        parts = st.split(".")
        is_thou = (len(parts[-1]) == 3 and all(len(p) == 3 for p in parts[1:-1])
                   and 1 <= len(parts[0]) <= 3)
        st = "".join(parts) if is_thou else "".join(parts[:-1]) + "." + parts[-1]

    if st.count(".") > 1:
        i = st.rfind(".")
        st = st[:i].replace(".", "") + "." + st[i+1:]

    try:
        v = float(st) * mult
        if neg:
            v = -v
        return round(v, 2)
    except:
        return None

@F.udf(returnType = types.StringType())
def fix_gender_udf(s):
    if s is None:
        return 'U'
    else:
        return str(s)[0].upper()

regions = { 
    'Hà Nội': 'Hanoi',
    'hanoi' : 'Hanoi',
    'Đà Nẵng' : 'Danang',
    'Đà nẵng' : 'Danang',
    'HCMC' : 'Ho Chi Minh City'
}

@F.udf(types.StringType())
def fix_customer_region_udf(s):
    if s is None or s=='NA':
        return "Unknown"
    s = s.strip()
    if s in regions.keys():
        return regions[s].title()
    return s.title()

@F.udf(returnType = types.StringType())
def fix_store_name_udf(s):
    if s is None:
        return 'Unknown'
    return s.strip().title() 

@F.udf(returnType = types.FloatType())
def store_rating_udf(value):
    if value is None:
        return float(0)
    value = value.replace(',', '.')

    if value.isalpha():
        return float(w2n.word_to_num(value))
    return float(value)

@F.udf(types.StringType())
def brand_udf(value):
    if (value is None) or (value.strip()=='—'):
        return 'Uknown'
    return value.strip().title()

condition = F.bround(F.col('list_price') - F.col('qty')*F.col('unit_price')*(1-F.col('discount')), 2)>=0
df = df.withColumn('order_id', F.upper(F.btrim(F.col('order_id')))) \
    .withColumn('order_line_id', F.upper(F.btrim(F.col('order_line_id')))) \
    .withColumn('order_datetime', F.to_date(ts1)) \
    .withColumn('customer_signup_date', F.to_date(ts2)) \
    .withColumn('coupon_code', F.when(F.col('coupon_code').isNotNull(), F.btrim(F.initcap(df.coupon_code))).otherwise(None)) \
    .withColumn('customer_id', F.btrim(F.col('customer_id'))) \
    .withColumn('customer_full_name', F.btrim(F.initcap(F.col('customer_full_name')))) \
    .withColumn('customer_email', customer_email_udf(F.col('customer_id'))) \
    .withColumn('discount', discount_udf(F.col('discount'))) \
    .withColumn('qty', qty_udf(F.col('qty'))) \
    .withColumn('color', get_color_udf(F.col('attributes'))) \
    .withColumn('weight_g', get_weight_g_udf(F.col('attributes'))) \
    .withColumn('category_name', normalize_category_udf(F.col('category_name'))) \
    .withColumn("customer_region", fix_customer_region_udf(F.col("customer_region"))) \
    .withColumn("customer_gender", fix_gender_udf(F.col("customer_gender"))) \
    .withColumn("store_name", fix_store_name_udf(F.col("store_name"))) \
    .withColumn("list_price", F.bround(fix_price_udf(F.col("list_price")), 2)) \
    .withColumn("unit_price", F.bround(fix_price_udf(F.col("unit_price")), 2)) \
    .withColumn('line_amount', F.when(condition, F.bround(F.col('list_price') - F.col('qty')*F.col('unit_price')*(1-F.col('discount')), 2)).otherwise(F.lit(None))) \
    .withColumn('store_region', F.when(F.col('store_region').isNotNull(), F.btrim(F.initcap(F.col('store_region')))).otherwise('Unknown')) \
    .withColumn('store_rating', store_rating_udf(F.col('store_rating'))) \
    .withColumn('product_name', F.when(F.col('product_name').isNotNull(), F.btrim(F.initcap(F.col('product_name')))).otherwise('Unknown')) \
    .withColumn('product_sku', F.when(F.col('product_sku').isNotNull(), F.btrim(F.upper(F.col('product_sku')))).otherwise('Unfilled')) \
    .withColumn('store_name', F.when(F.col('store_name').isNotNull(), F.btrim(F.initcap(F.col('store_name')))).otherwise('Unknown')) \
    .withColumn('store_id', F.upper(F.btrim(df.store_id))) \
    .withColumn("brand", F.when(F.col("brand").isNull() | (F.trim(F.col("brand")) == '—') | (F.trim(F.col("brand")) == ''), "Unknown").otherwise(F.initcap(F.trim(F.col("brand"))))) \
    .withColumn('order_status', order_status_udf(F.col('order_status')))

df = df.drop('department_name','attributes')

order_id_invalid = df.filter(F.length(df.order_id)!=7).count()
order_line_id_invalid = df.filter(F.length(df.order_line_id)!=9).count()
customer_id_invalid = df.filter(F.length(df.customer_id)!=7).count()
store_id_invalid = df.filter(F.length(df.store_id)!=5).count()
if order_id_invalid >0:
     raise ValueError(f'{order_id_invalid} invalid values in order_id')
if order_line_id_invalid >0:
    raise ValueError(f'{order_line_id_invalid} invalid values in order_line_id')
if customer_id_invalid>0:
    raise ValueError(f'{customer_id_invalid} invalid values in customer_id')
if store_id_invalid>0:
    raise ValueError(f'{store_id_invalid} invalid values in store_id')