from pyspark.sql import SparkSession, functions as F, types as T
import re

spark = (SparkSession.builder.master("local[*]").appName("clean_pipeline").getOrCreate())

PATH, ENC = "DATA_BINH.csv", "UTF-8"
raw = spark.read.option("header", True).option("encoding", ENC).csv(PATH)

CAT_MAP = {
    "cleaning":"Cleaning","blocks":"Blocks","makeup":"Makeup","skincare":"Skincare",
    "wearables":"Wearables","phones":"Phones","audio":"Audio","cameras":"Cameras",
    "laptops":"Laptops","accessories":"Accessories","board games":"Board Games",
    "furniture":"Furniture","home decor":"Home Decor",
    "toys":"Toys","toy":"Toys","toyz":"Toys","team sports":"Team Sports","rc":"Rc"
}
DEPT_MAP = {
    "beauty care":"Beauty Care","sports outdoors":"Sports & Outdoors","sports & outdoors":"Sports & Outdoors",
    "home living":"Home & Living","home & living":"Home & Living",
    "electronic":"Electronic","electronics":"Electronic",
    "fashion":"Fashion","fashon":"Fashion","fasion":"Fashion","fasho":"Fashion",
    "toys":"Toys","toyz":"Toys"
}
def mexpr(d): return F.create_map([x for kv in d.items() for x in (F.lit(kv[0]), F.lit(kv[1]))])
cat_m, dept_m = mexpr(CAT_MAP), mexpr(DEPT_MAP)

def norm_col(c, mp):
    x = F.lower(F.trim(F.col(c)))
    x = F.regexp_replace(x, r"[_/\\-]", " ")
    x = F.regexp_replace(x, r"&", " and ")
    x = F.regexp_replace(x, r"[^a-z ]", " ")
    x = F.regexp_replace(x, r"\band\b", " ")
    x = F.regexp_replace(x, r"\s+", " ")
    miss = (x.isNull() | (x=="") | x.isin("null","none","na","n/a","-","—"))
    fb = F.initcap(x)
    fb = F.when(fb=="Home Living","Home & Living").when(fb=="Sports Outdoors","Sports & Outdoors").otherwise(fb)
    return F.when(miss, F.lit(None)).otherwise(F.coalesce(mp[x], fb))

@F.udf(T.DoubleType())
def money_strict(v):
    if v is None: return None
    s = str(v).strip().lower()
    if s in ("","-","—","null","none","n/a"): return None
    neg = s.startswith("(") and s.endswith(")")
    if neg: s = s[1:-1]
    mult = 1
    if s.endswith("k"): mult,s = 1000,s[:-1]
    elif s.endswith("m"): mult,s = 1000000,s[:-1]
    elif s.endswith("b"): mult,s = 1000000000,s[:-1]
    s = re.sub(r"[^\d,.\-]","", s)
    if "," in s and "." in s:
        dec = "," if s.rfind(",")>s.rfind(".") else "."
        other = "." if dec=="," else ","
        s = s.replace(other,"").replace(dec,".")
    elif "," in s:
        p=s.split(","); s = "".join(p) if (len(p[-1])==3 and all(len(x)==3 for x in p[1:-1]) and 1<=len(p[0])<=3) else ("".join(p[:-1])+"."+p[-1])
    elif "." in s:
        p=s.split("."); s = "".join(p) if (len(p[-1])==3 and all(len(x)==3 for x in p[1:-1]) and 1<=len(p[0])<=3) else ("".join(p[:-1])+"."+p[-1])
    if s.count(".")>1:
        i=s.rfind("."); s=s[:i].replace(".","")+"."+s[i+1:]
    try:
        val = float(s)*mult
        if neg: val=-val
        return round(val,2)
    except: return None

sdf = (raw
    .withColumn("lp_usd", F.lower(F.col("list_price").cast("string")).rlike(r"\$|usd"))
    .withColumn("up_usd", F.lower(F.col("unit_price").cast("string")).rlike(r"\$|usd"))
    .withColumn("category_name",   norm_col("category_name", cat_m))
    .withColumn("department_name", norm_col("department_name", dept_m))
    .withColumn("list_price", money_strict(F.col("list_price")).cast(T.DecimalType(12,2)))
    .withColumn("unit_price", money_strict(F.col("unit_price")).cast(T.DecimalType(12,2)))
)

final_num = sdf.select("category_name","department_name","list_price","unit_price")
final_num.coalesce(1).write.mode("overwrite").option("header",True).csv("out/clean_num_no_fx")

final_2dp = (sdf.select(
    "category_name","department_name",
    F.when(F.col("lp_usd"),
           F.concat(F.format_string('%.2f',F.col("list_price").cast("double")), F.lit(" VND")))
     .otherwise(F.format_string('%.2f',F.col("list_price").cast("double"))).alias("list_price"),
    F.when(F.col("up_usd"),
           F.concat(F.format_string('%.2f',F.col("unit_price").cast("double")), F.lit(" VND")))
     .otherwise(F.format_string('%.2f',F.col("unit_price").cast("double"))).alias("unit_price")
))
final_2dp.coalesce(1).write.mode("overwrite").option("header",True).csv("out/clean_2dp_with_VND")

final_num.show(20, truncate=False)
print("rows_in =", raw.count(), "| rows_out =", final_num.count())