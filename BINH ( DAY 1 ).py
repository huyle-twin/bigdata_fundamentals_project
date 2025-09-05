from pyspark.sql import SparkSession, functions as F, types as T
import re

spark = (SparkSession.builder.master("local[*]").appName("clean_no_swap").getOrCreate())

PATH = "DATA_BINH.csv"
ENC  = "UTF-8"
sdf  = spark.read.option("header", True).option("encoding", ENC).csv(PATH)

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
    mapped = mp[x]
    fallback = F.initcap(x)
    fallback = F.when(fallback=="Home Living","Home & Living").when(fallback=="Sports Outdoors","Sports & Outdoors").otherwise(fallback)
    return F.when(miss, F.lit(None)).otherwise(F.coalesce(mapped, fallback))

@F.udf(T.DoubleType())
def money_strict(s):
    if s is None: return None
    st = str(s).strip().lower()
    if st in ("","-","—","null","none","n/a"): return None
    neg = st.startswith("(") and st.endswith(")")
    if neg: st = st[1:-1]
    mult = 1
    if st.endswith("k"): mult, st = 1000, st[:-1]
    elif st.endswith("m"): mult, st = 1000000, st[:-1]
    elif st.endswith("b"): mult, st = 1000000000, st[:-1]
    st = re.sub(r"[^\d,.\-]","", st)
    if "," in st and "." in st:
        dec = "," if st.rfind(",") > st.rfind(".") else "."
        other = "." if dec == "," else ","
        st = st.replace(other,"").replace(dec,".")
    elif "," in st:
        parts = st.split(",")
        is_thou = (len(parts[-1])==3 and all(len(p)==3 for p in parts[1:-1]) and 1<=len(parts[0])<=3)
        st = "".join(parts) if is_thou else ("".join(parts[:-1]) + "." + parts[-1])
    elif "." in st:
        parts = st.split(".")
        is_thou = (len(parts[-1])==3 and all(len(p)==3 for p in parts[1:-1]) and 1<=len(parts[0])<=3)
        st = "".join(parts) if is_thou else ("".join(parts[:-1]) + "." + parts[-1])
    if st.count(".")>1:
        i = st.rfind("."); st = st[:i].replace(".","") + "." + st[i+1:]
    try:
        v = float(st) * mult
        if neg: v = -v
        return round(v,2)
    except:
        return None

final = (sdf
    .withColumn("category_name",   norm_col("category_name", cat_m))
    .withColumn("department_name", norm_col("department_name", dept_m))
    .withColumn("list_price", money_strict(F.col("list_price")).cast(T.DecimalType(12,2)))
    .withColumn("unit_price", money_strict(F.col("unit_price")).cast(T.DecimalType(12,2)))
    .select("category_name","department_name","list_price","unit_price")
)

(final.coalesce(1).write.mode("overwrite").option("header", True).csv("out/clean_csv_no_swap"))
(final.select(
    "category_name","department_name",
    F.format_string('%.2f', F.col("list_price").cast("double")).alias("list_price"),
    F.format_string('%.2f', F.col("unit_price").cast("double")).alias("unit_price")
).coalesce(1).write.mode("overwrite").option("header", True).csv("out/clean_csv_no_swap_2dp"))

final.show(20, truncate=False)
print("rows_in =", sdf.count(), "| rows_out =", final.count())