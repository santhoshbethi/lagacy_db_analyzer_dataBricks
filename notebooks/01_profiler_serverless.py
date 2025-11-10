# Databricks notebook 01 — Data Profiler with Type Detection (Serverless Edition)
# (Full paste-ready code from ChatGPT)

from pyspark.sql import functions as F, types as T

# Widgets
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("source_schema", "")
dbutils.widgets.text("target_schema", "")
dbutils.widgets.text("sample_rows_per_table", "50000")

catalog = dbutils.widgets.get("catalog")
source_schema = dbutils.widgets.get("source_schema")
target_schema = dbutils.widgets.get("target_schema")
sample_n = int(dbutils.widgets.get("sample_rows_per_table"))

assert catalog and source_schema and target_schema, "Set all widget values."

spark.sql(f"USE CATALOG `{catalog}`")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{target_schema}`")

DATE_FORMATS = [
    "yyyy-MM-dd","MM/dd/yyyy","yyyy/MM/dd","yyyy.MM.dd",
    "dd-MM-yyyy","MM-dd-yyyy","yyyyMMdd"
]

def detect_date(col_name):
    exprs = [F.expr(f"try_to_date(`{{col_name}}`, '{fmt}')") for fmt in DATE_FORMATS]
    out = None
    for e in exprs:
        out = e if out is None else F.coalesce(out, e)
    return out

def detect_bool(col_name):
    c = F.lower(F.trim(F.col(col_name)))
    return (
        F.when(c.isin("true","t","1","yes","y"), True)
         .when(c.isin("false","f","0","no","n"), False)
         .otherwise(F.lit(None).cast("boolean"))
    )

def detect_numeric(col_name):
    cleaned = F.expr(f"regexp_replace(trim(`{{col_name}}`), '[^0-9\\.-]', '')")
    return cleaned.cast("decimal(38,9)")

tables = [
    r.asDict()
    for r in spark.sql(f"SHOW TABLES IN `{catalog}`.`{source_schema}`").collect()
    if not r.isTemporary
]

results = []

for t in tables:
    table_name = t["tableName"]
    df = spark.table(f"`{catalog}`.`{source_schema}`.`{table_name}`").limit(sample_n)
    total = df.count()
    if total == 0:
        continue

    for field in df.schema.fields:
        col_name = field.name
        col = F.col(col_name)
        dtype = field.dataType.simpleString()

        null_count = df.select(F.sum(F.when(col.isNull(), 1))).first()[0] or 0
        distinct_count = df.select(F.countDistinct(col)).first()[0] or 0

        null_rate = null_count / total
        distinct_rate = distinct_count / total

        date_match = bool_match = numeric_match = int_like = 0.0

        if isinstance(field.dataType, T.StringType):
            bool_col = detect_bool(col_name)
            bool_ok = df.select(F.sum(F.when(bool_col.isNotNull(), 1))).first()[0] or 0
            bool_match = bool_ok / total

            date_col = detect_date(col_name)
            date_ok = df.select(F.sum(F.when(date_col.isNotNull(), 1))).first()[0] or 0
            date_match = date_ok / total

            num_col = detect_numeric(col_name)
            num_ok = df.select(F.sum(F.when(num_col.isNotNull(), 1))).first()[0] or 0
            numeric_match = num_ok / total

            if num_ok > 0:
                int_ok = df.select(F.sum(F.when(F.floor(num_col) == num_col, 1))).first()[0] or 0
                int_like = int_ok / total

        freq_df = df.groupBy(col).count().orderBy(F.desc("count")).limit(8)
        samples = [str(r[col_name]) for r in freq_df.collect()]

        results.append({
            "table_name": table_name,
            "column_name": col_name,
            "data_type": dtype,
            "row_count": total,
            "null_rate": float(null_rate),
            "distinct_rate": float(distinct_rate),
            "date_match": float(date_match),
            "bool_match": float(bool_match),
            "numeric_match": float(numeric_match),
            "int_like": float(int_like),
            "sample_values": ", ".join(samples)
        })

profile_df = spark.createDataFrame(results)
profile_df.write.mode("overwrite").saveAsTable(
    f"`{catalog}`.`{target_schema}`.table_profile"
)

display(profile_df)
