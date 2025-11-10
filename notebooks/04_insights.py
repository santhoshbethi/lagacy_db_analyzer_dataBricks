# Notebook 04 — Insights

# MAGIC %sql
# SELECT table_name, COUNT(*) AS total_columns
# FROM {catalog}.{target_schema}.table_profile
# GROUP BY table_name;

# MAGIC %sql
# SELECT *
# FROM {catalog}.{target_schema}.table_profile
# WHERE date_match > 0.5
# ORDER BY date_match DESC;
