# Notebook 04 — Insights Notebook
# Example aggregations for dashboards.

# MAGIC %sql
# SELECT table_name, COUNT(*) AS cols
# FROM {catalog}.{target_schema}.table_profile
# GROUP BY table_name;

# MAGIC %sql
# SELECT table_name, column_name, date_match
# FROM {catalog}.{target_schema}.table_profile
# WHERE date_match > 0.5;
