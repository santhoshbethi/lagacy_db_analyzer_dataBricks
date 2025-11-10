# Notebook 02 — Dashboard Builder

# MAGIC %sql
# SELECT * FROM {catalog}.{target_schema}.table_profile;

# MAGIC %sql
# SELECT table_name, column_name, null_rate
# FROM {catalog}.{target_schema}.table_profile
# ORDER BY null_rate DESC;

# MAGIC %sql
# SELECT table_name, column_name, date_match
# FROM {catalog}.{target_schema}.table_profile
# WHERE date_match > 0.6;

# MAGIC %sql
# SELECT table_name, column_name, numeric_match
# FROM {catalog}.{target_schema}.table_profile
# WHERE numeric_match > 0.6;
