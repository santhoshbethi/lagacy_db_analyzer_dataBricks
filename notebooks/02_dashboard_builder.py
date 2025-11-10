# Notebook 02 — Dashboard Builder
# Use this to build charts from table_profile.
# Example queries:

# MAGIC %sql
# SELECT * FROM {catalog}.{target_schema}.table_profile;

# MAGIC %sql
# CREATE OR REPLACE VIEW {catalog}.{target_schema}.null_rates AS
# SELECT table_name, column_name, null_rate FROM {catalog}.{target_schema}.table_profile;

# MAGIC %sql
# CREATE OR REPLACE VIEW {catalog}.{target_schema}.type_detection AS
# SELECT table_name, column_name, date_match, bool_match, numeric_match FROM {catalog}.{target_schema}.table_profile;
