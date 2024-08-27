# Databricks notebook source
# MAGIC %sql
# MAGIC select * from `hive_metastore`.`default`.`customers_bronze` limit 5

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from `hive_metastore`.`default`.`messages_bronze` limit 5

# COMMAND ----------

# MAGIC %md
# MAGIC Create our silver table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE `hive_metastore`.`default`.`customers_messages_silver` AS
# MAGIC SELECT *,
# MAGIC CASE WHEN lower(message) LIKE '%warning%' THEN 'Warning'
# MAGIC      WHEN lower(message) LIKE '%restored%' THEN 'Restored'
# MAGIC      WHEN lower(message) LIKE '%outage%' THEN 'Outage'
# MAGIC      WHEN lower(message) LIKE '%estimate%' THEN 'Estimate'
# MAGIC      ELSE NULL
# MAGIC END AS status_update,
# MAGIC to_date(timestamp) as date
# MAGIC FROM
# MAGIC (SELECT * FROM `hive_metastore`.`default`.`customers_bronze`
# MAGIC LEFT JOIN `hive_metastore`.`default`.`messages_bronze`
# MAGIC using (customer_id))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from `hive_metastore`.`default`.`customers_messages_silver`

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE `hive_metastore`.`default`.`sent_messages_gold` AS
# MAGIC SELECT date, customer_id, count(message_id) as messages_sent
# MAGIC FROM `hive_metastore`.`default`.`customers_messages_silver`
# MAGIC where date is not null
# MAGIC group by 1,2

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE `hive_metastore`.`default`.`sent_updates_gold` AS
# MAGIC SELECT date, status_update, count(status_update) as updates_sent
# MAGIC FROM `hive_metastore`.`default`.`customers_messages_silver`
# MAGIC where date is not null
# MAGIC group by 1,2

# COMMAND ----------


