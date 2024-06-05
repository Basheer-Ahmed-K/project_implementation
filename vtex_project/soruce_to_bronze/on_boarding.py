# Databricks notebook source
storage_account_name = "projectstoragevtex"
storage_account_access_key = f"/R/BrreANf5eAZKMdCIyeJMbFacnrCQCcLj7urIXVgSkoA0LDBK/JQLJfXvfCK8bp19eZlTyBR+E+AStf7FZfA=="
container_name = "vtexproject"
 
# Create the DBFS mount point
mount_point = f"/mnt/{container_name}"
 
dbutils.fs.mount(
  source = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net",
  mount_point = mount_point,
  extra_configs = {f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net": storage_account_access_key}
)

# COMMAND ----------

df = spark.read.json("dbfs:/mnt/vtexproject/rawdata/vtex_test_data.json")
display(df)