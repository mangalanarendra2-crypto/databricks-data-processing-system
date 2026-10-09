# Databricks notebook source
# MAGIC %md
# MAGIC # Data Processing System - run pipeline
# MAGIC Import the repo into Databricks Repos (or deploy the bundle) so `src/` is on the path.

# COMMAND ----------
import os
import sys

# When running from a Databricks Repo the notebook lives in <repo>/notebooks
repo_root = os.path.abspath(os.path.join(os.getcwd(), ".."))
sys.path.append(os.path.join(repo_root, "src"))

# COMMAND ----------
dbutils.widgets.text("base_path", "/Volumes/main/dps/data", "Base path (volume or DBFS)")
os.environ["DPS_BASE_PATH"] = dbutils.widgets.get("base_path")

# COMMAND ----------
from dps.config import Config
from dps.pipeline import run

cfg = Config.from_env()
print(cfg)
counts = run(spark, cfg)
counts

# COMMAND ----------
# Peek at a gold table
display(spark.read.format("delta").load(f"{cfg.gold_path}/daily_revenue"))
