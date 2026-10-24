# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Connect Databricks to the Azure landing zone
# MAGIC
# MAGIC Run this **once**. It stores a **read-only SAS token** for the Azure Data Lake Storage Gen2
# MAGIC account in a Databricks secret scope, so the `ingest` task can read the raw files without
# MAGIC any credential appearing in code, job parameters or Git.
# MAGIC
# MAGIC **Create the token in the Azure portal:** Storage account → *Security + networking* →
# MAGIC *Shared access signature* → Allowed services: **Blob**, resource types: **Container + Object**,
# MAGIC permissions: **Read + List**, set an expiry date → *Generate SAS and connection string* →
# MAGIC copy the **SAS token**.
# MAGIC
# MAGIC Paste it into the `sas_token` box at the top of this notebook, run all cells, then
# MAGIC **clear the box** (the last cell also removes the widget).

# COMMAND ----------

dbutils.widgets.text("sas_token", "", "Azure SAS token")

# COMMAND ----------

import re

from databricks.sdk import WorkspaceClient

SCOPE, KEY = "olist", "adls-sas"
raw = dbutils.widgets.get("sas_token")
# accept a token, a SAS URL or a connection string; keep only the 'sv=...&sig=...' part
matches = [m for m in re.findall(r"sv=[^\s'\"?&]+(?:&[A-Za-z]+=[^\s'\"&]+)+", raw) if "sig=" in m]
assert matches, "Paste the SAS token from the Azure portal (it starts with 'sv=' and contains 'sig=')"
token = matches[-1]
params = dict(p.split("=", 1) for p in token.split("&"))
print("permissions:", params.get("sp"), "| expires:", params.get("se"))

w = WorkspaceClient()
if SCOPE not in [s.name for s in w.secrets.list_scopes()]:
    w.secrets.create_scope(scope=SCOPE)
w.secrets.put_secret(scope=SCOPE, key=KEY, string_value=token)
print(f"Stored secret {SCOPE}/{KEY}:", "[REDACTED]" if dbutils.secrets.get(SCOPE, KEY) else "missing")

# COMMAND ----------

dbutils.widgets.removeAll()  # don't leave the token in the notebook UI
