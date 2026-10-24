# Running on Databricks and Azure (free tiers)

There are two ways to run this in the cloud. Start with **Option A**: it is free and needs
no credit card. Option B adds Azure Data Lake Storage as the landing zone.

## Option A: Databricks Free Edition (free, no credit card)

1. **Sign up** at <https://www.databricks.com/learn/free-edition>. You get a serverless
   workspace with a Unity Catalog catalog called `workspace`.
2. **Create the landing volume.** In the workspace open *SQL Editor* and run:
   ```sql
   CREATE SCHEMA IF NOT EXISTS workspace.raw;
   CREATE VOLUME IF NOT EXISTS workspace.raw.landing;
   ```
3. **Upload the data.** Download the dataset (`make data`, or manually from
   [Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)), then in
   *Catalog → workspace → raw → landing* click **Upload to this volume** and upload the CSV
   files (skip `olist_geolocation_dataset.csv`, it is not used).
4. **Install the Databricks CLI** and log in:
   ```bash
   # macOS: brew tap databricks/tap && brew install databricks
   # Windows: winget install Databricks.DatabricksCLI
   databricks auth login --host https://<your-workspace-url>
   ```
5. **Deploy and run the job** from the repo folder:
   ```bash
   databricks bundle validate
   databricks bundle deploy -t dev
   databricks bundle run olist_lakehouse_job -t dev
   ```
6. **Look at the results.** In *Jobs & Pipelines* you will see the four-task job
   (bronze → silver → gold → quality). In *Catalog* you will find
   `workspace.bronze.*`, `workspace.silver.*` and `workspace.gold.*`. Open the
   *SQL Editor* and paste any query from `sql/analytics`, replacing
   `{{ gold.fact_order_items }}` with `workspace.gold.fact_order_items`.

Tip: take screenshots of the job run graph, the catalog tables and a dashboard on
`gold.mart_daily_sales` for your README and LinkedIn post.

## Option B: Azure Data Lake Storage Gen2 as the landing zone

Azure's free account gives a starting credit for the first month and some always-free
services; storing a few hundred MB in ADLS costs only cents. Check the current offer at
<https://azure.microsoft.com/free>.

1. **Create a storage account** in the Azure portal: *Storage accounts → Create*,
   Standard / LRS, and on the *Advanced* tab turn on **Hierarchical namespace** (this makes
   it ADLS Gen2).
2. **Upload the raw files** with the script in this repo:
   ```bash
   pip install -e ".[azure]"
   az login
   python scripts/upload_to_adls.py --account <storage-account> --container raw
   ```
   Files land in `abfss://raw@<account>.dfs.core.windows.net/olist/<today>/`.
3. **Read them from Databricks.** Databricks Free Edition cannot connect to your own cloud
   storage, so for this step use an **Azure Databricks** workspace (Azure offers a
   14-day Premium trial). In that workspace:
   - create a *Storage credential* (Access Connector for Azure Databricks with the
     *Storage Blob Data Contributor* role on the storage account) and an
     *External location* for the container (Catalog → External data);
   - deploy with the ADLS path as the raw path:
     ```bash
     databricks bundle deploy -t dev \
       --var="raw_path=abfss://raw@<account>.dfs.core.windows.net/olist/<date>" \
       --var="catalog=<your catalog>"
     ```

## Turning on CD from GitHub

1. In Databricks create a personal access token (*Settings → Developer → Access tokens*).
2. In GitHub open the repo → *Settings → Secrets and variables → Actions*:
   - Secrets: `DATABRICKS_HOST` (workspace URL) and `DATABRICKS_TOKEN`
   - Variables: `DATABRICKS_ENABLED` = `true`
3. Every push to `main` now runs CI, and if it passes, `cd.yml` deploys the bundle to
   the `prod` target and runs the job.

## Cost safety

- The job schedule is **paused** by default; un-pause it only if you want daily runs.
- Delete the Azure resource group when you are done so nothing keeps billing.
