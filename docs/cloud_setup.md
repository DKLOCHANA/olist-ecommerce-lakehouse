# Cloud setup: Azure Data Lake + Databricks Free Edition

This is how the project is deployed: raw files land in **Azure Data Lake Storage Gen2**, and a
**Databricks** job (serverless, Free Edition) ingests them and builds the lakehouse.

```
Azure ADLS Gen2  ──SAS (secret scope)──▶  ingest task  ──▶  UC Volume  ──▶  bronze ▶ silver ▶ gold ▶ quality
olistlakelochana/raw/olist/<date>/        (Databricks)      raw.landing
```

## 1. Azure: the landing zone

| Resource | Value |
|---|---|
| Resource group | `rg-olist-lakehouse` (Southeast Asia) |
| Storage account | `olistlakelochana` — StorageV2, Standard, **LRS**, **hierarchical namespace on** (ADLS Gen2) |
| Container | `raw` (private) |
| Load folder | `raw/olist/<yyyy-mm-dd>/` — one folder per load; the ingest task always takes the newest |

Upload a new load (from Azure Cloud Shell or any machine with the Azure CLI):

```bash
curl -sL -o olist.zip https://www.kaggle.com/api/v1/datasets/download/olistbr/brazilian-ecommerce
unzip -oq olist.zip -d olist && rm olist/olist_geolocation_dataset.csv
az storage blob upload-batch --account-name olistlakelochana --auth-mode key \
  -d raw --destination-path olist/$(date +%F) -s olist --overwrite
```

or from this repo: `python scripts/upload_to_adls.py --account olistlakelochana`.

Cost: a few hundred MB on LRS hot storage costs cents per month.

## 2. Connect Databricks to Azure (one time)

Databricks Free Edition can't use Unity Catalog external locations, so the job reads ADLS
with a **read-only, expiring SAS token** stored in a Databricks **secret scope**
(`olist` / `adls-sas`). The token never appears in code, Git or job parameters.

1. **Create the SAS token** — Azure portal → storage account `olistlakelochana` →
   *Security + networking* → **Shared access signature**:
   - Allowed services: **Blob** · Allowed resource types: **Container** + **Object**
   - Allowed permissions: **Read** + **List** only
   - Expiry: e.g. 3–6 months ahead · Allowed protocols: HTTPS only
   - Click **Generate SAS and connection string** and copy the **SAS token**.

   Or in Cloud Shell:
   ```bash
   az storage container generate-sas --account-name olistlakelochana --auth-mode key \
     --name raw --permissions rl --expiry $(date -u -d "+90 days" +%Y-%m-%dT%H:%MZ) --https-only -o tsv
   ```
2. **Store it** — in Databricks open the Git folder → `notebooks/00_connect_azure`, paste the
   token into the `sas_token` box, click **Run all**. The last cell removes the box again.

## 3. Databricks: deploy and run

The repo is cloned into the workspace as a **Git folder**
(*Workspace → Create → Git folder* → `https://github.com/DKLOCHANA/olist-ecommerce-lakehouse.git`).

- **Deploy the job** — open the Git folder, open `databricks.yml` and use **Deploy** in the
  bundle panel (target `dev`). Or from a terminal with the Databricks CLI:
  ```bash
  databricks auth login --host https://<workspace>.cloud.databricks.com
  databricks bundle deploy -t dev
  databricks bundle run olist_lakehouse_job -t dev
  ```
- **Run it** — *Jobs & Pipelines* → `[dev …] olist-lakehouse-dev` → **Run now**. Five tasks run
  on serverless compute: ingest → bronze → silver → gold → quality.
- **Explore** — open `notebooks/02_explore_gold` for the business questions, or query
  `workspace.gold.*` in the SQL editor.

No Azure yet? Set the bundle variable `ingest_source` to `kaggle` and the ingest task downloads
the public dataset directly.

## 4. CD from GitHub (optional)

1. Databricks → *Settings → Developer → Access tokens* → create a token.
2. GitHub repo → *Settings → Secrets and variables → Actions*:
   secrets `DATABRICKS_HOST`, `DATABRICKS_TOKEN`; variable `DATABRICKS_ENABLED=true`.
3. Every push to `main` then runs CI and, if it passes, deploys and runs the `prod` job.

## Cost safety

- The job schedule is **paused** by default.
- Free Edition is free; the only Azure cost is storage (cents). Delete the resource group
  `rg-olist-lakehouse` when you no longer need it.
