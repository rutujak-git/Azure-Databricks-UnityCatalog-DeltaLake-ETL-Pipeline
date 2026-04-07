# Azure-Databricks-UnityCatalog-DeltaLake-ETL-Pipeline
An end-to-end Azure Data Engineering project demonstrating an ETL pipeline using Databricks Unity Catalog, Medallion Architecture (Bronze, Silver, Gold), and Delta Lake optimization techniques.

# End-to-End ETL Pipeline using Azure Databricks Unity Catalog & Delta Lake

## 📌 Overview
I have created this project from scratch and published it on my YouTube Channel : www.youtube.com/@DataToCrunch .

<img width="1921" height="1078" alt="image" src="https://github.com/user-attachments/assets/a6e08fc2-7755-4479-b6ed-e5765c830b41" />

This project demonstrates a comprehensive, enterprise-grade data pipeline built on the **Azure Databricks Lakehouse** platform. It covers the transition from traditional workspace-local data management to **Unity Catalog**, focusing on unified data governance, fine-grained access control, and advanced Delta Lake optimization techniques.

The pipeline follows the **Medallion Architecture** to process retail data, transforming raw files into analytics-ready Gold tables used for **Databricks SQL BI Dashboards**.

---

## 🏗️ Architecture

Kaggle → ADLS Gen2 → Access Connector ID → Databricks Unity Catalog (Bronze → Silver → Gold) → Databricks BI Dashboards

<img width="1260" height="1177" alt="image" src="https://github.com/user-attachments/assets/261993e8-cbbc-4b34-bedb-3fda8ebf9313" />

---

## ⚙️ Tech Stack

- **Azure Databricks:** Spark Compute & Unity Catalog
- **Unity Catalog:** Governance (Metastore, Catalogs, Schemas, Lineage)
- **Delta Lake:** Storage Layer (Managed & External Tables)
- **Azure Data Lake Storage Gen2 (ADLS):** Primary Data Lake
- **Access Connector for Azure:** Managed Identity for secure authentication
- **Databricks SQL:** Serverless Warehouses & BI Dashboards

---

## 🔄 Pipeline Flow

### 🔹 Infrastructure & Governance (Unity Catalog)
- Configuration of **Metastore, Storage Credentials, and External Locations**.
- Creating the hierarchical Object Model: **Catalog → Schema → Tables**.
- Implementing **Access Control** with **Row-Level Security (RLS)** and **Column-Level Security (CLS)**.

### 🔹 Bronze Layer
- Ingestion of raw Kaggle data into **Managed Delta Tables**.
- Initial schema definition and metadata tagging.

### 🔹 Silver Layer
- Data cleaning, filtering, and transformation logic.
- Handling **Schema Evolution** and utilizing **Table Utility Commands** (DESCRIBE, ALTER, TAGS).

### 🔹 Gold Layer
- Business-level aggregations and optimized datasets.
- Implementation of **Deep & Shallow Clones** for environment parity.

### 🔹 Optimization & Maintenance
- Performance tuning using **Liquid Clustering, Z-Order, and OPTIMIZE**.
- Storage management with **VACUUM (Dry Run)** and **Time Travel** for data recovery.

---

## 🚀 Key Features

- **Unity Catalog Integration:** Centralized metadata and security management.
- **Advanced Delta Lake 3.x:** Liquid Clustering, Deletion Vectors, and Change Data Feed (CDF).
- **Security & Lineage:** End-to-end data lineage tracking and fine-grained access policies.
- **Enterprise Patterns:** Medallion Architecture with automated checkpoints and schema enforcement.
- **Visualization:** Interactive AI-powered dashboards built directly on Databricks SQL.

---

### ▶️ How to Run

- Cloud Provisioning: Deploy ADLS Gen2 and an Access Connector for Azure Databricks.
- Entra ID Permissions: Assign Storage Blob Data Contributor to the Access Connector.
- Governance Layer: Configure Storage Credentials and External Locations within Unity Catalog.
- Data Transformation: Process the Medallion layers to move data from raw landing to optimized Gold tables.
- Visualization: Build interactive insights using Databricks SQL Dashboards.

---

### 🎥 YouTube Walkthrough
https://youtu.be/hUTDVTuZO60?si=AjykpFaOwVnrrUFr
