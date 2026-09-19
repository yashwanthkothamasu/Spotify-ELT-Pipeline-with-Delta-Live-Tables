# Spotify-ELT-Pipeline-with-Delta-Live-Tables
Data Source

Spotify relational dataset with the following tables:
DimAlbum (album details)
DimArtist (artist details)
DimTrack (track details)
DimDate (date dimension)
FactStream (streaming events)

Tech Stack
Azure Data Factory (ADF)
Azure Data Lake Storage Gen2 (ADLS)
Azure Databricks
PySpark, Spark SQL
Delta Lake, Delta Live Tables (DLT)
Unity Catalog
Databricks Asset Bundles

What I Built
Source Layer (Azure SQL DB + ADF)
Stored the Spotify dataset in Azure SQL Database and used Azure Data Factory to orchestrate the extraction. Built ADF pipelines using Sub Activities, If-Else conditions, Lookup Activity, Date Lookup Activity, and Script Activity to handle conditional logic and incremental data extraction. Transferred the extracted data as files into Azure Blob Storage.

Bronze Layer (Raw Ingestion)

Landed the ADF output into ADLS in parquet format as the raw Bronze layer. Stored data as-is with no transformations, maintaining a full copy of every extraction run.

Silver Layer (Databricks Transformations)

Set up Azure Databricks workspace with External Locations using Storage Credentials, Unity Catalog with Catalog, Schema, and Tables. Used Autoloader (spark.readStream.format("cloudFiles")) for incremental data loading with checkpoint and schema location tracking. Applied transformations including drop duplicates, column renaming and casting with withColumn, filtering invalid records, and MERGE logic for Delta loads into Silver catalog tables.

Gold Layer (Delta Live Tables + CDC)

Built Delta Live Tables (DLT) pipelines for the Gold layer. Created staging tables using @dlt.view decorators and target streaming tables using @dlt.table. Applied APPLY CHANGES INTO (Auto CDC) flow for SCD Type 2 handling, which automatically tracks old and new records with updated_at timestamps. Added DLT Expectations for data quality validation before loading to target tables. Loaded final Gold tables into Unity Catalog.

Challenges and Solutions

Unity Metastore conflict with DLT: Initially used Unity Metastore for unified storage, but DLT pipeline creation failed due to metastore related issues. Resolved by creating separate storage paths for the Catalog and for DLT pipelines.
