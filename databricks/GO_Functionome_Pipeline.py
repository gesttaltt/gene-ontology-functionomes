# Databricks notebook source
# MAGIC %md
# MAGIC # Gene Ontology Functionome Pipeline - Databricks Edition
# MAGIC
# MAGIC This notebook orchestrates the complete GO functionome analysis pipeline:
# MAGIC - **Bronze Layer**: Raw data ingestion from GO APIs
# MAGIC - **Silver Layer**: Data cleaning and validation
# MAGIC - **Gold Layer**: Functionome metrics and enrichment analytics
# MAGIC
# MAGIC **Author**: Gene Functional Abstraction Team
# MAGIC **Version**: 2.0 (Spark/Delta Edition)
# MAGIC **Last Updated**: 2025-10-30

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Environment Setup

# COMMAND ----------

# Install dependencies (if not in cluster libraries)
# %pip install requests delta-spark great-expectations

# COMMAND ----------

import sys
import os
from datetime import datetime
from pyspark.sql import functions as F

# Import pipeline modules
# Note: Upload src/ and config/ folders to DBFS or use %run for notebooks
sys.path.append("/dbfs/mnt/go-pipeline/src")
sys.path.append("/dbfs/mnt/go-pipeline/config")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Configuration

# COMMAND ----------

# Pipeline configuration
ENVIRONMENT = "databricks"
MODE = "batch"  # Options: "batch", "streaming"
RUN_ID = datetime.now().strftime("%Y%m%d_%H%M%S")

print(f"Pipeline Run ID: {RUN_ID}")
print(f"Environment: {ENVIRONMENT}")
print(f"Mode: {MODE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Initialize Pipeline Components

# COMMAND ----------

from ingestion_spark import GOIngestionSpark
from processing_spark import GOProcessingSpark

# Initialize ingestion pipeline
ingestion = GOIngestionSpark(
    is_databricks=True,
    enable_streaming=(MODE == "streaming")
)

# Initialize processing pipeline
processing = GOProcessingSpark(
    spark=spark,  # Databricks provides 'spark' session
    is_databricks=True
)

print("✓ Pipeline components initialized")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. BRONZE LAYER - Data Ingestion
# MAGIC
# MAGIC Fetch raw data from Gene Ontology sources:
# MAGIC - GO Ontology (terms, relationships)
# MAGIC - Human Genome Annotations (GOA database)

# COMMAND ----------

# Run batch ingestion
print("="*80)
print("BRONZE LAYER INGESTION")
print("="*80)

ingestion_results = ingestion.run_batch_ingestion()

print("\n✓ Bronze layer ingestion complete")
display(ingestion_results['annotations'].limit(10))

# COMMAND ----------

# Check data quality
annotations_count = ingestion_results['annotations'].count()
ontology_count = ingestion_results['ontology'].count()

print(f"Bronze Layer Statistics:")
print(f"  • Annotations: {annotations_count:,}")
print(f"  • Ontology Terms: {ontology_count:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. SILVER LAYER - Data Cleaning & Validation
# MAGIC
# MAGIC Transform Bronze → Silver:
# MAGIC - Clean and validate annotations
# MAGIC - Filter by evidence quality
# MAGIC - Build ontology graph

# COMMAND ----------

print("="*80)
print("SILVER LAYER TRANSFORMATION")
print("="*80)

# Clean annotations
silver_annotations = processing.clean_annotations(
    ingestion_results['annotations']
)

# Build ontology graph
silver_ontology = processing.build_ontology_graph(
    ingestion_results['ontology']
)

print("\n✓ Silver layer transformation complete")

# COMMAND ----------

# Display cleaned data
display(silver_annotations.limit(10))

# COMMAND ----------

# Data quality report
print("Silver Layer Quality Report:")
print(f"  • Total cleaned annotations: {silver_annotations.count():,}")
print(f"  • Unique genes: {silver_annotations.select('gene_id').distinct().count():,}")
print(f"  • Unique GO terms: {silver_annotations.select('go_id').distinct().count():,}")

# Evidence breakdown
silver_annotations.groupBy("quality_tier").count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. GOLD LAYER - Functionome Analytics
# MAGIC
# MAGIC Compute business-ready metrics:
# MAGIC - Gene functional impact scores
# MAGIC - Pathway enrichment analysis

# COMMAND ----------

print("="*80)
print("GOLD LAYER ANALYTICS")
print("="*80)

# Compute gene functionome
functionome = processing.compute_gene_functionome(silver_annotations)

# Compute pathway enrichment
enrichment = processing.compute_pathway_enrichment(
    silver_annotations,
    silver_ontology
)

print("\n✓ Gold layer analytics complete")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.1 Top Genes by Functional Impact

# COMMAND ----------

# Display top 20 genes
top_genes = functionome.orderBy(F.col("functional_impact_index").desc()).limit(20)
display(top_genes.select(
    "gene_symbol",
    "gene_name",
    "functional_impact_index",
    "unique_go_terms",
    "biological_process_count",
    "molecular_function_count",
    "quality_tier"
))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.2 Functional Impact Distribution

# COMMAND ----------

# Create histogram visualization
display(
    functionome.select("functional_impact_index", "quality_tier")
    .summary("count", "mean", "stddev", "min", "25%", "50%", "75%", "max")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.3 Top Enriched Pathways

# COMMAND ----------

# Display top enriched pathways
top_pathways = enrichment.orderBy(F.col("enrichment_score").desc()).limit(20)
display(top_pathways.select(
    "go_id",
    "go_term",
    "go_aspect",
    "gene_count",
    "enrichment_score"
))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Data Export & Persistence

# COMMAND ----------

# Register Delta tables for SQL access
functionome.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("go_gold.gene_functionome")

enrichment.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("go_gold.pathway_enrichment")

print("✓ Tables registered:")
print("  • go_gold.gene_functionome")
print("  • go_gold.pathway_enrichment")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. SQL Analysis (Optional)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query top genes by impact
# MAGIC SELECT
# MAGIC   gene_symbol,
# MAGIC   gene_name,
# MAGIC   functional_impact_index,
# MAGIC   unique_go_terms,
# MAGIC   quality_tier
# MAGIC FROM go_gold.gene_functionome
# MAGIC ORDER BY functional_impact_index DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Aspect distribution
# MAGIC SELECT
# MAGIC   quality_tier,
# MAGIC   COUNT(*) as gene_count,
# MAGIC   AVG(functional_impact_index) as avg_impact,
# MAGIC   AVG(unique_go_terms) as avg_go_terms
# MAGIC FROM go_gold.gene_functionome
# MAGIC GROUP BY quality_tier
# MAGIC ORDER BY avg_impact DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Pipeline Summary

# COMMAND ----------

print("="*80)
print("PIPELINE EXECUTION SUMMARY")
print("="*80)
print(f"Run ID: {RUN_ID}")
print(f"Status: SUCCESS")
print(f"\nData Layers:")
print(f"  • Bronze: Raw GO data ingested")
print(f"  • Silver: {silver_annotations.count():,} cleaned annotations")
print(f"  • Gold: {functionome.count():,} gene metrics computed")
print(f"\nOutput Tables:")
print(f"  • go_gold.gene_functionome")
print(f"  • go_gold.pathway_enrichment")
print("="*80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC 1. **Visualization**: Connect Dash/Plotly dashboard to Delta tables
# MAGIC 2. **Scheduling**: Configure job scheduling for daily updates
# MAGIC 3. **Monitoring**: Set up data quality alerts
# MAGIC 4. **API**: Expose functionome data via REST API
# MAGIC 5. **ML**: Train models on functional impact scores
