# Gene Functional Abstraction Pipeline

Production-ready big data pipeline for analyzing human genome functional characteristics using Gene Ontology (GO) data with Apache Spark and Databricks.

## Overview

Ingests GO ontology and human genome annotations, computes functional impact metrics, and provides interactive visualization for exploring gene functions at scale.

## Key Features

- **Medallion Architecture**: Bronze → Silver → Gold data layers with Delta Lake
- **Functional Impact Index**: Weighted scoring combining pathway, function, and component annotations
- **Pathway Enrichment Analysis**: Statistical analysis of gene ontology distributions
- **Interactive Dashboard**: 5-tab Dash interface for exploration and export
- **Scalable Processing**: Spark-based for local development or Databricks cloud deployment

## Technologies

- Apache Spark 3.5+ & Delta Lake 3.0+
- Databricks for production deployment
- Dash/Plotly for visualization
- PAN-GO Functionome data source

## Quick Start

```bash
pip install -r requirements.txt
python src/main_spark.py
```

Dashboard available at `http://localhost:8050`

## Documentation

- `ARCHITECTURE.md` - Detailed architecture and design
- `README_SPARK.md` - User guide and deployment options
- `databricks/` - Production deployment notebook

## Project Structure

```
config/         # Configuration (APIs, paths, Spark settings)
src/            # Pipeline components (ingestion, processing, visualization)
databricks/     # Cloud deployment notebook
data/           # Local development data layers
```

## Computed Metrics

**Functional Impact Index** = (BP_count × 2.0) + (MF_count × 2.0) + (CC_count × 1.0) + (Experimental_evidence × 1.5)

Quality tiers based on evidence code types (experimental, phylogenetic, computational).
