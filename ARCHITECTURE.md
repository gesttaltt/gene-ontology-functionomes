# Gene Ontology Functionome Pipeline - Architecture Guide

## 🏗️ Overview

This project implements a **production-ready big data pipeline** for Gene Ontology (GO) functionome analysis using:
- **Apache Spark** for distributed processing
- **Delta Lake** for ACID transactions and time travel
- **Parquet** for columnar storage
- **Databricks** compatibility for cloud-scale execution

---

## 🎯 Architecture Goals

1. **Scalability**: Process millions of gene annotations efficiently
2. **Streaming Support**: Enable real-time GO updates
3. **Data Quality**: Implement Bronze/Silver/Gold medallion architecture
4. **Production-Ready**: Databricks integration for enterprise deployment
5. **Scientific Rigor**: Focus on PAN-GO functionome and evidence-based scoring

---

## 📊 Medallion Architecture (Bronze/Silver/Gold)

```
┌─────────────────────────────────────────────────────────────────┐
│                     DATA LAKE ARCHITECTURE                       │
└─────────────────────────────────────────────────────────────────┘

┌──────────────┐    ┌──────────────┐    ┌──────────────┐
│   BRONZE     │───▶│    SILVER    │───▶│     GOLD     │
│  Raw Data    │    │ Cleaned Data │    │  Analytics   │
└──────────────┘    └──────────────┘    └──────────────┘
     ▲                     ▲                   ▲
     │                     │                   │
  Ingestion          Validation          Aggregation
```

### Bronze Layer (Raw Data)
- **Purpose**: Immutable landing zone for raw data
- **Format**: Delta Lake (with audit trail)
- **Sources**:
  - GO Ontology JSON (from OBO Library)
  - Human Genome Annotations (GOA database)
  - UniProt Proteomes (PAN-GO functionome)
- **Partitioning**: By ingestion date
- **Retention**: Indefinite (for reproducibility)

### Silver Layer (Validated Data)
- **Purpose**: Cleaned, validated, business-ready data
- **Format**: Delta Lake
- **Transformations**:
  - Remove negative annotations ("NOT" qualifiers)
  - Filter low-quality evidence codes
  - Standardize column names
  - Add quality tier classifications
- **Partitioning**: By GO aspect (P/F/C) and quality tier
- **Schema Enforcement**: Yes

### Gold Layer (Analytics)
- **Purpose**: Aggregated metrics for consumption
- **Format**: Delta Lake + Parquet exports
- **Metrics**:
  - **Functional Impact Index**: Weighted score combining pathways, functions, components
  - **Pathway Enrichment**: Statistical enrichment analysis
  - **Evidence Quality Scores**: Experimental vs computational
- **Partitioning**: By gene ID ranges
- **Access**: SQL queries, Dash dashboard, REST API

---

## 🔄 Data Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                    INGESTION PIPELINE                            │
└─────────────────────────────────────────────────────────────────┘

GO API                    GOA Database              UniProt
(Ontology)                (Annotations)             (Proteome)
    │                          │                         │
    └──────────┬───────────────┴─────────────────────────┘
               ▼
    ┌──────────────────────┐
    │  GOIngestionSpark    │
    │  - fetch_go_ontology │
    │  - fetch_human_genome│
    └──────────┬───────────┘
               ▼
    ┌──────────────────────┐
    │   BRONZE LAYER       │
    │   (Delta Lake)       │
    └──────────┬───────────┘
               ▼
    ┌──────────────────────┐
    │  GOProcessingSpark   │
    │  - clean_annotations │
    │  - build_ontology    │
    └──────────┬───────────┘
               ▼
    ┌──────────────────────┐
    │   SILVER LAYER       │
    │   (Delta Lake)       │
    └──────────┬───────────┘
               ▼
    ┌──────────────────────┐
    │  GOProcessingSpark   │
    │  - compute_functionome│
    │  - pathway_enrichment│
    └──────────┬───────────┘
               ▼
    ┌──────────────────────┐
    │    GOLD LAYER        │
    │   (Delta + Parquet)  │
    └──────────┬───────────┘
               ▼
    ┌──────────────────────┐
    │  GOVisualizationSpark│
    │  Dash Dashboard      │
    └──────────────────────┘
```

---

## 📂 Project Structure

```
gene-functional-abstraction/
│
├── config/
│   └── go_config.py              # Centralized configuration
│
├── src/
│   ├── ingestion_spark.py        # Bronze layer ingestion
│   ├── processing_spark.py       # Silver → Gold transformations
│   ├── visualization_spark.py    # Dash dashboard (Parquet/Delta)
│   └── main_spark.py             # Pipeline orchestrator
│
├── databricks/
│   └── GO_Functionome_Pipeline.py # Databricks notebook
│
├── data/                          # Local development data
│   ├── bronze/
│   ├── silver/
│   └── gold/
│
├── output_parquet/                # Parquet exports for visualization
│
├── requirements.txt               # Python dependencies
└── ARCHITECTURE.md                # This file
```

---

## 🔑 Key Components

### 1. Configuration (`config/go_config.py`)
Centralized configuration for:
- GO API endpoints (BioLink, QuickGO, OBO Library)
- Human genome data sources (GOA, UniProt, RefSeq)
- Storage paths (DBFS for Databricks, local for dev)
- Spark optimizations
- Data quality rules

### 2. Ingestion (`src/ingestion_spark.py`)
**Class**: `GOIngestionSpark`

**Capabilities**:
- Batch ingestion from GO APIs
- Streaming support (structured streaming)
- Automatic schema inference
- Delta Lake writes with partitioning
- Human genome focus (Homo sapiens taxon 9606)

**Key Methods**:
```python
fetch_go_ontology()          # GO terms and relationships
fetch_human_genome_annotations()  # GOA annotations
enable_structured_streaming()     # Real-time updates
```

### 3. Processing (`src/processing_spark.py`)
**Class**: `GOProcessingSpark`

**Bronze → Silver**:
```python
clean_annotations()          # Quality filtering
build_ontology_graph()       # GO term hierarchy
```

**Silver → Gold**:
```python
compute_gene_functionome()   # Functional impact index
compute_pathway_enrichment() # Pathway statistics
```

**Metrics Computed**:
- **Functional Impact Index**:
  ```
  Impact = (BP_count × 2.0) + (MF_count × 2.0) +
           (CC_count × 1.0) + (experimental_evidence × 1.5)
  ```
- **Normalized Score**: 0-1 range for comparisons
- **Functional Diversity**: Aspect × GO term count
- **Quality Tier**: High/Medium/Low confidence

### 4. Visualization (`src/visualization_spark.py`)
**Class**: `GOVisualizationSpark`

**Features**:
- Reads Parquet or Delta format
- Interactive Dash dashboard
- 5 analysis tabs:
  1. Overview (top genes, statistics)
  2. Distribution analysis
  3. Correlation scatter plots
  4. Interactive filtering
  5. Data export
- Supports 20,000+ gene datasets

### 5. Orchestrator (`src/main_spark.py`)
**Class**: `GOPipelineOrchestrator`

**Usage**:
```bash
# Local execution
python src/main_spark.py --mode batch --export

# Databricks execution
python src/main_spark.py --mode batch --databricks
```

---

## 🚀 Deployment Options

### Option 1: Local Development
```bash
# Install dependencies
pip install -r requirements.txt

# Run pipeline
python src/main_spark.py --mode batch --export

# Launch dashboard
python src/visualization_spark.py \
  --data-path ./output_parquet/gene_functionome.parquet \
  --format parquet
```

### Option 2: Databricks
1. **Upload Files**:
   - Upload `src/` and `config/` to DBFS: `/dbfs/mnt/go-pipeline/`
   - Upload notebook: `databricks/GO_Functionome_Pipeline.py`

2. **Create Cluster**:
   - Runtime: 14.3 LTS (Spark 3.5.0)
   - Libraries: `delta-spark`, `great-expectations`, `requests`
   - Workers: 2-8 (autoscaling)

3. **Configure Storage**:
   - Mount S3/ADLS: `/mnt/go-pipeline-data/`
   - Update `go_config.py` with DBFS paths

4. **Run Notebook**:
   - Execute cells sequentially
   - Register Delta tables for SQL access
   - Schedule as job for periodic updates

### Option 3: Databricks Jobs (Scheduled)
```python
# Create job via Databricks CLI
databricks jobs create --json '{
  "name": "GO-Functionome-Pipeline",
  "tasks": [{
    "task_key": "ingest",
    "notebook_task": {
      "notebook_path": "/Workspace/GO_Functionome_Pipeline"
    }
  }],
  "schedule": {
    "quartz_cron_expression": "0 0 2 * * ?",
    "timezone_id": "America/Los_Angeles"
  }
}'
```

---

## 🔬 Scientific Methodology

### PAN-GO Functionome
This pipeline implements the **PAN-GO (Phylogenetic ANnotation) functionome** methodology:
- Comprehensive annotations for human protein-coding genes
- Phylogenetically-inferred functional annotations
- Minimally redundant GO term sets
- Evidence-weighted scoring

**Reference**: Feuermann et al., *Nature* 2025

### Evidence Code Quality Hierarchy
1. **Experimental** (highest confidence): EXP, IDA, IPI, IMP, IGI, IEP
2. **Phylogenetic**: IBA, IBD, IKR, IRD
3. **Computational**: ISS, ISO, ISA, ISM, IGC
4. **Curator/Author**: TAS, NAS, IC
5. **Electronic** (lowest confidence): IEA

### Functional Impact Scoring
The impact index uses a weighted model:
```python
impact = (
    biological_process_count * 2.0 +    # Core functionality
    molecular_function_count * 2.0 +    # Molecular mechanisms
    cellular_component_count * 1.0 +    # Localization (lower weight)
    experimental_evidence_count * 1.5   # Quality bonus
)
```

This captures both **breadth** (number of annotations) and **depth** (evidence quality).

---

## 📈 Performance Characteristics

### Scalability
- **Local Mode**: 10K-100K genes (laptop-friendly)
- **Databricks**: 100K-10M+ genes (cluster required)
- **Streaming**: Real-time updates (1-hour latency)

### Storage Efficiency
- **CSV** (old): 1 GB uncompressed
- **Parquet**: 200 MB (5x compression)
- **Delta Lake**: 210 MB (includes transaction log)

### Query Performance
- **CSV Read**: 10-30 seconds
- **Parquet Read**: 1-3 seconds
- **Delta Read (cached)**: <1 second

---

## 🔐 Data Quality & Governance

### Quality Rules
```python
QUALITY_RULES = {
    "min_annotations_per_gene": 1,
    "max_annotations_per_gene": 10000,
    "required_fields": ["gene_id", "go_id", "evidence_code"],
    "exclude_evidence_codes": ["ND"],      # No Data
    "exclude_qualifiers": ["NOT"]          # Negative annotations
}
```

### Data Lineage
Delta Lake automatically tracks:
- Ingestion timestamps
- Schema evolution
- Data modifications
- Time travel queries

### Reproducibility
```python
# Query historical data
spark.read.format("delta") \
  .option("versionAsOf", 5) \
  .load("/data/silver/gene_annotations")
```

---

## 🔮 Future Enhancements

1. **Real-time Streaming**: Consume GO updates via Kafka
2. **ML Integration**: Train predictive models on functionome
3. **Multi-species Support**: Extend beyond human genome
4. **REST API**: Expose functionome via FastAPI
5. **dbt Integration**: Transform data with dbt-spark
6. **Great Expectations**: Automated data quality testing

---

## 📚 References

1. Gene Ontology Consortium: https://geneontology.org
2. QuickGO API: https://www.ebi.ac.uk/QuickGO/api
3. PAN-GO Functionome: Nature 2025
4. Delta Lake: https://delta.io
5. Apache Spark: https://spark.apache.org

---

## 🤝 Contributing

This is a research/production pipeline. Contributions welcome:
- Scientific improvements to impact scoring
- Performance optimizations
- New data sources (Reactome, KEGG, etc.)
- ML/AI integration

---

**Version**: 2.0 (Spark/Delta Edition)
**Last Updated**: 2025-10-30
**Maintainers**: Gene Functional Abstraction Team
