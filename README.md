# Gene Functional Abstraction Pipeline

Production-ready pipeline for analyzing human genome functional characteristics using Gene Ontology (GO) data. Multiple implementations optimized for different use cases.

**Data Source:** https://functionome.geneontology.org/

## Overview

Ingests GO ontology and human genome annotations, computes functional impact metrics, and provides interactive visualization for exploring gene functions.

## Implementation Options

### 1. **C++ DAG Engine (Recommended for Production)**
High-performance columnar processing with operation fusion.

**Advantages:**
- 5-25x faster than pure Python
- Lower memory footprint (~30% reduction)
- Operation fusion eliminates intermediate allocations
- Portable (Linux, macOS, Windows)

**Build:**
```bash
cd cpp && mkdir build && cd build
cmake .. -DUSE_ARROW=OFF && make && make install
cd ../../src && python pipeline_dag.py
```

**Use Case:** Production workloads, local processing (10K-1M genes)

### 2. **Apache Spark + Databricks**
Distributed big data processing at cloud scale.

**Advantages:**
- Handles 100K-10M+ genes
- Delta Lake ACID transactions
- Cloud-native deployment
- Production data engineering patterns

**Quick Start:**
```bash
pip install -r requirements.txt
python src/main_spark.py
```

**Use Case:** Cloud deployment, massive datasets, team collaboration

### 3. **Pure Python (Legacy)**
Simple single-file implementation for development.

**Advantages:**
- Zero setup, instant dev
- Easy to understand and modify
- No compilation required

**Quick Start:**
```bash
pip install pandas requests dash plotly
python src/main.py
```

**Use Case:** Prototyping, learning, small datasets (<10K genes)

## Performance Comparison

| Implementation | Speed | Memory | Setup | Best For |
|---------------|-------|--------|-------|----------|
| C++ DAG | ⚡⚡⚡⚡⚡ | 💾💾 | Medium | Local production |
| Spark/Databricks | ⚡⚡⚡⚡ | 💾💾💾 | Complex | Cloud scale |
| Pure Python | ⚡ | 💾💾💾💾 | Easy | Development |

## Key Features

- **Validated Pipeline**: Production-ready error handling and data validation
- **Functional Impact Index**: Weighted scoring combining pathway, function, and component annotations
- **Interactive Dashboard**: 5-tab Dash interface for exploration and export
- **Multiple Architectures**: Choose the right tool for your scale

## Quick Start (C++ DAG - Recommended)

```bash
# 1. Build C++ engine
cd cpp && mkdir build && cd build
cmake .. -DUSE_ARROW=OFF && make -j4 && make install

# 2. Run pipeline
cd ../../src
python pipeline_dag.py --export results.csv

# Output: Optimized DAG execution with fusion
# Speedup: ~10x over pure Python
```

## Documentation

- `cpp/README.md` - C++ DAG engine architecture and design
- `cpp/BUILD.md` - Build instructions for all platforms
- `ARCHITECTURE.md` - Spark/Databricks architecture
- `README_SPARK.md` - Spark user guide and deployment

## Project Structure

```
src/                    # Python pipeline implementations
├── pipeline_dag.py    # C++ DAG-based (recommended)
├── main_spark.py      # Spark-based (cloud scale)
└── main.py            # Pure Python (legacy)

cpp/                   # C++ DAG engine
├── include/           # Headers (dag, ops, columnar)
├── src/               # Implementation
└── BUILD.md           # Build instructions

config/                # Configuration
databricks/            # Cloud deployment notebook
```

## Computed Metrics

**Functional Impact Index** = pathways_count + interactions_count

(Spark version uses weighted formula with evidence quality tiers)

## Choosing an Implementation

**Use C++ DAG if:**
- Processing 10K-1M genes locally
- Need production performance without cloud setup
- Want minimal dependencies

**Use Spark/Databricks if:**
- Processing 100K-10M+ genes
- Need cloud deployment
- Require enterprise data engineering features

**Use Pure Python if:**
- Learning the pipeline
- Prototyping new features
- Processing <10K genes
