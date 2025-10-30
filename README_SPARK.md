# 🧬 Gene Ontology Functionome Pipeline

**Production-ready big data pipeline for analyzing gene functional impact using Spark + Databricks**

[![Python](https://img.shields.io/badge/Python-3.9%2B-blue)](https://www.python.org/)
[![Spark](https://img.shields.io/badge/Spark-3.5%2B-orange)](https://spark.apache.org/)
[![Delta Lake](https://img.shields.io/badge/Delta%20Lake-3.0%2B-blue)](https://delta.io/)

---

## 🎯 What This Does

Analyzes **human genome functional characteristics** by:
1. Ingesting Gene Ontology (GO) data from official APIs
2. Computing **Functional Impact Index** for each gene
3. Performing pathway enrichment analysis
4. Providing interactive visualization dashboard

**Beyond simple visualization**: This is a full ETL pipeline with data quality, evidence-based scoring, and production deployment capabilities.

---

## ⚡ Quick Start

### Local Development

```bash
# 1. Clone repository
cd gene-functional-abstraction

# 2. Install dependencies
pip install -r requirements.txt

# 3. Run pipeline (takes 5-10 minutes)
python src/main_spark.py --mode batch --export

# 4. Launch dashboard
python src/visualization_spark.py \
  --data-path ./output_parquet/gene_functionome.parquet
```

Dashboard opens at: **http://localhost:8050**

---

## 🏗️ Architecture

### Medallion Design (Bronze → Silver → Gold)

```
GO APIs → Bronze (raw) → Silver (cleaned) → Gold (analytics) → Dashboard
```

- **Bronze**: Raw GO ontology + human genome annotations
- **Silver**: Validated, quality-filtered data
- **Gold**: Functional impact metrics + enrichment scores

**Storage**: Delta Lake (ACID transactions) + Parquet (portability)

---

## 📊 Key Metrics Computed

### Functional Impact Index
Weighted score combining:
- Biological process annotations (weight: 2.0)
- Molecular function annotations (weight: 2.0)
- Cellular component annotations (weight: 1.0)
- Experimental evidence bonus (weight: 1.5)

**Formula**:
```
Impact = (BP × 2) + (MF × 2) + (CC × 1) + (Experimental × 1.5)
```

### Quality Tiers
- **High Confidence**: ≥5 experimental evidence codes
- **Medium Confidence**: ≥3 phylogenetic evidence codes
- **Low Confidence**: Computational/curator annotations only

---

## 🚀 Databricks Deployment

### Setup

1. **Upload Files**:
   ```bash
   databricks fs cp -r config/ dbfs:/mnt/go-pipeline/config/
   databricks fs cp -r src/ dbfs:/mnt/go-pipeline/src/
   ```

2. **Import Notebook**:
   - Upload `databricks/GO_Functionome_Pipeline.py`

3. **Create Cluster**:
   - Runtime: 14.3 LTS (Spark 3.5.0)
   - Workers: 2-8 (autoscaling recommended)
   - Libraries: `delta-spark`, `requests`, `great-expectations`

4. **Run Notebook**:
   - Execute all cells
   - Register Delta tables: `go_gold.gene_functionome`, `go_gold.pathway_enrichment`

### Query Results via SQL

```sql
-- Top genes by functional impact
SELECT gene_symbol, functional_impact_index, unique_go_terms, quality_tier
FROM go_gold.gene_functionome
ORDER BY functional_impact_index DESC
LIMIT 10;

-- Pathway enrichment analysis
SELECT go_term, gene_count, enrichment_score
FROM go_gold.pathway_enrichment
WHERE go_aspect = 'P'
ORDER BY enrichment_score DESC
LIMIT 20;
```

---

## 📂 Project Structure

```
gene-functional-abstraction/
│
├── config/
│   └── go_config.py              # API endpoints, storage paths, Spark config
│
├── src/
│   ├── main_spark.py             # Pipeline orchestrator
│   ├── ingestion_spark.py        # GO data ingestion (Bronze)
│   ├── processing_spark.py       # ETL transformations (Silver → Gold)
│   └── visualization_spark.py    # Dash dashboard
│
├── databricks/
│   └── GO_Functionome_Pipeline.py # Databricks notebook
│
├── data/                          # Local storage (Bronze/Silver/Gold)
├── output_parquet/                # Parquet exports for dashboard
├── requirements.txt               # Python dependencies
├── ARCHITECTURE.md                # Detailed architecture guide
└── README_SPARK.md                # This file
```

---

## 🔬 Data Sources

### Official Gene Ontology Resources

1. **GO Ontology**: http://purl.obolibrary.org/obo/go/go-basic.json
2. **Human Annotations**: https://ftp.ebi.ac.uk/pub/databases/GO/goa/HUMAN/
3. **QuickGO API**: https://www.ebi.ac.uk/QuickGO/api/
4. **BioLink API**: http://api.geneontology.org/api

### Focus: PAN-GO Functionome
Comprehensive human protein-coding gene annotations using phylogenetic inference (Feuermann et al., *Nature* 2025).

---

## 🎨 Dashboard Features

Interactive Dash application with 5 tabs:

1. **📊 Overview**: Summary stats + top genes bar chart
2. **📈 Distributions**: Histograms of impact metrics
3. **🔬 Correlation**: Scatter plots (pathways vs functions)
4. **🔍 Filter & Explore**: Interactive filtering + data table
5. **💾 Export**: Download CSV results

**Performance**: Handles 20,000+ genes smoothly

---

## 🔧 Configuration

Edit `config/go_config.py` to customize:

### API Endpoints
```python
GO_API_BASE = "http://api.geneontology.org/api"
HUMAN_GENOME["goa_human"] = "https://ftp.ebi.ac.uk/..."
```

### Storage Paths
```python
# Local development
LOCAL_STORAGE_PATHS = {
    "bronze": "./data/bronze/",
    "silver": "./data/silver/",
    "gold": "./data/gold/"
}

# Databricks production
STORAGE_PATHS = {
    "bronze": "/dbfs/data/bronze/",
    ...
}
```

### Spark Tuning
```python
SPARK_CONFIG = {
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.files.maxPartitionBytes": "134217728",  # 128MB
    "spark.sql.parquet.compression.codec": "snappy"
}
```

---

## 📊 Usage Examples

### Example 1: Batch Ingestion + Processing

```python
from ingestion_spark import GOIngestionSpark
from processing_spark import GOProcessingSpark

# Initialize
ingestion = GOIngestionSpark(is_databricks=False)
processing = GOProcessingSpark(spark=ingestion.spark)

# Run pipeline
results = ingestion.run_batch_ingestion()
functionome = processing.compute_gene_functionome(results['annotations'])

# Show top genes
functionome.orderBy("functional_impact_index", ascending=False).show(10)
```

### Example 2: Full Pipeline with Export

```bash
python src/main_spark.py \
  --mode batch \
  --export \
  --output-dir ./output_parquet
```

### Example 3: Custom Visualization

```python
from visualization_spark import GOVisualizationSpark

viz = GOVisualizationSpark(
    data_path="./output_parquet/gene_functionome.parquet",
    format="parquet"
)
viz.launch(port=8050)
```

---

## 🔬 Scientific Methodology

### Evidence Code Quality
Annotations ranked by evidence type:
1. **Experimental** (EXP, IDA, IPI, IMP, IGI, IEP) - highest confidence
2. **Phylogenetic** (IBA, IBD, IKR, IRD) - inferred by evolution
3. **Computational** (ISS, ISO, ISA, ISM) - predicted
4. **Curator** (TAS, NAS, IC) - literature-based
5. **Electronic** (IEA) - automated, lowest confidence

### Quality Filtering
- Excludes "NOT" qualifiers (negative annotations)
- Excludes "ND" evidence (No Data available)
- Minimum 1 annotation per gene
- Maximum 10,000 annotations (flag outliers)

---

## 🚦 Performance Benchmarks

### Local Mode (8 core laptop)
- Ingestion: ~3-5 minutes
- Processing: ~2-3 minutes
- Visualization: <1 second load time

### Databricks (4-node cluster)
- Ingestion: ~1-2 minutes
- Processing: ~30-60 seconds
- Query (SQL): <100ms (cached)

### Storage Efficiency
| Format | Size | Read Speed |
|--------|------|------------|
| CSV | 1.0 GB | 10-30s |
| Parquet | 200 MB | 1-3s |
| Delta Lake | 210 MB | <1s (cached) |

---

## 🔮 Future Enhancements

### Short-term
- [ ] Streaming ingestion (Kafka consumer)
- [ ] REST API (FastAPI)
- [ ] Multi-species support (mouse, rat, zebrafish)

### Medium-term
- [ ] Machine learning integration (gene function prediction)
- [ ] Integration with Reactome, KEGG pathways
- [ ] Great Expectations data quality framework

### Long-term
- [ ] Real-time GO annotation updates
- [ ] Network analysis (protein-protein interactions)
- [ ] Clinical variant impact scoring

---

## 🐛 Troubleshooting

### Issue: "PySpark not found"
```bash
pip install pyspark delta-spark
```

### Issue: "Delta Lake not configured"
Add to Spark session:
```python
.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
.config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
```

### Issue: "GO API timeout"
Increase timeout in `ingestion_spark.py`:
```python
response = requests.get(url, timeout=120)  # 2 minutes
```

---

## 📚 References

1. **Gene Ontology Consortium**: https://geneontology.org
2. **GO Documentation**: https://geneontology.org/docs/
3. **PAN-GO Functionome**: Feuermann et al., *Nature* 2025
4. **Delta Lake**: https://delta.io
5. **Apache Spark**: https://spark.apache.org
6. **Databricks**: https://databricks.com

---

## 📄 License

MIT License - see LICENSE file

---

## 🤝 Contributing

Contributions welcome! Areas of interest:
- Scientific validation of impact scoring
- Performance optimizations
- Additional data sources
- ML/AI integration

---

## 📧 Contact

For questions or collaboration:
- Open an issue on GitHub
- Email: [your-email@domain.com]

---

**Version**: 2.0 (Spark/Delta Edition)
**Status**: Production-Ready
**Last Updated**: 2025-10-30
