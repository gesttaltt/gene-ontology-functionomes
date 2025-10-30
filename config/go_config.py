"""
Gene Ontology API Configuration
Centralized configuration for GO data sources, focusing on human genome and functionomes
"""

# ============================================================================
# Gene Ontology API Endpoints
# ============================================================================

# Primary GO API (BioLink Model)
GO_API_BASE = "http://api.geneontology.org/api"

# QuickGO REST API (EBI)
QUICKGO_API_BASE = "https://www.ebi.ac.uk/QuickGO/services"

# GO Ontology Files
GO_ONTOLOGY = {
    "basic_json": "http://purl.obolibrary.org/obo/go/go-basic.json",
    "basic_obo": "http://purl.obolibrary.org/obo/go/go-basic.obo",
    "full_owl": "http://purl.obolibrary.org/obo/go.owl"
}

# ============================================================================
# Human Genome Annotations (PAN-GO Functionome)
# ============================================================================

# Human genome-specific annotation sources
HUMAN_GENOME = {
    # UniProt Human Proteome (recommended for PAN-GO functionome)
    "uniprot_proteome": "https://ftp.ebi.ac.uk/pub/databases/GO/goa/proteomes/",
    "human_proteome_id": "9606",  # Homo sapiens taxon ID

    # GOA Human Annotations (compressed GAF format)
    "goa_human": "https://ftp.ebi.ac.uk/pub/databases/GO/goa/HUMAN/goa_human.gaf.gz",

    # NCBI RefSeq (gene-centric annotations)
    "ncbi_refseq": "https://ftp.ncbi.nlm.nih.gov/gene/DATA/GENE_INFO/Mammalia/",
    "ncbi_gene_ontology": "Homo_sapiens.gene_info.gz",

    # Complex Portal (protein complexes)
    "complex_portal": "https://ftp.ebi.ac.uk/pub/databases/intact/complex/current/complextab/9606.tsv"
}

# ============================================================================
# Streaming Configuration
# ============================================================================

# For future real-time updates (GO releases are periodic)
STREAMING_CONFIG = {
    "checkpoint_location": "/dbfs/checkpoints/go_annotations",
    "trigger_interval": "1 day",  # GO updates are not real-time
    "max_files_per_trigger": 1,
    "schema_evolution": "addNewColumns"
}

# ============================================================================
# Data Lake Storage Paths (Databricks DBFS)
# ============================================================================

# Delta Lake Bronze/Silver/Gold architecture
STORAGE_PATHS = {
    # Bronze Layer (raw ingested data)
    "bronze": {
        "ontology": "/dbfs/data/bronze/go_ontology/",
        "annotations": "/dbfs/data/bronze/go_annotations/",
        "human_proteome": "/dbfs/data/bronze/human_proteome/"
    },

    # Silver Layer (cleaned, validated data)
    "silver": {
        "ontology_terms": "/dbfs/data/silver/ontology_terms/",
        "gene_annotations": "/dbfs/data/silver/gene_annotations/",
        "functionome": "/dbfs/data/silver/functionome/"
    },

    # Gold Layer (aggregated, business-ready metrics)
    "gold": {
        "functional_impact": "/dbfs/data/gold/functional_impact/",
        "gene_metrics": "/dbfs/data/gold/gene_metrics/",
        "pathway_enrichment": "/dbfs/data/gold/pathway_enrichment/"
    }
}

# ============================================================================
# Local Development Paths (non-Databricks)
# ============================================================================

LOCAL_STORAGE_PATHS = {
    "bronze": {
        "ontology": "./data/bronze/go_ontology/",
        "annotations": "./data/bronze/go_annotations/",
        "human_proteome": "./data/bronze/human_proteome/"
    },
    "silver": {
        "ontology_terms": "./data/silver/ontology_terms/",
        "gene_annotations": "./data/silver/gene_annotations/",
        "functionome": "./data/silver/functionome/"
    },
    "gold": {
        "functional_impact": "./data/gold/functional_impact/",
        "gene_metrics": "./data/gold/gene_metrics/",
        "pathway_enrichment": "./data/gold/pathway_enrichment/"
    }
}

# ============================================================================
# Spark Configuration
# ============================================================================

SPARK_CONFIG = {
    "app_name": "GO-Functionome-Pipeline",
    "master": "local[*]",  # Override in Databricks

    # Delta Lake optimizations
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",

    # Performance tuning
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.sql.files.maxPartitionBytes": "134217728",  # 128MB

    # Parquet optimizations
    "spark.sql.parquet.compression.codec": "snappy",
    "spark.sql.parquet.mergeSchema": "false",
    "spark.sql.parquet.filterPushdown": "true"
}

# ============================================================================
# GO Term Categories for Functionome Analysis
# ============================================================================

GO_ASPECTS = {
    "biological_process": "P",
    "molecular_function": "F",
    "cellular_component": "C"
}

# Evidence codes for annotation quality filtering
# https://geneontology.org/docs/guide-go-evidence-codes/
EVIDENCE_CODES = {
    "experimental": ["EXP", "IDA", "IPI", "IMP", "IGI", "IEP"],
    "phylogenetic": ["IBA", "IBD", "IKR", "IRD"],
    "computational": ["ISS", "ISO", "ISA", "ISM", "IGC", "RCA"],
    "author": ["TAS", "NAS"],
    "curator": ["IC", "ND"],
    "electronic": ["IEA"]
}

# ============================================================================
# API Query Parameters
# ============================================================================

QUICKGO_PARAMS = {
    "taxonId": "9606",  # Homo sapiens
    "limit": 100,
    "page": 1,
    "includeFields": "goId,goName,goAspect,symbol,qualifier,evidenceCode,reference,taxonId"
}

# ============================================================================
# Cache Configuration
# ============================================================================

CACHE_CONFIG = {
    "enabled": True,
    "ttl_hours": 24,  # GO updates are not frequent
    "local_cache_dir": "./cache/",
    "dbfs_cache_dir": "/dbfs/cache/go_data/"
}

# ============================================================================
# Data Quality Rules
# ============================================================================

QUALITY_RULES = {
    "min_annotations_per_gene": 1,
    "max_annotations_per_gene": 10000,  # Flag potential errors
    "required_fields": ["gene_id", "gene_symbol", "go_id", "go_term", "evidence_code"],
    "exclude_evidence_codes": ["ND"],  # No biological Data available
    "exclude_qualifiers": ["NOT"]  # Negative annotations
}
