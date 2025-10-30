"""
Spark-based Gene Ontology Ingestion Pipeline
Supports batch and streaming ingestion with Delta Lake storage
Focused on human genome and PAN-GO functionome
"""

import sys
import os
from typing import Optional, Dict, Any
from datetime import datetime
import requests
import gzip
import io

# Add config to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'config'))
from go_config import (
    HUMAN_GENOME, GO_ONTOLOGY, STORAGE_PATHS, LOCAL_STORAGE_PATHS,
    SPARK_CONFIG, CACHE_CONFIG, STREAMING_CONFIG, QUALITY_RULES
)

try:
    from pyspark.sql import SparkSession, DataFrame
    from pyspark.sql.functions import (
        col, explode, from_json, schema_of_json, to_timestamp,
        current_timestamp, lit, when, count, avg, sum as spark_sum,
        struct, collect_list, array_distinct, size, monotonically_increasing_id
    )
    from pyspark.sql.types import (
        StructType, StructField, StringType, IntegerType,
        ArrayType, TimestampType, BooleanType
    )
    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False
    print("Warning: PySpark not installed. Install with: pip install pyspark")


class GOIngestionSpark:
    """
    Spark-based ingestion pipeline for Gene Ontology data
    Supports both local and Databricks execution
    """

    def __init__(self, is_databricks: bool = False, enable_streaming: bool = False):
        """
        Initialize Spark-based GO ingestion pipeline

        Args:
            is_databricks: Whether running in Databricks environment
            enable_streaming: Enable structured streaming mode
        """
        if not PYSPARK_AVAILABLE:
            raise ImportError("PySpark is required. Install with: pip install pyspark")

        self.is_databricks = is_databricks
        self.enable_streaming = enable_streaming
        self.storage_paths = STORAGE_PATHS if is_databricks else LOCAL_STORAGE_PATHS

        # Initialize Spark session
        self.spark = self._create_spark_session()

        # Create storage directories
        self._init_storage()

    def _create_spark_session(self) -> SparkSession:
        """Create and configure Spark session"""
        builder = SparkSession.builder.appName(SPARK_CONFIG["app_name"])

        if not self.is_databricks:
            builder = builder.master(SPARK_CONFIG["master"])

        # Apply Spark configurations
        for key, value in SPARK_CONFIG.items():
            if key not in ["app_name", "master"]:
                builder = builder.config(key, value)

        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel("WARN")

        print(f"✓ Spark session initialized (Databricks: {self.is_databricks})")
        return spark

    def _init_storage(self):
        """Initialize storage directories for Bronze/Silver/Gold layers"""
        for layer in ['bronze', 'silver', 'gold']:
            for storage_type, path in self.storage_paths[layer].items():
                if not self.is_databricks:
                    os.makedirs(path, exist_ok=True)
        print("✓ Storage layers initialized (Bronze/Silver/Gold)")

    def fetch_human_genome_annotations(self, format: str = "gaf") -> DataFrame:
        """
        Fetch human genome GO annotations from GOA

        Args:
            format: Annotation format ('gaf', 'gpad', 'gpi')

        Returns:
            Spark DataFrame with raw annotations (Bronze layer)
        """
        print(f"\n[INGESTION] Fetching human genome annotations from GOA...")

        url = HUMAN_GENOME["goa_human"]

        try:
            # Download compressed GAF file
            response = requests.get(url, stream=True)
            response.raise_for_status()

            # Decompress gzip content
            with gzip.GzipFile(fileobj=io.BytesIO(response.content)) as gz:
                content = gz.read().decode('utf-8')

            # Parse GAF format (tab-delimited)
            lines = [line for line in content.split('\n')
                     if line and not line.startswith('!')]

            print(f"✓ Downloaded {len(lines)} annotation records")

            # Create DataFrame from GAF format
            # GAF 2.2 format: https://geneontology.org/docs/go-annotation-file-gaf-format-2.2/
            schema = StructType([
                StructField("db", StringType(), True),
                StructField("db_object_id", StringType(), True),
                StructField("db_object_symbol", StringType(), True),
                StructField("qualifier", StringType(), True),
                StructField("go_id", StringType(), True),
                StructField("db_reference", StringType(), True),
                StructField("evidence_code", StringType(), True),
                StructField("with_from", StringType(), True),
                StructField("aspect", StringType(), True),
                StructField("db_object_name", StringType(), True),
                StructField("db_object_synonym", StringType(), True),
                StructField("db_object_type", StringType(), True),
                StructField("taxon", StringType(), True),
                StructField("date", StringType(), True),
                StructField("assigned_by", StringType(), True),
                StructField("annotation_extension", StringType(), True),
                StructField("gene_product_form_id", StringType(), True),
            ])

            # Parse tab-delimited data
            data = [tuple(line.split('\t')) for line in lines]

            df = self.spark.createDataFrame(data, schema)

            # Add ingestion metadata
            df = df.withColumn("ingestion_timestamp", current_timestamp()) \
                   .withColumn("source", lit("GOA_HUMAN")) \
                   .withColumn("format", lit(format))

            # Write to Bronze layer
            bronze_path = self.storage_paths['bronze']['annotations']
            self._write_delta(df, bronze_path, mode="overwrite", partition_cols=["aspect"])

            print(f"✓ Saved to Bronze layer: {bronze_path}")
            return df

        except Exception as e:
            print(f"✗ Error fetching human genome annotations: {e}")
            raise

    def fetch_go_ontology(self, format: str = "json") -> DataFrame:
        """
        Fetch GO ontology structure

        Args:
            format: Ontology format ('json', 'obo', 'owl')

        Returns:
            Spark DataFrame with ontology terms (Bronze layer)
        """
        print(f"\n[INGESTION] Fetching GO ontology ({format} format)...")

        url = GO_ONTOLOGY.get(f"basic_{format}", GO_ONTOLOGY["basic_json"])

        try:
            response = requests.get(url, timeout=60)
            response.raise_for_status()

            if format == "json":
                data = response.json()

                # Extract nodes (GO terms) from JSON
                graphs = data.get('graphs', [])
                if graphs:
                    nodes = graphs[0].get('nodes', [])

                    # Create DataFrame from nodes
                    df = self.spark.createDataFrame(nodes)

                    # Add metadata
                    df = df.withColumn("ingestion_timestamp", current_timestamp()) \
                           .withColumn("source", lit("GO_BASIC"))

                    # Write to Bronze layer
                    bronze_path = self.storage_paths['bronze']['ontology']
                    self._write_delta(df, bronze_path, mode="overwrite")

                    print(f"✓ Loaded {df.count()} GO terms to Bronze layer")
                    return df

            else:
                raise NotImplementedError(f"Format '{format}' parsing not yet implemented")

        except Exception as e:
            print(f"✗ Error fetching GO ontology: {e}")
            raise

    def fetch_human_proteome(self) -> DataFrame:
        """
        Fetch human proteome data (PAN-GO functionome focus)

        Returns:
            Spark DataFrame with human proteome annotations
        """
        print(f"\n[INGESTION] Fetching human proteome (PAN-GO functionome)...")

        # For production: fetch from UniProt proteome
        # For now: create sample schema for demonstration

        proteome_url = f"{HUMAN_GENOME['uniprot_proteome']}{HUMAN_GENOME['human_proteome_id']}.gaf.gz"
        print(f"Source: {proteome_url}")

        # Note: This requires actual proteome file download
        # Implementation similar to fetch_human_genome_annotations
        print("⚠ Human proteome ingestion requires proteome file download")
        print("  Use fetch_human_genome_annotations() for GOA annotations")

        return None

    def _write_delta(self, df: DataFrame, path: str, mode: str = "overwrite",
                     partition_cols: Optional[list] = None):
        """
        Write DataFrame to Delta Lake format with optimizations

        Args:
            df: Spark DataFrame to write
            path: Target path
            mode: Write mode ('overwrite', 'append', 'merge')
            partition_cols: Columns to partition by
        """
        writer = df.write.format("delta").mode(mode)

        if partition_cols:
            writer = writer.partitionBy(*partition_cols)

        writer.save(path)

    def write_parquet(self, df: DataFrame, path: str, mode: str = "overwrite",
                      partition_cols: Optional[list] = None):
        """
        Write DataFrame to Parquet format (alternative to Delta)

        Args:
            df: Spark DataFrame to write
            path: Target path
            mode: Write mode
            partition_cols: Columns to partition by
        """
        writer = df.write.mode(mode) \
                        .option("compression", "snappy") \
                        .option("mergeSchema", "false")

        if partition_cols:
            writer = writer.partitionBy(*partition_cols)

        writer.parquet(path)
        print(f"✓ Written to Parquet: {path}")

    def read_delta(self, path: str) -> DataFrame:
        """Read DataFrame from Delta Lake"""
        return self.spark.read.format("delta").load(path)

    def read_parquet(self, path: str) -> DataFrame:
        """Read DataFrame from Parquet"""
        return self.spark.read.parquet(path)

    def enable_structured_streaming(self, source_path: str,
                                    checkpoint_path: str) -> DataFrame:
        """
        Enable structured streaming for continuous ingestion

        Args:
            source_path: Source directory for streaming files
            checkpoint_path: Checkpoint location for fault tolerance

        Returns:
            Streaming DataFrame
        """
        print(f"\n[STREAMING] Enabling structured streaming...")
        print(f"  Source: {source_path}")
        print(f"  Checkpoint: {checkpoint_path}")

        # Create streaming DataFrame
        stream_df = self.spark.readStream \
            .format("json") \
            .option("maxFilesPerTrigger", STREAMING_CONFIG["max_files_per_trigger"]) \
            .schema(self._get_annotation_schema()) \
            .load(source_path)

        return stream_df

    def _get_annotation_schema(self) -> StructType:
        """Define schema for GO annotations"""
        return StructType([
            StructField("gene_id", StringType(), False),
            StructField("gene_symbol", StringType(), True),
            StructField("go_id", StringType(), False),
            StructField("go_term", StringType(), True),
            StructField("aspect", StringType(), True),
            StructField("evidence_code", StringType(), True),
            StructField("reference", StringType(), True),
            StructField("taxon_id", StringType(), True),
            StructField("date", StringType(), True),
        ])

    def run_batch_ingestion(self) -> Dict[str, DataFrame]:
        """
        Run complete batch ingestion pipeline

        Returns:
            Dictionary of DataFrames (ontology, annotations, proteome)
        """
        print("\n" + "="*70)
        print("GENE ONTOLOGY BATCH INGESTION PIPELINE (SPARK)")
        print("="*70)

        results = {}

        # 1. Fetch GO ontology
        try:
            results['ontology'] = self.fetch_go_ontology(format="json")
        except Exception as e:
            print(f"Warning: Ontology ingestion failed: {e}")

        # 2. Fetch human genome annotations (PAN-GO functionome)
        try:
            results['annotations'] = self.fetch_human_genome_annotations(format="gaf")
        except Exception as e:
            print(f"Warning: Annotation ingestion failed: {e}")

        print("\n" + "="*70)
        print("BATCH INGESTION COMPLETE")
        print("="*70)

        return results


def main():
    """Example usage for local development"""
    # Initialize ingestion pipeline (local mode)
    ingestion = GOIngestionSpark(is_databricks=False, enable_streaming=False)

    # Run batch ingestion
    results = ingestion.run_batch_ingestion()

    # Show sample data
    if 'annotations' in results:
        print("\n[SAMPLE] Human genome annotations:")
        results['annotations'].show(10, truncate=False)
        print(f"Total annotations: {results['annotations'].count()}")

    if 'ontology' in results:
        print("\n[SAMPLE] GO ontology terms:")
        results['ontology'].select("id", "lbl", "type").show(10, truncate=False)
        print(f"Total terms: {results['ontology'].count()}")


if __name__ == "__main__":
    main()
