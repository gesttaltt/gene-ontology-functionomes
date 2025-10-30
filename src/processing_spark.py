"""
Spark-based Gene Ontology Processing Pipeline
Transforms Bronze → Silver → Gold with functionome metrics
"""

import sys
import os
from typing import Optional, Dict, List
from datetime import datetime

# Add config to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'config'))
from go_config import (
    STORAGE_PATHS, LOCAL_STORAGE_PATHS, GO_ASPECTS,
    EVIDENCE_CODES, QUALITY_RULES
)

try:
    from pyspark.sql import SparkSession, DataFrame
    from pyspark.sql.functions import (
        col, explode, count, countDistinct, sum as spark_sum, avg, stddev,
        min as spark_min, max as spark_max, when, lit, array_contains,
        collect_set, size, array_distinct, struct, row_number,
        dense_rank, percent_rank, current_timestamp, coalesce
    )
    from pyspark.sql.window import Window
    from pyspark.sql.types import DoubleType, IntegerType
    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False


class GOProcessingSpark:
    """
    Spark-based processing for Gene Ontology data
    Implements Bronze → Silver → Gold transformations
    """

    def __init__(self, spark: SparkSession, is_databricks: bool = False):
        """
        Initialize processing pipeline

        Args:
            spark: Active SparkSession
            is_databricks: Whether running in Databricks
        """
        if not PYSPARK_AVAILABLE:
            raise ImportError("PySpark is required")

        self.spark = spark
        self.is_databricks = is_databricks
        self.storage_paths = STORAGE_PATHS if is_databricks else LOCAL_STORAGE_PATHS

    # ========================================================================
    # BRONZE → SILVER: Data Cleaning & Validation
    # ========================================================================

    def clean_annotations(self, bronze_df: DataFrame) -> DataFrame:
        """
        Clean and validate GO annotations (Bronze → Silver)

        Args:
            bronze_df: Raw annotations from Bronze layer

        Returns:
            Cleaned DataFrame for Silver layer
        """
        print("\n[PROCESSING] Cleaning GO annotations (Bronze → Silver)...")

        # 1. Apply quality rules
        df = bronze_df.filter(
            # Remove negative annotations
            ~col("qualifier").contains("NOT") &
            # Remove "No Data" evidence codes
            (col("evidence_code") != "ND") &
            # Ensure required fields are not null
            col("db_object_id").isNotNull() &
            col("go_id").isNotNull() &
            col("evidence_code").isNotNull()
        )

        # 2. Standardize column names
        df = df.select(
            col("db_object_id").alias("gene_id"),
            col("db_object_symbol").alias("gene_symbol"),
            col("db_object_name").alias("gene_name"),
            col("go_id"),
            col("aspect").alias("go_aspect"),
            col("evidence_code"),
            col("db_reference").alias("reference"),
            col("taxon"),
            col("assigned_by"),
            col("date").alias("annotation_date"),
            col("ingestion_timestamp")
        )

        # 3. Add quality flags
        df = df.withColumn(
            "is_experimental",
            col("evidence_code").isin(EVIDENCE_CODES["experimental"])
        ).withColumn(
            "is_phylogenetic",
            col("evidence_code").isin(EVIDENCE_CODES["phylogenetic"])
        ).withColumn(
            "is_computational",
            col("evidence_code").isin(EVIDENCE_CODES["computational"])
        ).withColumn(
            "quality_tier",
            when(col("is_experimental"), lit("high"))
            .when(col("is_phylogenetic"), lit("medium"))
            .otherwise(lit("low"))
        )

        # 4. Add processing metadata
        df = df.withColumn("processed_timestamp", current_timestamp())

        print(f"✓ Cleaned {df.count()} annotations")
        print(f"  Experimental: {df.filter(col('is_experimental')).count()}")
        print(f"  Phylogenetic: {df.filter(col('is_phylogenetic')).count()}")
        print(f"  Computational: {df.filter(col('is_computational')).count()}")

        # Write to Silver layer
        silver_path = self.storage_paths['silver']['gene_annotations']
        self._write_delta(df, silver_path, mode="overwrite",
                         partition_cols=["go_aspect", "quality_tier"])

        print(f"✓ Saved to Silver layer: {silver_path}")
        return df

    def build_ontology_graph(self, bronze_ontology_df: DataFrame) -> DataFrame:
        """
        Process GO ontology structure (Bronze → Silver)

        Args:
            bronze_ontology_df: Raw ontology from Bronze layer

        Returns:
            Processed ontology graph for Silver layer
        """
        print("\n[PROCESSING] Building ontology graph (Bronze → Silver)...")

        # Extract relevant GO term information
        df = bronze_ontology_df.filter(
            col("type") == "CLASS"
        ).select(
            col("id").alias("go_id"),
            col("lbl").alias("go_term"),
            col("meta.definition.val").alias("definition"),
            col("meta.basicPropertyValues").alias("properties"),
            col("meta.synonyms").alias("synonyms")
        )

        # Add GO aspect classification
        df = df.withColumn(
            "go_aspect",
            when(col("go_id").startswith("GO:0008150"), lit("biological_process"))
            .when(col("go_id").startswith("GO:0003674"), lit("molecular_function"))
            .when(col("go_id").startswith("GO:0005575"), lit("cellular_component"))
            .otherwise(lit("unknown"))
        )

        # Add metrics
        df = df.withColumn(
            "property_count",
            coalesce(size(col("properties")), lit(0))
        ).withColumn(
            "synonym_count",
            coalesce(size(col("synonyms")), lit(0))
        )

        print(f"✓ Processed {df.count()} GO terms")

        # Write to Silver layer
        silver_path = self.storage_paths['silver']['ontology_terms']
        self._write_delta(df, silver_path, mode="overwrite",
                         partition_cols=["go_aspect"])

        print(f"✓ Saved to Silver layer: {silver_path}")
        return df

    # ========================================================================
    # SILVER → GOLD: Functionome Metrics & Analytics
    # ========================================================================

    def compute_gene_functionome(self, silver_annotations_df: DataFrame) -> DataFrame:
        """
        Compute PAN-GO functionome metrics (Silver → Gold)

        Calculates comprehensive functional impact metrics per gene

        Args:
            silver_annotations_df: Cleaned annotations from Silver layer

        Returns:
            Gene-level functionome metrics for Gold layer
        """
        print("\n[ANALYTICS] Computing gene functionome metrics (Silver → Gold)...")

        # 1. Aggregate by gene
        gene_metrics = silver_annotations_df.groupBy("gene_id", "gene_symbol", "gene_name").agg(
            # Basic counts
            countDistinct("go_id").alias("unique_go_terms"),
            count("go_id").alias("total_annotations"),

            # Aspect-specific counts
            spark_sum(when(col("go_aspect") == "P", 1).otherwise(0)).alias("biological_process_count"),
            spark_sum(when(col("go_aspect") == "F", 1).otherwise(0)).alias("molecular_function_count"),
            spark_sum(when(col("go_aspect") == "C", 1).otherwise(0)).alias("cellular_component_count"),

            # Evidence quality metrics
            spark_sum(when(col("is_experimental"), 1).otherwise(0)).alias("experimental_evidence_count"),
            spark_sum(when(col("is_phylogenetic"), 1).otherwise(0)).alias("phylogenetic_evidence_count"),
            spark_sum(when(col("is_computational"), 1).otherwise(0)).alias("computational_evidence_count"),

            # Unique aspects
            countDistinct(when(col("go_aspect").isNotNull(), col("go_aspect"))).alias("aspect_diversity"),

            # Collect all GO terms for downstream analysis
            collect_set("go_id").alias("go_term_list")
        )

        # 2. Compute Functional Impact Index (weighted scoring)
        gene_metrics = gene_metrics.withColumn(
            "functional_impact_index",
            (
                col("biological_process_count") * 2.0 +  # Higher weight for processes
                col("molecular_function_count") * 2.0 +
                col("cellular_component_count") * 1.0 +
                col("experimental_evidence_count") * 1.5  # Bonus for experimental evidence
            ).cast(DoubleType())
        )

        # 3. Compute normalized scores (0-1 range)
        max_impact = gene_metrics.agg(spark_max("functional_impact_index")).collect()[0][0]

        gene_metrics = gene_metrics.withColumn(
            "normalized_impact_score",
            (col("functional_impact_index") / lit(max_impact)).cast(DoubleType())
        )

        # 4. Compute functional diversity score (Shannon-like)
        gene_metrics = gene_metrics.withColumn(
            "functional_diversity_score",
            (
                col("aspect_diversity") * col("unique_go_terms")
            ).cast(DoubleType())
        )

        # 5. Add percentile ranks
        window_spec = Window.orderBy(col("functional_impact_index").desc())

        gene_metrics = gene_metrics.withColumn(
            "impact_percentile",
            percent_rank().over(window_spec)
        ).withColumn(
            "impact_rank",
            dense_rank().over(window_spec)
        )

        # 6. Add quality tier classification
        gene_metrics = gene_metrics.withColumn(
            "quality_tier",
            when(col("experimental_evidence_count") >= 5, lit("high_confidence"))
            .when(col("phylogenetic_evidence_count") >= 3, lit("medium_confidence"))
            .otherwise(lit("low_confidence"))
        )

        # 7. Add processing metadata
        gene_metrics = gene_metrics.withColumn(
            "computed_timestamp",
            current_timestamp()
        )

        print(f"✓ Computed functionome for {gene_metrics.count()} genes")

        # Show summary statistics
        stats = gene_metrics.select(
            avg("functional_impact_index").alias("avg_impact"),
            stddev("functional_impact_index").alias("stddev_impact"),
            spark_min("functional_impact_index").alias("min_impact"),
            spark_max("functional_impact_index").alias("max_impact"),
            avg("unique_go_terms").alias("avg_go_terms")
        ).collect()[0]

        print(f"  Avg Impact Index: {stats['avg_impact']:.2f}")
        print(f"  StdDev Impact: {stats['stddev_impact']:.2f}")
        print(f"  Range: [{stats['min_impact']:.2f}, {stats['max_impact']:.2f}]")
        print(f"  Avg GO Terms/Gene: {stats['avg_go_terms']:.2f}")

        # Write to Gold layer
        gold_path = self.storage_paths['gold']['functional_impact']
        self._write_delta(gene_metrics, gold_path, mode="overwrite")

        print(f"✓ Saved to Gold layer: {gold_path}")
        return gene_metrics

    def compute_pathway_enrichment(self, silver_annotations_df: DataFrame,
                                   silver_ontology_df: DataFrame) -> DataFrame:
        """
        Compute pathway/process enrichment metrics (Silver → Gold)

        Args:
            silver_annotations_df: Cleaned annotations
            silver_ontology_df: Ontology graph

        Returns:
            Pathway enrichment metrics for Gold layer
        """
        print("\n[ANALYTICS] Computing pathway enrichment (Silver → Gold)...")

        # Join annotations with ontology to get term names
        df = silver_annotations_df.join(
            silver_ontology_df.select("go_id", "go_term", "go_aspect"),
            on="go_id",
            how="inner"
        )

        # Aggregate by GO term
        pathway_metrics = df.groupBy("go_id", "go_term", "go_aspect").agg(
            countDistinct("gene_id").alias("gene_count"),
            count("gene_id").alias("annotation_count"),
            spark_sum(when(col("is_experimental"), 1).otherwise(0)).alias("experimental_count"),
            collect_set("gene_id").alias("gene_list")
        )

        # Add enrichment score (simple metric: gene count * evidence quality)
        pathway_metrics = pathway_metrics.withColumn(
            "enrichment_score",
            (col("gene_count") * (1 + col("experimental_count") * 0.1)).cast(DoubleType())
        )

        print(f"✓ Computed enrichment for {pathway_metrics.count()} pathways")

        # Write to Gold layer
        gold_path = self.storage_paths['gold']['pathway_enrichment']
        self._write_delta(pathway_metrics, gold_path, mode="overwrite",
                         partition_cols=["go_aspect"])

        print(f"✓ Saved to Gold layer: {gold_path}")
        return pathway_metrics

    # ========================================================================
    # Utility Methods
    # ========================================================================

    def _write_delta(self, df: DataFrame, path: str, mode: str = "overwrite",
                     partition_cols: Optional[List[str]] = None):
        """Write DataFrame to Delta Lake"""
        writer = df.write.format("delta").mode(mode)
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
        writer.save(path)

    def run_full_pipeline(self, bronze_annotations_path: str,
                         bronze_ontology_path: str) -> Dict[str, DataFrame]:
        """
        Run complete Bronze → Silver → Gold pipeline

        Args:
            bronze_annotations_path: Path to Bronze annotations
            bronze_ontology_path: Path to Bronze ontology

        Returns:
            Dictionary of Gold layer DataFrames
        """
        print("\n" + "="*70)
        print("GENE ONTOLOGY PROCESSING PIPELINE (SPARK)")
        print("Bronze → Silver → Gold Transformation")
        print("="*70)

        results = {}

        # Load Bronze data
        print("\n[LOAD] Reading Bronze layer data...")
        bronze_annotations = self.spark.read.format("delta").load(bronze_annotations_path)
        bronze_ontology = self.spark.read.format("delta").load(bronze_ontology_path)
        print(f"✓ Loaded {bronze_annotations.count()} annotations")
        print(f"✓ Loaded {bronze_ontology.count()} ontology terms")

        # Silver layer transformations
        silver_annotations = self.clean_annotations(bronze_annotations)
        silver_ontology = self.build_ontology_graph(bronze_ontology)

        # Gold layer analytics
        results['functionome'] = self.compute_gene_functionome(silver_annotations)
        results['enrichment'] = self.compute_pathway_enrichment(
            silver_annotations, silver_ontology
        )

        print("\n" + "="*70)
        print("PROCESSING PIPELINE COMPLETE")
        print("="*70)

        return results


def main():
    """Example usage"""
    from pyspark.sql import SparkSession

    # Initialize Spark
    spark = SparkSession.builder \
        .appName("GO-Processing") \
        .master("local[*]") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    # Initialize processing
    processor = GOProcessingSpark(spark, is_databricks=False)

    # Define paths
    storage = LOCAL_STORAGE_PATHS
    bronze_annotations = storage['bronze']['annotations']
    bronze_ontology = storage['bronze']['ontology']

    # Run pipeline
    try:
        results = processor.run_full_pipeline(bronze_annotations, bronze_ontology)

        # Show samples
        if 'functionome' in results:
            print("\n[SAMPLE] Top 10 genes by functional impact:")
            results['functionome'].orderBy(col("functional_impact_index").desc()) \
                .select("gene_symbol", "gene_name", "functional_impact_index",
                       "unique_go_terms", "quality_tier") \
                .show(10, truncate=False)

    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
