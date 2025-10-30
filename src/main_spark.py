"""
Main Orchestrator for Spark-based Gene Ontology Pipeline
Runs ingestion → processing → analytics → visualization
"""

import sys
import os
import logging
from datetime import datetime
from typing import Optional

# Add config to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'config'))
from go_config import SPARK_CONFIG, LOCAL_STORAGE_PATHS, STORAGE_PATHS

try:
    from pyspark.sql import SparkSession
    from ingestion_spark import GOIngestionSpark
    from processing_spark import GOProcessingSpark
    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False
    print("ERROR: PySpark not installed. Run: pip install pyspark delta-spark")
    sys.exit(1)


class GOPipelineOrchestrator:
    """
    Orchestrates the complete Gene Ontology pipeline
    Supports both local and Databricks execution
    """

    def __init__(self, is_databricks: bool = False, enable_streaming: bool = False):
        """
        Initialize pipeline orchestrator

        Args:
            is_databricks: Whether running in Databricks environment
            enable_streaming: Enable structured streaming mode
        """
        self.is_databricks = is_databricks
        self.enable_streaming = enable_streaming
        self.storage_paths = STORAGE_PATHS if is_databricks else LOCAL_STORAGE_PATHS

        # Configure logging
        self._setup_logging()

        # Initialize components
        self.logger.info("Initializing Gene Ontology Pipeline...")
        self.ingestion = GOIngestionSpark(
            is_databricks=is_databricks,
            enable_streaming=enable_streaming
        )
        self.processing = GOProcessingSpark(
            spark=self.ingestion.spark,
            is_databricks=is_databricks
        )

        self.logger.info("✓ Pipeline initialized successfully")

    def _setup_logging(self):
        """Configure logging"""
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        self.logger = logging.getLogger("GO-Pipeline")

    def run_full_pipeline(self, mode: str = "batch") -> dict:
        """
        Run complete pipeline: Ingestion → Processing → Analytics

        Args:
            mode: Execution mode ('batch' or 'streaming')

        Returns:
            Dictionary with execution results
        """
        start_time = datetime.now()

        self.logger.info("="*80)
        self.logger.info("GENE ONTOLOGY FUNCTIONOME PIPELINE - SPARK EDITION")
        self.logger.info(f"Mode: {mode.upper()}")
        self.logger.info(f"Environment: {'Databricks' if self.is_databricks else 'Local'}")
        self.logger.info(f"Start Time: {start_time}")
        self.logger.info("="*80)

        results = {
            'start_time': start_time,
            'mode': mode,
            'environment': 'databricks' if self.is_databricks else 'local'
        }

        try:
            # ================================================================
            # STAGE 1: INGESTION (Bronze Layer)
            # ================================================================
            self.logger.info("\n[STAGE 1/3] INGESTION - Bronze Layer")
            self.logger.info("-" * 80)

            if mode == "batch":
                ingestion_results = self.ingestion.run_batch_ingestion()
                results['ingestion'] = ingestion_results
                self.logger.info("✓ Batch ingestion completed")

            elif mode == "streaming":
                self.logger.info("⚠ Streaming mode not fully implemented yet")
                self.logger.info("  Falling back to batch ingestion...")
                ingestion_results = self.ingestion.run_batch_ingestion()
                results['ingestion'] = ingestion_results

            # ================================================================
            # STAGE 2: PROCESSING (Silver Layer)
            # ================================================================
            self.logger.info("\n[STAGE 2/3] PROCESSING - Silver Layer")
            self.logger.info("-" * 80)

            bronze_annotations = self.storage_paths['bronze']['annotations']
            bronze_ontology = self.storage_paths['bronze']['ontology']

            # Transform Bronze → Silver
            self.logger.info("Transforming Bronze → Silver...")
            bronze_ann_df = self.processing.spark.read.format("delta").load(bronze_annotations)
            bronze_ont_df = self.processing.spark.read.format("delta").load(bronze_ontology)

            silver_annotations = self.processing.clean_annotations(bronze_ann_df)
            silver_ontology = self.processing.build_ontology_graph(bronze_ont_df)

            results['silver'] = {
                'annotations': silver_annotations,
                'ontology': silver_ontology
            }
            self.logger.info("✓ Silver layer transformation completed")

            # ================================================================
            # STAGE 3: ANALYTICS (Gold Layer)
            # ================================================================
            self.logger.info("\n[STAGE 3/3] ANALYTICS - Gold Layer")
            self.logger.info("-" * 80)

            # Compute functionome metrics
            functionome = self.processing.compute_gene_functionome(silver_annotations)
            enrichment = self.processing.compute_pathway_enrichment(
                silver_annotations, silver_ontology
            )

            results['gold'] = {
                'functionome': functionome,
                'enrichment': enrichment
            }
            self.logger.info("✓ Gold layer analytics completed")

            # ================================================================
            # PIPELINE SUMMARY
            # ================================================================
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()

            self.logger.info("\n" + "="*80)
            self.logger.info("PIPELINE EXECUTION COMPLETE")
            self.logger.info("="*80)
            self.logger.info(f"Duration: {duration:.2f} seconds ({duration/60:.2f} minutes)")
            self.logger.info(f"End Time: {end_time}")

            # Show key metrics
            if 'functionome' in results['gold']:
                gene_count = results['gold']['functionome'].count()
                self.logger.info(f"\nResults:")
                self.logger.info(f"  • Total genes analyzed: {gene_count:,}")

                # Get top gene
                top_gene = results['gold']['functionome'] \
                    .orderBy("functional_impact_index", ascending=False) \
                    .select("gene_symbol", "functional_impact_index") \
                    .first()

                if top_gene:
                    self.logger.info(f"  • Top gene by impact: {top_gene['gene_symbol']} "
                                   f"(score: {top_gene['functional_impact_index']:.2f})")

            results['end_time'] = end_time
            results['duration_seconds'] = duration
            results['status'] = 'success'

            return results

        except Exception as e:
            self.logger.error(f"Pipeline failed: {e}")
            import traceback
            traceback.print_exc()

            results['status'] = 'failed'
            results['error'] = str(e)
            return results

    def export_results_to_parquet(self, output_dir: str = "./output_parquet"):
        """
        Export Gold layer results to Parquet for downstream consumption

        Args:
            output_dir: Output directory for Parquet files
        """
        self.logger.info(f"\n[EXPORT] Exporting results to Parquet: {output_dir}")

        try:
            # Read Gold layer data
            functionome_path = self.storage_paths['gold']['functional_impact']
            enrichment_path = self.storage_paths['gold']['pathway_enrichment']

            functionome_df = self.processing.spark.read.format("delta").load(functionome_path)
            enrichment_df = self.processing.spark.read.format("delta").load(enrichment_path)

            # Export to Parquet
            os.makedirs(output_dir, exist_ok=True)

            functionome_df.write.mode("overwrite") \
                .parquet(os.path.join(output_dir, "gene_functionome.parquet"))

            enrichment_df.write.mode("overwrite") \
                .parquet(os.path.join(output_dir, "pathway_enrichment.parquet"))

            self.logger.info("✓ Results exported successfully")
            self.logger.info(f"  • {output_dir}/gene_functionome.parquet")
            self.logger.info(f"  • {output_dir}/pathway_enrichment.parquet")

        except Exception as e:
            self.logger.error(f"Export failed: {e}")


def main():
    """Main entry point for local execution"""
    import argparse

    parser = argparse.ArgumentParser(description="Gene Ontology Functionome Pipeline")
    parser.add_argument(
        '--mode', type=str, default='batch',
        choices=['batch', 'streaming'],
        help='Execution mode (default: batch)'
    )
    parser.add_argument(
        '--databricks', action='store_true',
        help='Enable Databricks mode'
    )
    parser.add_argument(
        '--export', action='store_true',
        help='Export results to Parquet after processing'
    )
    parser.add_argument(
        '--output-dir', type=str, default='./output_parquet',
        help='Output directory for Parquet export'
    )

    args = parser.parse_args()

    # Initialize and run pipeline
    orchestrator = GOPipelineOrchestrator(
        is_databricks=args.databricks,
        enable_streaming=(args.mode == 'streaming')
    )

    # Run pipeline
    results = orchestrator.run_full_pipeline(mode=args.mode)

    # Export if requested
    if args.export and results.get('status') == 'success':
        orchestrator.export_results_to_parquet(args.output_dir)

    # Return exit code
    sys.exit(0 if results.get('status') == 'success' else 1)


if __name__ == "__main__":
    main()
