"""
Production-ready Gene Functional Abstraction Pipeline
Using C++ DAG engine with operation fusion
"""

import pandas as pd
from typing import Optional
import sys
import os

# Try to import C++ DAG engine
try:
    import pyfunctionome_dag as dag
    DAG_AVAILABLE = True
except ImportError:
    print("Warning: C++ DAG engine not available. Install with: cd cpp && mkdir build && cd build && cmake .. && make")
    DAG_AVAILABLE = False

from process_ontology import process_go_json
from ingestion import fetch_go_data


class FunctionomeDAGPipeline:
    """
    High-performance pipeline using C++ DAG with operation fusion
    Falls back to pure Python if DAG not available
    """

    def __init__(self, use_dag: bool = True):
        self.use_dag = use_dag and DAG_AVAILABLE

        if self.use_dag:
            print("✓ Using C++ DAG engine with operation fusion")
        else:
            print("ℹ Using pure Python pipeline")

    def run(self, go_json: Optional[dict] = None) -> pd.DataFrame:
        """
        Run the complete pipeline with validation and error handling

        Args:
            go_json: Gene Ontology JSON data (if None, fetches from API)

        Returns:
            DataFrame with gene functional impact scores
        """

        # Ingest data
        if go_json is None:
            print("Fetching GO data from API...")
            go_json = fetch_go_data()

        # Process using DAG or Python
        if self.use_dag:
            return self._run_with_dag(go_json)
        else:
            return self._run_with_python(go_json)

    def _run_with_dag(self, go_json: dict) -> pd.DataFrame:
        """Run pipeline using C++ DAG engine"""

        # Convert GO JSON to columnar format
        df = process_go_json(go_json)

        # Build DAG graph
        graph = dag.dag.Graph()

        # Create source node - load data into columnar table
        def load_data():
            table = dag.columnar.Table()

            # Add gene column
            gene_col = dag.columnar.StringColumn()
            for gene_name in df['gene']:
                gene_col.append(gene_name)
            table.add_column("gene", gene_col)

            # Add pathways column
            pathways_col = dag.columnar.Int64Column()
            for val in df['pathways']:
                pathways_col.append(int(val))
            table.add_column("pathways", pathways_col)

            # Add interactions column
            interactions_col = dag.columnar.Int64Column()
            for val in df['interactions']:
                interactions_col.append(int(val))
            table.add_column("interactions", interactions_col)

            return table

        source = dag.dag.SourceNode(load_data, "go_source")
        graph.add_node(source)

        # Create map node - compute impact index
        def compute_impact(table):
            # Add impact_index column = pathways + interactions
            impact_col = dag.columnar.Int64Column()

            pathways = table.get_column("pathways")
            interactions = table.get_column("interactions")

            for i in range(table.num_rows()):
                impact = pathways[i] + interactions[i]
                impact_col.append(impact)

            table.add_column("impact_index", impact_col)
            return table

        map_node = dag.ops.MapNode(compute_impact, "compute_impact")
        map_node.add_input(source)
        graph.add_node(map_node)

        # Create filter node - filter genes with impact > threshold
        def filter_high_impact(table, row_idx):
            impact_col = table.get_column("impact_index")
            return impact_col[row_idx] > 5

        filter_node = dag.ops.FilterNode(filter_high_impact, "filter_high_impact")
        filter_node.add_input(map_node)
        graph.add_node(filter_node)

        # Optimize DAG (fuse operations)
        print("\nOptimizing DAG...")
        optimizer = dag.dag.Optimizer()
        optimizer.optimize(graph)

        # Print execution plan
        print("\n" + "="*60)
        graph.print_execution_plan()
        print("="*60)

        # Execute pipeline
        print("\nExecuting pipeline...")
        result_table = graph.execute("filter_high_impact")

        # Convert back to pandas
        print(f"\nPipeline complete. Result: {result_table.num_rows()} rows")
        print("\nResult preview:")
        print(result_table)

        # Convert to pandas DataFrame
        genes = []
        pathways_list = []
        interactions_list = []
        impacts = []

        gene_col = result_table.get_column("gene")
        pathways_col = result_table.get_column("pathways")
        interactions_col = result_table.get_column("interactions")
        impact_col = result_table.get_column("impact_index")

        for i in range(result_table.num_rows()):
            genes.append(gene_col[i])
            pathways_list.append(pathways_col[i])
            interactions_list.append(interactions_col[i])
            impacts.append(impact_col[i])

        return pd.DataFrame({
            'gene': genes,
            'pathways': pathways_list,
            'interactions': interactions_list,
            'impact_index': impacts
        })

    def _run_with_python(self, go_json: dict) -> pd.DataFrame:
        """Run pipeline using pure Python (fallback)"""
        from classification import compute_impact_index

        # Process and classify
        df = process_go_json(go_json)
        df = compute_impact_index(df)

        # Filter high impact genes
        df = df[df['impact_index'] > 5].copy()

        return df


def main():
    """Example usage"""
    import argparse

    parser = argparse.ArgumentParser(description="Gene Functional Abstraction Pipeline with C++ DAG")
    parser.add_argument('--no-dag', action='store_true', help='Disable C++ DAG engine')
    parser.add_argument('--export', type=str, help='Export results to CSV')

    args = parser.parse_args()

    # Run pipeline
    pipeline = FunctionomeDAGPipeline(use_dag=not args.no_dag)
    result = pipeline.run()

    # Export if requested
    if args.export:
        result.to_csv(args.export, index=False)
        print(f"\n✓ Results exported to {args.export}")

    print(f"\n✓ Pipeline complete. Processed {len(result)} genes")
    print("\nTop genes by functional impact:")
    print(result.sort_values('impact_index', ascending=False).head(10))


if __name__ == "__main__":
    main()
