"""
Spark-compatible Visualization Dashboard
Reads from Parquet/Delta format for big data compatibility
"""

import sys
import os
from typing import Optional

# Add config to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'config'))
from go_config import LOCAL_STORAGE_PATHS, STORAGE_PATHS

try:
    import pandas as pd
    import plotly.express as px
    import plotly.graph_objects as go
    from dash import Dash, dcc, html, Input, Output, dash_table
    import dash_bootstrap_components as dbc
    DASH_AVAILABLE = True
except ImportError:
    DASH_AVAILABLE = False
    print("Warning: Dash/Plotly not installed. Install with: pip install dash plotly")

try:
    from pyspark.sql import SparkSession
    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False


class GOVisualizationSpark:
    """
    Interactive dashboard for Gene Ontology functionome data
    Supports reading from Parquet and Delta Lake formats
    """

    def __init__(self, data_path: str, format: str = "parquet",
                 is_databricks: bool = False, spark: Optional[SparkSession] = None):
        """
        Initialize visualization dashboard

        Args:
            data_path: Path to functionome data (Parquet or Delta)
            format: Data format ('parquet' or 'delta')
            is_databricks: Whether running in Databricks
            spark: Optional SparkSession (required for Delta format)
        """
        if not DASH_AVAILABLE:
            raise ImportError("Dash and Plotly are required. Install with: pip install dash plotly")

        self.data_path = data_path
        self.format = format
        self.is_databricks = is_databricks
        self.spark = spark

        # Load data
        self.df = self._load_data()

        # Initialize Dash app
        self.app = Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
        self._build_layout()
        self._register_callbacks()

    def _load_data(self) -> pd.DataFrame:
        """
        Load functionome data from Parquet or Delta format

        Returns:
            Pandas DataFrame with functionome data
        """
        print(f"Loading data from {self.data_path} (format: {self.format})...")

        if self.format == "delta":
            if not PYSPARK_AVAILABLE or self.spark is None:
                raise ValueError("Spark session required for Delta format")

            # Read Delta table and convert to Pandas
            spark_df = self.spark.read.format("delta").load(self.data_path)
            df = spark_df.toPandas()

        elif self.format == "parquet":
            # Read Parquet directly with Pandas
            df = pd.read_parquet(self.data_path)

        else:
            raise ValueError(f"Unsupported format: {self.format}")

        print(f"✓ Loaded {len(df):,} gene records")

        # Ensure required columns exist
        required_cols = [
            'gene_symbol', 'functional_impact_index',
            'biological_process_count', 'molecular_function_count',
            'cellular_component_count'
        ]

        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            print(f"Warning: Missing columns: {missing_cols}")

        return df

    def _build_layout(self):
        """Build Dash app layout"""
        self.app.layout = dbc.Container([
            # Header
            dbc.Row([
                dbc.Col([
                    html.H1("🧬 Gene Ontology Functionome Dashboard",
                           className="text-center mb-4 mt-4"),
                    html.P(
                        f"Interactive analysis of {len(self.df):,} genes with functional impact metrics",
                        className="text-center text-muted mb-4"
                    ),
                ])
            ]),

            # Tabs
            dbc.Tabs([
                # Tab 1: Overview
                dbc.Tab(label="📊 Overview", children=[
                    dbc.Row([
                        dbc.Col([
                            html.H4("Summary Statistics", className="mt-4 mb-3"),
                            html.Div(id="summary-stats")
                        ], width=12)
                    ]),
                    dbc.Row([
                        dbc.Col([
                            html.H4("Top Genes by Functional Impact", className="mt-4 mb-3"),
                            dcc.Graph(id="top-genes-bar")
                        ], width=12)
                    ])
                ]),

                # Tab 2: Distribution Analysis
                dbc.Tab(label="📈 Distributions", children=[
                    dbc.Row([
                        dbc.Col([
                            html.H4("Metric Distribution", className="mt-4 mb-3"),
                            html.Label("Select Metric:"),
                            dcc.Dropdown(
                                id="histogram-metric",
                                options=[
                                    {"label": "Functional Impact Index", "value": "functional_impact_index"},
                                    {"label": "Biological Processes", "value": "biological_process_count"},
                                    {"label": "Molecular Functions", "value": "molecular_function_count"},
                                    {"label": "Cellular Components", "value": "cellular_component_count"},
                                    {"label": "Unique GO Terms", "value": "unique_go_terms"}
                                ],
                                value="functional_impact_index"
                            ),
                            dcc.Graph(id="histogram")
                        ], width=12)
                    ])
                ]),

                # Tab 3: Scatter Analysis
                dbc.Tab(label="🔬 Correlation Analysis", children=[
                    dbc.Row([
                        dbc.Col([
                            html.H4("Multi-dimensional Analysis", className="mt-4 mb-3"),
                            dcc.Graph(id="scatter-plot")
                        ], width=12)
                    ])
                ]),

                # Tab 4: Interactive Filtering
                dbc.Tab(label="🔍 Filter & Explore", children=[
                    dbc.Row([
                        dbc.Col([
                            html.H4("Filter Genes", className="mt-4 mb-3"),
                            html.Label("Minimum Functional Impact:"),
                            dcc.Slider(
                                id="impact-slider",
                                min=float(self.df['functional_impact_index'].min()),
                                max=float(self.df['functional_impact_index'].max()),
                                value=float(self.df['functional_impact_index'].quantile(0.75)),
                                marks=None,
                                tooltip={"placement": "bottom", "always_visible": True}
                            ),
                            html.Div(id="filtered-count", className="mt-3 mb-3"),
                            html.Div([
                                dash_table.DataTable(
                                    id="filtered-table",
                                    columns=[
                                        {"name": "Gene Symbol", "id": "gene_symbol"},
                                        {"name": "Gene Name", "id": "gene_name"},
                                        {"name": "Impact Index", "id": "functional_impact_index"},
                                        {"name": "GO Terms", "id": "unique_go_terms"},
                                        {"name": "Quality", "id": "quality_tier"}
                                    ],
                                    page_size=20,
                                    sort_action="native",
                                    filter_action="native",
                                    style_table={'overflowX': 'auto'},
                                    style_cell={'textAlign': 'left'},
                                    style_header={
                                        'backgroundColor': 'rgb(230, 230, 230)',
                                        'fontWeight': 'bold'
                                    }
                                )
                            ])
                        ], width=12)
                    ])
                ]),

                # Tab 5: Data Export
                dbc.Tab(label="💾 Export", children=[
                    dbc.Row([
                        dbc.Col([
                            html.H4("Data Export Options", className="mt-4 mb-3"),
                            html.P(f"Data Source: {self.data_path}"),
                            html.P(f"Format: {self.format.upper()}"),
                            html.P(f"Total Records: {len(self.df):,}"),
                            html.Div([
                                html.Button("Download CSV", id="download-csv-btn",
                                          className="btn btn-primary mt-3"),
                                dcc.Download(id="download-csv")
                            ])
                        ], width=12)
                    ])
                ])
            ])
        ], fluid=True)

    def _register_callbacks(self):
        """Register Dash callbacks for interactivity"""

        # Summary statistics
        @self.app.callback(
            Output("summary-stats", "children"),
            Input("histogram-metric", "value")
        )
        def update_summary(metric):
            stats = self.df['functional_impact_index'].describe()
            return dbc.Table([
                html.Thead([
                    html.Tr([html.Th("Statistic"), html.Th("Value")])
                ]),
                html.Tbody([
                    html.Tr([html.Td("Total Genes"), html.Td(f"{len(self.df):,}")]),
                    html.Tr([html.Td("Mean Impact"), html.Td(f"{stats['mean']:.2f}")]),
                    html.Tr([html.Td("Std Dev"), html.Td(f"{stats['std']:.2f}")]),
                    html.Tr([html.Td("Min"), html.Td(f"{stats['min']:.2f}")]),
                    html.Tr([html.Td("25th Percentile"), html.Td(f"{stats['25%']:.2f}")]),
                    html.Tr([html.Td("Median"), html.Td(f"{stats['50%']:.2f}")]),
                    html.Tr([html.Td("75th Percentile"), html.Td(f"{stats['75%']:.2f}")]),
                    html.Tr([html.Td("Max"), html.Td(f"{stats['max']:.2f}")])
                ])
            ], bordered=True, hover=True, striped=True)

        # Top genes bar chart
        @self.app.callback(
            Output("top-genes-bar", "figure"),
            Input("histogram-metric", "value")
        )
        def update_bar_chart(metric):
            top_genes = self.df.nlargest(20, 'functional_impact_index')
            fig = px.bar(
                top_genes,
                x='gene_symbol',
                y='functional_impact_index',
                title='Top 20 Genes by Functional Impact',
                labels={'functional_impact_index': 'Functional Impact Index',
                       'gene_symbol': 'Gene Symbol'},
                color='functional_impact_index',
                color_continuous_scale='Viridis'
            )
            fig.update_layout(xaxis_tickangle=-45)
            return fig

        # Histogram
        @self.app.callback(
            Output("histogram", "figure"),
            Input("histogram-metric", "value")
        )
        def update_histogram(metric):
            fig = px.histogram(
                self.df,
                x=metric,
                nbins=50,
                title=f'Distribution of {metric.replace("_", " ").title()}',
                labels={metric: metric.replace("_", " ").title()},
                color_discrete_sequence=['#636EFA']
            )
            return fig

        # Scatter plot
        @self.app.callback(
            Output("scatter-plot", "figure"),
            Input("histogram-metric", "value")
        )
        def update_scatter(metric):
            fig = px.scatter(
                self.df,
                x='biological_process_count',
                y='molecular_function_count',
                size='functional_impact_index',
                color='functional_impact_index',
                hover_data=['gene_symbol', 'gene_name'],
                title='Biological Processes vs Molecular Functions',
                labels={
                    'biological_process_count': 'Biological Process Count',
                    'molecular_function_count': 'Molecular Function Count',
                    'functional_impact_index': 'Impact Index'
                },
                color_continuous_scale='Plasma'
            )
            return fig

        # Filtered table
        @self.app.callback(
            [Output("filtered-table", "data"),
             Output("filtered-count", "children")],
            Input("impact-slider", "value")
        )
        def update_filtered_table(min_impact):
            filtered = self.df[self.df['functional_impact_index'] >= min_impact]
            count_text = html.P(f"Showing {len(filtered):,} genes with impact ≥ {min_impact:.2f}")
            return filtered.to_dict('records'), count_text

        # CSV download
        @self.app.callback(
            Output("download-csv", "data"),
            Input("download-csv-btn", "n_clicks"),
            prevent_initial_call=True
        )
        def download_csv(n_clicks):
            from dash import dcc
            return dcc.send_data_frame(self.df.to_csv, "gene_functionome.csv", index=False)

    def launch(self, host: str = "0.0.0.0", port: int = 8050, debug: bool = False):
        """
        Launch the dashboard

        Args:
            host: Host address
            port: Port number
            debug: Enable debug mode
        """
        print(f"\n{'='*80}")
        print("GENE ONTOLOGY FUNCTIONOME DASHBOARD")
        print(f"{'='*80}")
        print(f"Data Source: {self.data_path}")
        print(f"Format: {self.format.upper()}")
        print(f"Records: {len(self.df):,}")
        print(f"\n🌐 Dashboard URL: http://{host}:{port}")
        print(f"{'='*80}\n")

        self.app.run_server(host=host, port=port, debug=debug)


def main():
    """Example usage for local development"""
    import argparse

    parser = argparse.ArgumentParser(description="GO Functionome Visualization")
    parser.add_argument(
        '--data-path', type=str,
        default='./output_parquet/gene_functionome.parquet',
        help='Path to functionome data (Parquet or Delta)'
    )
    parser.add_argument(
        '--format', type=str, default='parquet',
        choices=['parquet', 'delta'],
        help='Data format'
    )
    parser.add_argument(
        '--port', type=int, default=8050,
        help='Port number (default: 8050)'
    )
    parser.add_argument(
        '--debug', action='store_true',
        help='Enable debug mode'
    )

    args = parser.parse_args()

    # Initialize visualization
    viz = GOVisualizationSpark(
        data_path=args.data_path,
        format=args.format,
        is_databricks=False
    )

    # Launch dashboard
    viz.launch(port=args.port, debug=args.debug)


if __name__ == "__main__":
    main()
