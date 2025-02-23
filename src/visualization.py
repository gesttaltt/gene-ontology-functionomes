import dash
from dash import dcc, html, Input, Output
import plotly.express as px
import pandas as pd

def launch_dashboard(classified_data):
    """
    Launches an interactive dashboard using Dash to visualize classified gene data.
    
    The dashboard includes:
      - Bar Chart of impact index per gene.
      - Histogram of a selectable column (impact_index, pathways, interactions).
      - Scatter Plot of pathways vs. interactions with marker size by impact_index.
      - Interactive Filtering via a slider on the impact index.
      - Summary Statistics of the dataset.
    
    Parameters:
        classified_data (pandas.DataFrame): DataFrame containing the classified gene data.
    """
    # Pre-compute some figures for static tabs
    fig_bar = px.bar(
        classified_data, 
        x='gene', 
        y='impact_index', 
        title="Impact Index per Gene",
        labels={"impact_index": "Functional Impact Index", "gene": "Gene"}
    )
    
    fig_scatter = px.scatter(
        classified_data, 
        x='pathways', 
        y='interactions', 
        size='impact_index', 
        hover_name='gene',
        title="Pathways vs. Interactions",
        labels={"pathways": "Number of Pathways", "interactions": "Number of Interactions"}
    )
    
    # Initialize the Dash app with a Bootstrap stylesheet for better styling
    app = dash.Dash(__name__, external_stylesheets=[
        'https://stackpath.bootstrapcdn.com/bootstrap/4.5.2/css/bootstrap.min.css'
    ])
    
    # Define the layout using tabs
    app.layout = html.Div([
        html.H1("Gene Functional Abstraction Dashboard", style={'textAlign': 'center'}),
        dcc.Tabs(id="tabs", value="tab-bar", children=[
            dcc.Tab(label="Bar Chart", value="tab-bar", children=[
                dcc.Graph(id="bar-chart", figure=fig_bar)
            ]),
            dcc.Tab(label="Histogram", value="tab-hist", children=[
                html.Div([
                    html.Label("Select Column for Histogram:"),
                    dcc.Dropdown(
                        id="histogram-dropdown",
                        options=[
                            {"label": "Impact Index", "value": "impact_index"},
                            {"label": "Pathways", "value": "pathways"},
                            {"label": "Interactions", "value": "interactions"}
                        ],
                        value="impact_index"
                    ),
                    dcc.Graph(id="histogram-graph")
                ], style={'padding': '20px'})
            ]),
            dcc.Tab(label="Scatter Plot", value="tab-scatter", children=[
                dcc.Graph(id="scatter-plot", figure=fig_scatter)
            ]),
            dcc.Tab(label="Interactive Filtering", value="tab-filter", children=[
                html.Div([
                    html.Label("Minimum Impact Index:"),
                    dcc.Slider(
                        id="filter-slider",
                        min=classified_data['impact_index'].min(),
                        max=classified_data['impact_index'].max(),
                        step=1,
                        value=classified_data['impact_index'].min(),
                        marks={int(val): str(int(val)) for val in classified_data['impact_index'].unique()}
                    ),
                    dcc.Graph(id="filter-graph")
                ], style={'padding': '20px'})
            ]),
            dcc.Tab(label="Summary Statistics", value="tab-summary", children=[
                html.Div(id="summary-stats", style={'padding': '20px'})
            ])
        ])
    ], style={'margin': '20px'})
    
    # Callback to update the histogram based on selected column
    @app.callback(
        Output("histogram-graph", "figure"),
        Input("histogram-dropdown", "value")
    )
    def update_histogram(selected_column):
        fig = px.histogram(
            classified_data,
            x=selected_column,
            nbins=20,
            title=f"Histogram of {selected_column.capitalize()}",
            labels={selected_column: selected_column.capitalize()}
        )
        return fig

    # Callback to update the interactive filtering graph based on slider value
    @app.callback(
        Output("filter-graph", "figure"),
        Input("filter-slider", "value")
    )
    def update_filtered_graph(min_impact):
        filtered = classified_data[classified_data['impact_index'] >= min_impact]
        fig = px.bar(
            filtered,
            x='gene',
            y='impact_index',
            title=f"Genes with Impact Index >= {min_impact}",
            labels={"impact_index": "Functional Impact Index", "gene": "Gene"}
        )
        return fig

    # Callback to generate summary statistics as an HTML table
    @app.callback(
        Output("summary-stats", "children"),
        Input("tabs", "value")
    )
    def update_summary(tab_value):
        # Generate summary statistics only when the summary tab is active
        stats = classified_data.describe().reset_index()
        # Convert the summary statistics DataFrame into an HTML table
        table_header = [
            html.Thead(html.Tr([html.Th(col) for col in stats.columns]))
        ]
        rows = []
        for index, row in stats.iterrows():
            rows.append(html.Tr([html.Td(round(val, 2)) if isinstance(val, (float, int)) else html.Td(val) for val in row]))
        table_body = [html.Tbody(rows)]
        return html.Table(table_header + table_body, className="table table-striped")
    
    # Run the Dash app
    app.run_server(debug=True, port=8050)

# Example usage for testing:
if __name__ == "__main__":
    # If running this module directly, create a sample DataFrame for demonstration.
    sample_data = pd.DataFrame({
        'gene': ['GeneA', 'GeneB', 'GeneC', 'GeneD'],
        'pathways': [3, 5, 2, 4],
        'interactions': [10, 15, 7, 12]
    })
    sample_data['impact_index'] = sample_data['pathways'] + sample_data['interactions']
    launch_dashboard(sample_data)
