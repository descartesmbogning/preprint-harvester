"""
Preprint Infrastructure Explorer — MVP Dash App

Purpose
-------
Interactive dashboard to help researchers understand the Global Preprint Dataset,
with a first focus on the impact of work-level clustering/deduplication on server counts,
internal duplicates/versions, and cross-server reassignment.

Expected input files
--------------------
Place these files in: outputs_new/server_dedupe_impact/

1. table_server_deduplication_impact_article_ready.csv
2. server_to_server_flow_sankey_cross_server_only.csv
3. server_flow_heatmap_percent_cross_server_only.csv
4. dedupe_source_by_server.csv
5. server_deduplication_metrics_full.csv

Run
---
python app.py

Install if needed
-----------------
pip install dash pandas plotly dash-bootstrap-components
"""

from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from dash import Dash, Input, Output, dcc, html, dash_table
import dash_bootstrap_components as dbc

# ============================================================
# 1. CONFIG
# ============================================================

DATA_DIR = Path("notebooks/outputs_new/server_dedupe_impact")

ARTICLE_TABLE_PATH = DATA_DIR / "table_server_deduplication_impact_article_ready.csv"
FLOW_SANKEY_PATH = DATA_DIR / "server_to_server_flow_sankey_cross_server_only.csv"
HEATMAP_PATH = DATA_DIR / "server_flow_heatmap_percent_cross_server_only.csv"
DEDUPE_SOURCE_PATH = DATA_DIR / "dedupe_source_by_server.csv"
FULL_METRICS_PATH = DATA_DIR / "server_deduplication_metrics_full.csv"

TOP_N_DEFAULT = 25

# ============================================================
# 2. DATA LOADING
# ============================================================


def load_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing file: {path}")
    return pd.read_csv(path)


article_df = load_csv(ARTICLE_TABLE_PATH)
flow_df = load_csv(FLOW_SANKEY_PATH)
heatmap_df = pd.read_csv(HEATMAP_PATH, index_col=0) if HEATMAP_PATH.exists() else pd.DataFrame()
dedupe_source_df = load_csv(DEDUPE_SOURCE_PATH) if DEDUPE_SOURCE_PATH.exists() else pd.DataFrame()
full_metrics_df = load_csv(FULL_METRICS_PATH) if FULL_METRICS_PATH.exists() else article_df.copy()

# Clean numeric columns
for df in [article_df, flow_df, full_metrics_df]:
    for col in df.columns:
        if col not in ["server_name", "server_typology", "from_server", "to_server", "flow_type"]:
            df[col] = pd.to_numeric(df[col], errors="ignore")

# Add friendly percentage columns for tables
percentage_cols = [
    "pct_change_after_deduplication",
    "true_stability_rate",
    "internal_duplication_rate",
    "cross_server_outgoing_rate",
    "cross_server_incoming_rate",
    "overall_deduplication_reduction_rate",
]

for col in percentage_cols:
    if col in article_df.columns:
        article_df[f"{col}_pct"] = (article_df[col] * 100).round(2)

# ============================================================
# 3. HELPER FUNCTIONS
# ============================================================


def metric_card(title, value, subtitle=None):
    return dbc.Card(
        dbc.CardBody(
            [
                html.Div(title, className="text-muted small"),
                html.H3(value, className="mb-0"),
                html.Div(subtitle or "", className="text-muted small mt-1"),
            ]
        ),
        className="shadow-sm border-0 rounded-4 h-100",
    )


def format_int(x):
    try:
        return f"{int(x):,}"
    except Exception:
        return "—"


def format_pct(x):
    try:
        return f"{100 * float(x):.1f}%"
    except Exception:
        return "—"


def get_top_servers(df, n=25, sort_col="records_before_deduplication"):
    if sort_col not in df.columns:
        sort_col = df.select_dtypes(include="number").columns[0]
    return df.sort_values(sort_col, ascending=False).head(n).copy()


# ============================================================
# 4. FIGURE FUNCTIONS
# ============================================================


def make_before_after_bar(df, top_n=25):
    plot_df = get_top_servers(df, top_n, "records_before_deduplication")

    long_df = plot_df.melt(
        id_vars="server_name",
        value_vars=[
            "records_before_deduplication",
            "parent_records_after_deduplication",
        ],
        var_name="stage",
        value_name="records",
    )

    long_df["stage"] = long_df["stage"].replace(
        {
            "records_before_deduplication": "Before deduplication",
            "parent_records_after_deduplication": "After deduplication (parent records)",
        }
    )

    fig = px.bar(
        long_df,
        x="records",
        y="server_name",
        color="stage",
        barmode="group",
        orientation="h",
        title=f"Records before vs after work-level clustering — Top {top_n} servers",
        labels={"records": "Number of records", "server_name": "Server", "stage": "Stage"},
    )
    fig.update_layout(yaxis={"categoryorder": "total ascending"}, height=max(650, top_n * 30))
    return fig



def make_typology_scatter(df):
    size_col = "records_before_deduplication"
    fig = px.scatter(
        df,
        x="true_stability_rate",
        y="cross_server_outgoing_rate",
        size=size_col,
        color="server_typology" if "server_typology" in df.columns else None,
        hover_name="server_name",
        hover_data={
            "records_before_deduplication": ":,",
            "parent_records_after_deduplication": ":,",
            "internal_duplication_rate": ":.2%",
            "cross_server_incoming_rate": ":.2%",
            "net_cross_server_flow": ":,",
        },
        title="Server typology: stability vs cross-server reassignment",
        labels={
            "true_stability_rate": "True stability rate",
            "cross_server_outgoing_rate": "Cross-server outgoing rate",
            "server_typology": "Server typology",
            size_col: "Records before dedupe",
        },
    )
    fig.update_layout(height=650)
    fig.update_xaxes(tickformat=".0%")
    fig.update_yaxes(tickformat=".0%")
    return fig



def make_deduplication_sources_bar(df, top_n=25):
    plot_df = get_top_servers(df, top_n, "records_before_deduplication")

    cols = [
        "stable_parent_records",
        "internal_duplicates_or_versions",
        "cross_server_outgoing_records",
    ]
    cols = [c for c in cols if c in plot_df.columns]

    long_df = plot_df.melt(
        id_vars="server_name",
        value_vars=cols,
        var_name="category",
        value_name="records",
    )

    long_df["category"] = long_df["category"].replace(
        {
            "stable_parent_records": "Stable parents",
            "internal_duplicates_or_versions": "Internal duplicates / versions",
            "cross_server_outgoing_records": "Cross-server reassigned",
        }
    )

    fig = px.bar(
        long_df,
        x="records",
        y="server_name",
        color="category",
        orientation="h",
        title=f"Sources of deduplication impact — Top {top_n} servers",
        labels={"records": "Number of records", "server_name": "Server", "category": "Record behavior"},
    )
    fig.update_layout(yaxis={"categoryorder": "total ascending"}, height=max(650, top_n * 30))
    return fig



def make_sankey(flow_df, top_n_flows=75):
    if flow_df.empty:
        return go.Figure().update_layout(title="No cross-server flow data available")

    plot_df = flow_df.sort_values("n_records", ascending=False).head(top_n_flows).copy()

    sources = plot_df["from_server"].astype(str).tolist()
    targets = plot_df["to_server"].astype(str).tolist()
    labels = sorted(set(sources + targets))
    label_to_idx = {label: i for i, label in enumerate(labels)}

    fig = go.Figure(
        data=[
            go.Sankey(
                node=dict(
                    pad=15,
                    thickness=18,
                    line=dict(width=0.5),
                    label=labels,
                ),
                link=dict(
                    source=[label_to_idx[s] for s in sources],
                    target=[label_to_idx[t] for t in targets],
                    value=plot_df["n_records"].tolist(),
                    customdata=plot_df[["from_server", "to_server", "n_records"]].values,
                    hovertemplate="%{customdata[0]} → %{customdata[1]}<br>Records: %{customdata[2]:,}<extra></extra>",
                ),
            )
        ]
    )

    fig.update_layout(
        title=f"Cross-server reassignment flows — Top {top_n_flows} flows",
        height=750,
        font_size=11,
    )
    return fig



def make_heatmap(heatmap_df, top_n=40):
    if heatmap_df.empty:
        return go.Figure().update_layout(title="No heatmap data available")

    # Keep servers with strongest total flow values
    tmp = heatmap_df.copy()
    tmp["row_sum"] = tmp.sum(axis=1)
    top_rows = tmp.sort_values("row_sum", ascending=False).head(top_n).index
    plot_df = heatmap_df.loc[top_rows]

    # Keep top columns appearing in selected rows
    top_cols = plot_df.sum(axis=0).sort_values(ascending=False).head(top_n).index
    plot_df = plot_df[top_cols]

    fig = px.imshow(
        plot_df,
        aspect="auto",
        title=f"Cross-server reassignment heatmap — Top {top_n} servers",
        labels=dict(x="Parent server after clustering", y="Original server", color="Share of original server"),
    )
    fig.update_layout(height=800)
    fig.update_coloraxes(colorbar_tickformat=".0%")
    return fig


# ============================================================
# 5. APP SETUP
# ============================================================

app = Dash(
    __name__,
    external_stylesheets=[dbc.themes.BOOTSTRAP],
    suppress_callback_exceptions=True,
)

app.title = "Preprint Infrastructure Explorer"

# ============================================================
# 6. PAGE LAYOUTS
# ============================================================


def navbar():
    return dbc.NavbarSimple(
        brand="Preprint Infrastructure Explorer",
        brand_href="/",
        color="dark",
        dark=True,
        children=[
            dbc.NavItem(dbc.NavLink("Overview", href="/")),
            dbc.NavItem(dbc.NavLink("Deduplication Impact", href="/dedupe")),
            dbc.NavItem(dbc.NavLink("Server Flows", href="/flows")),
            dbc.NavItem(dbc.NavLink("Data & Methods", href="/methods")),
        ],
        className="mb-4",
    )



def overview_page():
    total_before = article_df["records_before_deduplication"].sum()
    total_after = article_df["parent_records_after_deduplication"].sum()
    total_internal = article_df.get("internal_duplicates_or_versions", pd.Series(dtype=float)).sum()
    total_cross = article_df.get("cross_server_outgoing_records", pd.Series(dtype=float)).sum()
    n_servers = article_df["server_name"].nunique()

    return dbc.Container(
        [
            html.H2("Understanding the Global Preprint Dataset"),
            html.P(
                "This dashboard helps researchers explore how work-level clustering changed server-level counts, "
                "separating stable parent records, internal duplicates or versions, and cross-server reassignment."
            ),
            dbc.Row(
                [
                    dbc.Col(metric_card("Records before deduplication", format_int(total_before)), md=3),
                    dbc.Col(metric_card("Parent records after deduplication", format_int(total_after)), md=3),
                    dbc.Col(metric_card("Servers", format_int(n_servers)), md=2),
                    dbc.Col(metric_card("Internal duplicates / versions", format_int(total_internal)), md=2),
                    dbc.Col(metric_card("Cross-server reassigned", format_int(total_cross)), md=2),
                ],
                className="g-3 mb-4",
            ),
            dbc.Alert(
                [
                    html.Strong("Core idea: "),
                    "Deduplication does not only reduce counts. It also reveals whether reductions come from versions within the same server or from records reassigned across servers after parent selection.",
                ],
                color="info",
            ),
            dcc.Graph(figure=make_before_after_bar(article_df, TOP_N_DEFAULT)),
            dcc.Graph(figure=make_deduplication_sources_bar(article_df, TOP_N_DEFAULT)),
        ],
        fluid=True,
    )



def dedupe_page():
    typologies = sorted(article_df["server_typology"].dropna().unique()) if "server_typology" in article_df.columns else []

    table_cols = [
        "server_name",
        "records_before_deduplication",
        "parent_records_after_deduplication",
        "absolute_change_after_deduplication",
        "stable_parent_records",
        "internal_duplicates_or_versions",
        "cross_server_incoming_records",
        "cross_server_outgoing_records",
        "net_cross_server_flow",
        "true_stability_rate_pct",
        "internal_duplication_rate_pct",
        "cross_server_outgoing_rate_pct",
        "overall_deduplication_reduction_rate_pct",
        "server_typology",
    ]
    table_cols = [c for c in table_cols if c in article_df.columns]

    return dbc.Container(
        [
            html.H2("Deduplication Impact"),
            html.P(
                "This page compares server-level counts before and after work-level clustering. "
                "It distinguishes internal duplicate/version records from cross-server reassignment."
            ),
            dbc.Row(
                [
                    dbc.Col(
                        [
                            html.Label("Top N servers"),
                            dcc.Slider(
                                id="top-n-slider",
                                min=10,
                                max=100,
                                step=5,
                                value=25,
                                marks={10: "10", 25: "25", 50: "50", 100: "100"},
                            ),
                        ],
                        md=6,
                    ),
                    dbc.Col(
                        [
                            html.Label("Server typology"),
                            dcc.Dropdown(
                                id="typology-filter",
                                options=[{"label": t, "value": t} for t in typologies],
                                value=[],
                                multi=True,
                                placeholder="Filter by typology",
                            ),
                        ],
                        md=6,
                    ),
                ],
                className="mb-4",
            ),
            dcc.Graph(id="before-after-graph"),
            dcc.Graph(id="typology-scatter"),
            html.H4("Article-ready server table"),
            dash_table.DataTable(
                id="article-table",
                columns=[{"name": c, "id": c} for c in table_cols],
                data=article_df[table_cols].to_dict("records"),
                page_size=15,
                sort_action="native",
                filter_action="native",
                style_table={"overflowX": "auto"},
                style_cell={
                    "textAlign": "left",
                    "fontSize": "12px",
                    "padding": "6px",
                    "minWidth": "120px",
                },
                style_header={"fontWeight": "bold"},
            ),
        ],
        fluid=True,
    )



def flows_page():
    return dbc.Container(
        [
            html.H2("Server Flows"),
            html.P(
                "This page shows how records were reassigned from their original server to the selected parent server after work-level clustering."
            ),
            dbc.Row(
                [
                    dbc.Col(
                        [
                            html.Label("Top N cross-server flows"),
                            dcc.Slider(
                                id="top-flow-slider",
                                min=20,
                                max=200,
                                step=10,
                                value=75,
                                marks={20: "20", 75: "75", 150: "150", 200: "200"},
                            ),
                        ],
                        md=6,
                    ),
                    dbc.Col(
                        [
                            html.Label("Top N servers in heatmap"),
                            dcc.Slider(
                                id="top-heatmap-slider",
                                min=10,
                                max=80,
                                step=5,
                                value=40,
                                marks={10: "10", 40: "40", 80: "80"},
                            ),
                        ],
                        md=6,
                    ),
                ],
                className="mb-4",
            ),
            dcc.Graph(id="sankey-graph"),
            dcc.Graph(id="heatmap-graph"),
            html.H4("Top cross-server flows"),
            dash_table.DataTable(
                columns=[{"name": c, "id": c} for c in flow_df.columns],
                data=flow_df.sort_values("n_records", ascending=False).head(100).to_dict("records"),
                page_size=15,
                sort_action="native",
                filter_action="native",
                style_table={"overflowX": "auto"},
                style_cell={"textAlign": "left", "fontSize": "12px", "padding": "6px"},
                style_header={"fontWeight": "bold"},
            ),
        ],
        fluid=True,
    )



def methods_page():
    return dbc.Container(
        [
            html.H2("Data & Methods"),
            dbc.Accordion(
                [
                    dbc.AccordionItem(
                        [
                            html.P(
                                "A parent record is the representative record selected to describe one scholarly work after duplicate and versioned records have been grouped."
                            ),
                            html.P(
                                "The parent index should be used when researchers want to count unique works rather than all raw records."
                            ),
                        ],
                        title="What is a parent record?",
                    ),
                    dbc.AccordionItem(
                        [
                            html.P(
                                "Internal duplicates or versions are records whose original server is the same as the selected parent server, but that did not survive as the selected parent record."
                            ),
                            html.Code("server_name == parent_server_name_selected AND is_parent_selected == False"),
                        ],
                        title="What is internal duplication/versioning?",
                    ),
                    dbc.AccordionItem(
                        [
                            html.P(
                                "Cross-server reassignment occurs when a record originally assigned to one server is grouped under a parent record from another server."
                            ),
                            html.Code("server_name != parent_server_name_selected"),
                        ],
                        title="What is cross-server reassignment?",
                    ),
                    dbc.AccordionItem(
                        [
                            html.P(
                                "Deduplication is useful because raw record counts can overestimate the number of unique scholarly works when multiple versions, reposts, or infrastructure duplicates exist."
                            ),
                            html.P(
                                "Separating internal duplicates from cross-server flows helps researchers understand whether reductions are due to versioning within a server or infrastructure overlap across servers."
                            ),
                        ],
                        title="Why does this matter?",
                    ),
                ],
                start_collapsed=False,
            ),
            html.Hr(),
            html.H4("Recommended interpretation"),
            html.Ul(
                [
                    html.Li("High true stability rate: most records remain selected as parents within the same server."),
                    html.Li("High internal duplication rate: many records are versions or duplicates within the same server."),
                    html.Li("High outgoing cross-server rate: many records are parented under another server."),
                    html.Li("High incoming cross-server rate: the server absorbs records originally assigned to other servers."),
                ]
            ),
        ],
        fluid=True,
    )


# ============================================================
# 7. ROUTING
# ============================================================

app.layout = html.Div(
    [
        dcc.Location(id="url"),
        navbar(),
        html.Div(id="page-content"),
    ]
)


@app.callback(Output("page-content", "children"), Input("url", "pathname"))
def display_page(pathname):
    if pathname == "/dedupe":
        return dedupe_page()
    if pathname == "/flows":
        return flows_page()
    if pathname == "/methods":
        return methods_page()
    return overview_page()


# ============================================================
# 8. CALLBACKS
# ============================================================


@app.callback(
    Output("before-after-graph", "figure"),
    Output("typology-scatter", "figure"),
    Output("article-table", "data"),
    Input("top-n-slider", "value"),
    Input("typology-filter", "value"),
)
def update_dedupe_page(top_n, selected_typologies):
    dff = article_df.copy()

    if selected_typologies:
        dff = dff[dff["server_typology"].isin(selected_typologies)]

    fig1 = make_before_after_bar(dff, top_n)
    fig2 = make_typology_scatter(dff)

    table_cols = [col["id"] for col in dash_table.DataTable().columns] if False else None
    data = dff.sort_values("records_before_deduplication", ascending=False).to_dict("records")

    return fig1, fig2, data


@app.callback(
    Output("sankey-graph", "figure"),
    Output("heatmap-graph", "figure"),
    Input("top-flow-slider", "value"),
    Input("top-heatmap-slider", "value"),
)
def update_flows(top_flow_n, top_heatmap_n):
    return make_sankey(flow_df, top_flow_n), make_heatmap(heatmap_df, top_heatmap_n)


# ============================================================
# 9. RUN
# ============================================================

if __name__ == "__main__":
    app.run(debug=True)
