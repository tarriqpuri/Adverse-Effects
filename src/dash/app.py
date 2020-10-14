import pickle
import copy
import pathlib
from urllib import urlretrieve
import dash
import math
import datetime as dt
import pandas as pd
from dash.dependencies import Input, Output, State, ClientsideFunction
import dash_core_components as dcc
import dash_html_components as html

import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

import psycopg2

pgconnect = psycopg2.connect(

host = 'ec2-34-229-140-20.compute-1.amazonaws.com',
port = 5431,
database = 'test',
user = 'tarriq',
password = "insight")
pgcursor = pgconnect.cursor()

pgcursor.execute("Select column_name "
             "from information_schema.columns "
             "where table_name = 'patientsremovecols';"
             )

columndf = pd.DataFrame(pgcursor.fetchall())
columndf = columndf[columndf[0] != 'safetyreportid']
print(columndf)

d={}
count=0
for feat in columndf[0]:
    count=count+1
    print(feat)
    query = ("select " + feat +
             ", count(distinct(safetyreportid)), count(seriousnessdeath) "
             "from patientsRemovecols group by " + feat + ";"
            )
    pgcursor.execute(query)
    d[feat] = pd.DataFrame(pgcursor.fetchall())

pgcursor.execute("Select column_name "
             "from information_schema.columns "
             "where table_name = 'drugs';"
             )


columndf = pd.DataFrame(pgcursor.fetchall())
columndf = columndf[columndf[0] != 'safetyreportid']
columndf = columndf[columndf[0] != 'index']
columndf = columndf[columndf[0] != 'drug-key']
columndf = columndf[columndf[0] != 'spl_id']
columndf = columndf[columndf[0] != 'spl_set_id']
columndf = columndf[columndf[0] != 'safetyreportid']
print(columndf)

count=0
for feat in columndf[0]:
    count=count+1
    print(feat)
    query = ("SELECT drugs." + feat +
             ", COUNT(DISTINCT(drugs.safetyreportid)), COUNT(patients.seriousnessdeath) "
             "FROM drugs INNER JOIN patients ON " 
             "drugs.safetyreportid = patients.safetyreportid "
             "GROUP BY " + feat + ";"
            )
    pgcursor.execute(query)
    d[feat] = pd.DataFrame(pgcursor.fetchall())



pgcursor.execute("Select column_name "
             "from information_schema.columns "
             "where table_name = 'reactions';"
             )


columndf = pd.DataFrame(pgcursor.fetchall())
columndf = columndf[columndf[0] != 'safetyreportid']
columndf = columndf[columndf[0] != 'index']
columndf = columndf[columndf[0] != 'reaction-key']

print(columndf)

count=0
for feat in columndf[0]:
    count=count+1
    print(feat)
    query = ("SELECT reactions." + feat +
             ", COUNT(DISTINCT(reactions.safetyreportid)), COUNT(patients.seriousnessdeath) "
             "FROM reactions INNER JOIN patients ON " 
             "reactions.safetyreportid = patients.safetyreportid "
             "GROUP BY " + feat + ";"
            )
    pgcursor.execute(query)
    d[feat] = pd.DataFrame(pgcursor.fetchall())


print(d["occurcountry"])
# Create controls for features
features = [
    {"label": str(key), "value": str(key)} for key in d
]


layout = dict(
    autosize=True,
    automargin=True,
    margin=dict(l=80, r=10, b=60, t=40),
    hovermode="closest",
    plot_bgcolor="#F9F9F9",
    paper_bgcolor="#F9F9F9",
    legend=dict(font=dict(size=10), orientation="h"),
    title="Adverse Effects",
)

app = dash.Dash(
    __name__, meta_tags=[{"name": "viewport", "content": "width=device-width"}]
)
server = app.server


# Create app layout
app.layout = html.Div(
    [
        dcc.Store(id="aggregate_data"),
        # empty Div to trigger javascript file for graph resizing
        html.Div(id="output-clientside"),
        html.Div(id='none',children=[],style={'display': 'none'}),
        html.Div(
            [
                html.Div(
                    [
                        html.Img(
                            src=app.get_asset_url("insight.png"),
                            id="plotly-image",
                            style={
                                "height": "60px",
                                "width": "auto",
                                "margin-bottom": "25px",
                            },
                        )
                    ],
                    className="one-third column",
                ),
                html.Div(
                    [
                        html.Div(
                            [
                                html.H3(
                                    "Adverse Effects",
                                    style={"margin-bottom": "0px"},
                                ),
                                html.H5(
                                    "Data Exploration Tool", style={"margin-top": "0px"}
                                ),
                            ]
                        )
                    ],
                    className="one-half column",
                    id="title",
                ),
            ],
            id="header",
            className="row flex-display",
            style={"margin-bottom": "25px"},
        ),
        html.Div(
            [
                html.Div(
                    [
                        html.P("Filter by Feature:", className="feature_label"),
                        dcc.Dropdown(
                            id="feature_selection",
                            options=features,
                            value='occurcountry',
                            searchable=False
                        ),
                        dcc.RadioItems(
                            id="null_selection",
                            options=[
                                {'label': 'Exclude Nulls', 'value': 0},
                                {'label': 'Include Nulls', 'value': 1}
                            ],
                            value=0,
                            labelStyle={'display': 'inline-block'}
                        ),
                        html.Div(
                            [dcc.Graph(id="temp-graph")],
                        ),
                    ],
                    className="pretty_container four columns",
                    id="cross-filter-options",
                ),
                html.Div(
                    [
                        html.Div(
                            [dcc.Graph(id="example-graph")],
                            className="pretty_container",
                        ),
                        html.Div(
                            [
                                html.Div(
                                    [html.H6(id="length_text"), html.P("Unique Values")],
                                    id="length",
                                    style={'width':'20%'},
                                    className="mini_container",
                                ),
                                html.Div(
                                    [html.H6(id="average_text"), html.P("Avg No. Cases")],
                                    id="avg",
                                    style={'width':'20%'},
                                    className="mini_container",
                                ),
                                html.Div(
                                    [html.H6(id="top_text"), html.P("Most Common Value")],
                                    id="top",
                                    style={'width':'45%'},
                                    className="mini_container",
                                ),
                            ],
                            id="info-container",
                            className="row container-display",
                        ),

                    ],
                    id="right-column",
                    className="eight columns",
               ),
            ],
            className="row flex-display",
        ),
    ],
    id="mainContainer",
    style={"display": "flex", "flex-direction": "column"},
)


@app.callback(
    Output('example-graph', 'figure'),
    [Input('feature_selection', 'value'),
     Input('null_selection','value')]
    )
def update_figure(feature, showNull):
    layout_count = copy.deepcopy(layout)
    if showNull == 0:
       cleandf = d[feature].dropna()
       x = cleandf[1]
       y = cleandf[2]
    else:
       x = d[feature][1]
       y = d[feature][2]


    data = [
        dict(
            type="scatter",
            mode="markers",
            x = x,
            y = y,
            name="Cases vs. Deaths",
            opacity=0,
            hoverinfo="skip",
        ),
        dict(
            type="scatter",
            mode="markers",
            x = x,
            y = y,
            name="Cases vs. Deaths",
        ),
    ]
    layout_count["height"] = 390
    layout_count["title"] = "Cases vs. Deaths : " + feature
    layout_count["dragmode"] = "select"
    layout_count["showlegend"] = False
    layout_count["autosize"] = True
    layout_count["xaxis"] = {"title" : "Cases"}
    layout_count["yaxis"] = {"title" : "Deaths"}
    figure = dict(data=data, layout=layout_count)

    return figure


@app.callback(
    Output('temp-graph', 'figure'),
    [Input('feature_selection', 'value'),
     Input('null_selection','value')]
    )
def temp_figure(feature,showNull):
    layout_pie = copy.deepcopy(layout)
    if showNull == 0:
       cleandf = d[feature].dropna()
       labels = cleandf[0]
       values = cleandf[1]
    else:
       labels = d[feature][0]
       values = d[feature][1]


    data = [
        dict(
            type="pie",
            textposition="none",       # https://stackoverflow.com/questions/42558268/hidding-low-percent-in-donut-chart
            showlegend=False,
            labels=labels,
            values=values,
            textinfo=None,
            name="Well Type Breakdown",
            hole=0.5,
            domain={"x": [0, 1], "y": [0, 1]},
        ),
    ]

    layout_pie["title"] = ""
    layout_pie["font"] = dict(color="#777777")

    figure = dict(data=data, layout=layout_pie)
    return figure



# Selectors -> total unique values
@app.callback(
    Output("length_text", "children"),
    Input('feature_selection', 'value')
)
def update_well_text(feature):
    length = len(d[feature].index)
    return length

# Selectors -> total unique values
@app.callback(
    Output("average_text", "children"),
    Input('feature_selection', 'value')
)
def update_avg_text(feature):
    avg = round( d[feature][1].mean() )
    return avg

# Selectors -> total unique values
@app.callback(
    Output("top_text", "children"),
    Input('feature_selection', 'value')
)
def update_top_text(feature):
    max = d[feature][1].max()
    name = d[feature].iloc[ d[feature][1].idxmax()  , 0]
    top = name + " (" + str(max) + " Cases)"
    return top


# Main
if __name__ == "__main__":
    app.run_server(debug=True, port=80, host="ec2-3-88-118-29.compute-1.amazonaws.com")
