  GNU nano 2.9.3                                                                                                                                                                                      app2.py                                                                                                                                                                                                 

import dash
import dash_core_components as dcc
import dash_html_components as html
import plotly.express as px
import pandas as pd
import plotly.graph_objects as go

import psycopg2

pgconnect = psycopg2.connect(

host = '',
port = 5431,
database = '',
user = '',
password = "")
pgcursor = pgconnect.cursor()


# Download forecast data tablepgcursor.execute("SELECT * FROM fx_data")


pgcursor.execute("SELECT * FROM query1;")
#print(pgcursor.fetchone())
#pgcursor.execute("select reactionmeddrapt, count(distinct safetyreportid) from reactions where reactionmeddrapt is not null group by reactionmeddrapt order by count(distinct safetyreportid) desc limit 20;")

def generate_table(dataframe, max_rows=10):
    return html.Table([
        html.Thead(
            html.Tr([html.Th(col) for col in dataframe.columns])
        ),
        html.Tbody([
            html.Tr([
                html.Td(dataframe.iloc[i][col]) for col in dataframe.columns
            ]) for i in range(min(len(dataframe), max_rows))
        ])
    ])



# Download forecast data tablepgcursor.execute("SELECT * FROM fx_data")
df = pd.DataFrame(pgcursor.fetchall())
#print(df)

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)


#fig = px.bar(rx_df, x=0, y=1, color="City", barmode="group")
#fig = px.bar(df,x=0,y=1)

temp = df.groupby([3, 0]).size()
temp1 = temp.rename('size').reset_index()
temp1=temp1.groupby(3, as_index=False).sum().sort_values('size',ascending=False).head(10)

fig = go.Figure(data=[
    go.Bar(name='Major', x=temp1[3], y=temp1['size'])
])

app.layout = html.Div(children=[
    html.H1(children='Hello Dash'),

    html.Div(children='''
        Dash: A web application framework for Python.
    '''),

    dcc.Graph(
        id='example-graph',
        figure=fig
    )
])

if __name__ == '__main__':
    #app.run_server(debug=True)
    app.run_server(debug=True, port=8050, host="ec2-34-236-146-112.compute-1.amazonaws.com")

    pgcursor.close()
    pgconnect.close()

