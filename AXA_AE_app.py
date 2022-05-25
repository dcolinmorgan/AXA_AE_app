from dash import Dash,html,dcc
from dash.dependencies import Input, Output
import gzip,numpy as np, pandas as pd
import plotly.express as px
import plotly.figure_factory as ff

px.set_mapbox_access_token('pk.eyJ1IjoiZGNvbGlubW9yZ2FuIiwiYSI6ImNsM2kwc3p2YjBhOGUzam1zOXdtenV0d2wifQ.R-SgXef7l-FI_zO7qYuQDQ')
app = Dash(__name__)

# f = gzip.GzipFile('mysite/coord_fulldiag_UVI.npy.gz', "r")
f = gzip.GzipFile('mysite/coord_fulldiag_UVI_min.npy.gz', "r")

data5=np.load(f,allow_pickle=True)
# data5=pd.read_csv('/content/drive/MyDrive/hku/AXA/loc_coord_diag.txt',sep='\t')
data5=pd.DataFrame(data5,columns=['pm25', 'pm10', 'o3', 'no2', 'so2', 'co', 'lat',
       'long', 'name', 'year', 'week', 'diag1', 'UVI'])

# del data5['loc1_x'],data5['loc1_y'],data5['kmeans{k}'],data5['date']#,data5['name']
# data5['year']=pd.to_datetime(data5['year'], format='%Y')
data6=data5.melt(['lat','long','year','week','name','diag1'])
data6=data6[data6.value>0]
data6['diag2']=data6['diag1'].str.split('(').str[1].str.split(')').str[0]
data6['diag2']=data6['diag2'].astype(str)

app.layout = html.Div([
    # html.H4('Live adjustable subplot-width'),
    # dcc.Graph(id="graph"),
    # html.P("Subplots Width:"),
    html.Div([

      # html.Div([
        # dcc.Slider(
        #     id='slider', min=2014, max=2021,
        #     value=2016, step=1),

        dcc.Slider(
            data6['year'].min(),
            data6['year'].max(),
            step=None,
            id='slider',
            value=data6['year'].min(),
            marks={str(year): str(year) for year in data6['year'].unique()}
        ),#, style={'width': '49%', 'padding': '0px 20px 20px 20px'})
    # ])

        dcc.Dropdown(
              data6['variable'].unique(),
              'pm10',
              id='dropdown'
          )
    ]),

    html.Div([
            dcc.Graph(id='graph0'),
            dcc.Graph(
                id='graphA'
                # hoverData={'points': [{'customdata': 'pm25'}]}
            )],style={'width': '49%', 'display': 'inline-block', 'padding': '0 20'}),
    html.Div([
            dcc.Graph(id='graphB'),
            dcc.Graph(id='graphC')],
         style={'width': '49%', 'display': 'inline-block', 'padding': '0 20'}
        )


])

def get_data(data6,year_value,variable_value):
    return data6[(data6['year']==year_value)&(data6['variable']==variable_value)]

@app.callback(
    Output("graph0", "figure"),
    Input("slider", "value"),
    Input("dropdown", "value"))
def plottt_line(year_value,variable_value):
    df=get_data(data6,year_value,variable_value)
    fig = px.line(data_frame=df, x='week', y='value',color='name')
    return fig #.show()

@app.callback(
    Output("graphA", "figure"),
    Input("slider", "value"),
    Input("dropdown", "value"))
def plottt_geo(year_value,variable_value):
    df=get_data(data6,year_value,variable_value)
    df=df.groupby(['lat','long','year','week','name','variable']).count().reset_index()
    fig=ff.create_hexbin_mapbox(
      data_frame=df, lat="lat", lon="long",
      nx_hexagon=10, opacity=0.9, labels={"color": variable_value},
      color="diag1", agg_func=np.sum, color_continuous_scale="Icefire")
    # fig = px.density_mapbox(data6, lat='lat', lon='long', z='value', radius=10,
    #      center=dict(lat=0, lon=180), zoom=0,
    #      mapbox_style="stamen-terrain")
    return fig #.show()

@app.callback(
    Output("graphB", "figure"),
    Input("slider", "value"),
    Input("dropdown", "value"))
def plottt_line(year_value,variable_value):
    df=get_data(data6,year_value,variable_value)
    # fig = px.line(data_frame=df, x='week', y='value',color='name')
    fig = px.bar(data_frame=data6[data6['year']==year_value], x='diag2', y='value',color='name')
    return fig #.show()

@app.callback(
    Output("graphC", "figure"),
    Input("slider", "value"),
    Input("dropdown", "value"))
def plottt_line(year_value,variable_value):
    # df=get_data(data6,year_value,variable_value)
    fig = px.scatter(data_frame=data5[data5['year']==year_value], x='o3', y='pm10',color='name')
    return fig #.show()

# if __name__ == '__main__':
# app.run_server()
