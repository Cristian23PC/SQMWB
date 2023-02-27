import dash_design_kit as ddk
import plotly.express as px
from dash import dcc, register_page
import numpy as np
import pyodbc
import pandas as pd
from dash import Dash, dcc, dash_table, Input, Output, State,ALL, html
import dash_bootstrap_components as dbc
from plotly.subplots import make_subplots
import plotly.graph_objects as go
import dash
from dash.exceptions import PreventUpdate
from datetime import datetime,timedelta
import json
from constants import tabla_pilas_dict,tabla_sectores_pilas_dict,tabla_simulaciones_gproms,tabla_pila_semana_anho,tabla_riegos,tabla_data_lims
from tasks import postgres_engine
from itertools import cycle
import json
from tasks import REDIS_URL
import redis
import os


register_page(__name__,title='Simulacion Pilas',name='Simulacion Pilas')
redis_instance = redis.StrictRedis.from_url(os.environ.get("REDIS_URL", "redis://127.0.0.1:6379"))

def get_dataframe(semana,año,pila):
    
    df_sim = pd.read_sql("""SELECT * FROM {} WHERE "pila_sim"='{}' AND "semana_sim"={} AND "Anho_sim"={};""".format(tabla_simulaciones_gproms,pila,semana,año), postgres_engine)

    df_riego = pd.read_sql("""SELECT * FROM {} WHERE "Pila"='{}' AND "Semana_anho"<='{}';""".format(tabla_riegos,pila,str(año)+"-"+str(semana)), postgres_engine)

    df_drenaje = pd.read_sql("""SELECT * FROM {} WHERE "Pila2_lims"='{}' AND "Semana_anho_lims"<='{}';""".format(tabla_data_lims,pila,str(año)+"-"+str(semana)), postgres_engine)

    df_riego.Sector_riego.fillna('Sin Sector',inplace=True)

    return df_sim,df_riego,df_drenaje






def serve_layout():
    pilas=pd.read_sql("SELECT * FROM {};".format(tabla_pilas_dict), postgres_engine)
    pilas.pilas.fillna('Sin Pila',inplace=True)

    sectores_pilas=pd.read_sql("SELECT * FROM {};".format(tabla_sectores_pilas_dict), postgres_engine)
    sectores_pilas.Sector_riego.fillna('Sin Sector',inplace=True)
    
    layout=ddk.SidebarCompanion([
        ddk.SectionTitle('Simulaciones Pilas'),
        ddk.ControlCard(children=
            [   
            ddk.ControlItem(children=dcc.Dropdown(id="slct_sector",
                 options=[
                     {"label": name, "value": name}
                      for name in list(sectores_pilas.Sector_riego.unique())],
                 multi=False,
                 placeholder="Elija un Sector",
                 style={'width': "100%"},
                 value=[x for x in sectores_pilas.Sector_riego.unique()][0],
                 persistence=True
                 ),label='Seleccionar Sector:'),   

            ddk.ControlItem(children=dcc.Dropdown(id="slct_pila",
                 options=[
                     {"label": name, "value": name}
                      for name in list(pilas.pilas.unique())],
                 multi=False,
                 placeholder="Elija una Pila",
                 style={'width': "100%"},
                 value=477,
                 persistence=True
                 ),label='Seleccionar Pila:'),

            ddk.ControlItem(children=
                dcc.Dropdown(id="slct_año",
                 options=[
                     {"label": name, "value": name}
                      for name in [2021,2022,2023]],
                 placeholder="Elija un Año",
                 style={'width': "100%"},
                 value=2022,
                 persistence=True
                 ),label='Seleccionar Año'),

            ddk.ControlItem(children=dcc.Slider(id="slct_sem",min=1,max=53,step=None,
                 value=1,persistence=True
                 ),label='Seleccionar Semana',width=100
                )
                ],orientation='horizontal'),
            
            dbc.Alert(
            "No hay data de simulaciones para fecha-pila",
            id="alert-no-data",
            dismissable=True,
            fade=False,
            is_open=False,
            color="danger"
            ),

        dcc.Loading(children=[ddk.Card(width=100, children=[ddk.CardHeader(children=ddk.FullScreen(
                    children=[
                        html.Button('Pantalla Completa')
                    ],
                    target_id='scatter-plot'
                ))
               ,ddk.Graph(id='scatter-plot')])])
    ])
    
    return layout

layout=serve_layout


@dash.callback(Output('slct_pila','options'),
                Input('slct_sector','value'))
def update_pilas(sector):
    
    sectores_pilas=pd.read_sql("SELECT * FROM {};".format(tabla_sectores_pilas_dict), postgres_engine)
    sectores_pilas.Sector_riego.fillna('Sin Sector',inplace=True)
    pilas=list(sectores_pilas.loc[sectores_pilas.Sector_riego==sector].Pila)
    pilas.sort()
    
    return [{"label": name, "value": name} for name in pilas]
        
        
@dash.callback(
    Output("slct_sem", "marks"), 
    Output("slct_sem", "min"), 
    Output("slct_sem", "max"), 
    Output("slct_sem", "value"),
    Output("alert-no-data","is_open"), 
    Input("slct_pila", "value"),
    Input("slct_año", "value"),
    Input("slct_sem", "value"),
    prevent_initial_call=False)
def update_semana(pila,año,semana):

    pila_semana_anho=pd.read_sql("SELECT * FROM {};".format(tabla_pila_semana_anho), postgres_engine)

    dff=pila_semana_anho.copy()
    
    dff['Anho_sim']=dff['Semana_anho_sim'].str.split(pat='-',expand=True)[0]
    dff['semana_sim']=dff['Semana_anho_sim'].str.split(pat='-',expand=True)[1]
    semana_año=str(año)+"-"+str(semana)
    dff=dff[(dff.pila_sim==pila)&(dff['Semana_anho_sim']==semana_año)]
    
    if dff.shape[0]>0:
        return {int(a): {'label':str(b)} for a, b in zip(range(54), range(54))},1,53,semana,False
    else:
        return {int(a): {'label':str(b)} for a, b in zip(range(54), range(54))},1,53,semana,True


@dash.callback(
    Output("scatter-plot", "figure"),
    Input("slct_sem", "value"),
    State("slct_pila", "value"),
    State("slct_año", "value"),
    prevent_initial_call=True
    )
def update_bar_chart(semana,pila,año):

    if len(str(semana))==1:
        semana="0"+str(semana)

    df_sim,df_riegos,df_drenajes=get_dataframe(semana,año,pila)
    
    df_drenajes['Tipo_lims']=df_drenajes['Tipo_lims'].str.replace("SR","SR_drenaje")
    df_drenajes['Tipo_lims']=df_drenajes['Tipo_lims'].str.replace("SI","SI_drenaje")
    
    df_riegos['Tipo_riego']=df_riegos['Tipo_riego'].str.replace("SI","SI_riego")
    df_riegos['Tipo_riego']=df_riegos['Tipo_riego'].str.replace("MEZCLA","MEZCLA_riego")
    
    df_sim['colors']= df_sim['resultado'].apply(lambda x: 1 if x=='Na2SO4' else (2 if x== 'MgSO4xNa2SO4x4H2O' else (3 if x=='NaNO3' else 4)))
    df_drenajes['color_drenaje']=df_drenajes['Tipo_lims'].apply(lambda x: 5 if x=='SR_drenaje' else 6)
    df_riegos['color_riego']=df_riegos['Tipo_riego'].apply(lambda x: 1 if x=='SI_riego' else (2 if x== 'MEZCLA' else 3))

    mask = df_sim[(df_sim['pila_sim']==pila)&(df_sim['semana_sim']==int(semana))&(df_sim['Anho_sim']==int(año))]
    
    plot = go.Figure()
    plot.update_layout(showlegend=True,
                        xaxis_title="NaNO3",
                        yaxis_title="Na2SO4")
    colores = px.colors.qualitative.Plotly
    
    resultados=mask.resultado.unique()
    resultados.sort()
    colores=colores[0:len(resultados)]
    resultados_colores=dict(zip(resultados,colores))

    for i in mask.resultado.unique():
        plot.add_trace(go.Scatter(
            x= mask[mask['resultado']==i].NaNO3_sim,
            y= mask[mask['resultado']==i].Na2SO4_sim,
            mode='markers',
            marker= dict(color=mask.colors),
            marker_color=resultados_colores[i],
            name=i            
            )
            )

    for i in df_drenajes.Tipo_lims.unique():
        if i == "SR_drenaje":
            color='#FF0000'
        else:
            color='#0000FF'
        plot.add_trace(go.Scatter(
        x= df_drenajes[df_drenajes['Tipo_lims']==i].NaNO3_lims,
        y= df_drenajes[df_drenajes['Tipo_lims']==i].Na2SO4_lims,
        mode='markers',
        marker= dict(color=df_drenajes.color_drenaje),
        marker_color=color,name=i)
        )

    for i in df_riegos.Tipo_riego.unique():
        if i == "SI_riego":
            color='#AE00FF'
        else:
            color='#00FF27'
        plot.add_trace(go.Scatter(
        x= df_riegos[df_riegos['Tipo_riego']==i].NaNO3_riego,
        y= df_riegos[df_riegos['Tipo_riego']==i].Na2SO4_riego,
        mode='markers',
        marker= dict(color=df_riegos.color_riego),
        marker_color=color,name=i)
        )

    return plot

