import dash_design_kit as ddk
import plotly.express as px
from dash import dcc, register_page
import pandas as pd
from dash import Dash, dcc, dash_table, Input, Output, State,ALL, html
import plotly.graph_objects as go
import dash
import dash_bootstrap_components as dbc
from dash.exceptions import PreventUpdate
import json
from constants import tabla_ternarios,tabla_ternarios_sistemas,tabla_pilas_dict,tabla_sectores_pilas_dict,tabla_pila_semana_anho,tabla_ternarios_sistemas_riego
from tasks import postgres_engine
from itertools import cycle
import json
from tasks import REDIS_URL
import redis


register_page(__name__,title='Diagrama de Fases',name='Diagrama de Fases',path='/')
   
tab1_content = ddk.Card(
    children=[ddk.CardHeader(children=ddk.FullScreen(
                    children=[
                        html.Button('Pantalla Completa')
                    ],
                    target_id='graph1'
                )),
        # ddk.CardHeader(title="Diagrama Na2SO4-MgCl2-NaNO3"),
        ddk.Graph(id='graph1')]
        )

tab2_content = ddk.Card(
children=[ddk.CardHeader(children=ddk.FullScreen(
                children=[
                    html.Button('Pantalla Completa')
                ],
                target_id='graph2'
            )),
    # ddk.CardHeader(title="Diagrama NaNO3-Na2SO4-H2O"),
    ddk.Graph(id='graph2')]
    )

tab3_content = ddk.Card(
children=[ddk.CardHeader(children=ddk.FullScreen(
                children=[
                    html.Button('Pantalla Completa')
                ],
                target_id='graph3'
            )),
    # ddk.CardHeader(title="Diagrama Na2SO4-KCl-NaNO3"),
    ddk.Graph(id='graph3')]
    )

tab4_content = ddk.Card(
children=[ddk.CardHeader(children=ddk.FullScreen(
                children=[
                    html.Button('Pantalla Completa')
                ],
                target_id='graph4'
            )),
    # ddk.CardHeader(title="Diagrama Na2SO4-KCl-NaNO3"),
    ddk.Graph(id='graph4')]
    )

tabs = dcc.Tabs(
    id="tabs",
    value="tab-1",
    children=[
        dcc.Tab(
            label="Diagrama NaCl-NaNO3-Na2SO4",
            value="tab-1",
            selected_style = {"fontWeight": "bold"}
        ),
        dcc.Tab(
            label="Diagrama NaNO3-Na2SO4-H2O",
            value="tab-2",
            selected_style={"fontWeight": "bold"}),
        dcc.Tab(
            label="Diagrama Na2SO4-KCl-NaNO3",
            value="tab-3",
            selected_style={"fontWeight": "bold"}
            ),
        dcc.Tab(
            label="Diagrama Na2SO4-MgCl2-NaNO3",
            value="tab-4",
            selected_style={"fontWeight": "bold"})
    ]
)

def serve_layout():
    pilas=pd.read_sql("SELECT * FROM {};".format(tabla_pilas_dict), postgres_engine)
    pilas.pilas.fillna('Sin Pila',inplace=True)

    sectores_pilas=pd.read_sql("SELECT * FROM {};".format(tabla_sectores_pilas_dict), postgres_engine)
    sectores_pilas.Sector_riego.fillna('Sin Sector',inplace=True)


    cartas_control=ddk.ControlCard(children=
            [   
            ddk.ControlItem(children=dcc.Dropdown(id="slct_sector_fase",
                 options=[
                     {"label": name, "value": name}
                      for name in list(sectores_pilas.Sector_riego.unique())],
                 multi=False,
                 placeholder="Elija un Sector",
                 style={'width': "100%"},
                 value=[x for x in sectores_pilas.Sector_riego.unique()][0],
                 persistence=True
                 ),label='Seleccionar Sector:'),   

            ddk.ControlItem(children=dcc.Dropdown(id="slct_pila_fase",
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
                dcc.Dropdown(id="slct_año_fase",
                 options=[
                     {"label": name, "value": name}
                      for name in [2021,2022,2023]],
                 placeholder="Elija un Año",
                 style={'width': "100%"},
                 value=2022,
                 persistence=True
                 ),label='Seleccionar Año'),

            ddk.ControlItem(children=dcc.Slider(id="slct_sem_fase",min=1,max=53,step=None,
                 value=1,persistence=True
                 ),label='Seleccionar Semana',width=100
                ),
            dbc.Alert(
            "No hay data de simulaciones para fecha-pila",
            id="alert-no-data-fase",
            dismissable=True,
            fade=False,
            is_open=False,
            color="danger"
            )
                ],orientation='horizontal')
    
    layout=ddk.SidebarCompanion([
        ddk.SectionTitle('Diagrama de Fases'),
        cartas_control,
        ddk.Card(children=tabs),
        ddk.Card(id="update-tab")
        
    ])
    
    return layout

layout=serve_layout

@dash.callback(Output("update-tab", "children"), [Input("tabs", "value")])

def render_tabs(tab):
    if tab == "tab-1":
        return tab1_content
    elif tab == "tab-2":
        return tab2_content
    elif tab == "tab-3":
        return tab3_content
    elif tab == "tab-4":
        return tab4_content
    


@dash.callback(Output('slct_pila_fase','options'),
                Input('slct_sector_fase','value'))
def update_pilas(sector):
    sectores_pilas=pd.read_sql("SELECT * FROM {};".format(tabla_sectores_pilas_dict), postgres_engine)
    sectores_pilas.Sector_riego.fillna('Sin Sector',inplace=True)
    pilas=list(sectores_pilas.loc[sectores_pilas.Sector_riego==sector].Pila)
    pilas.sort()
    return [{"label": name, "value": name} for name in pilas]
    
 #       
@dash.callback(
    Output("slct_sem_fase", "marks"), 
    Output("slct_sem_fase", "min"), 
    Output("slct_sem_fase", "max"), 
    Output("slct_sem_fase", "value"),
    Output("alert-no-data-fase","is_open"), 
    Input("slct_pila_fase", "value"),
    Input("slct_año_fase", "value"),
    Input("slct_sem_fase", "value"),
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
    Output("graph1", "figure"),
    Input("slct_sem_fase", "value"),
    State("slct_pila_fase", "value"),
    State("slct_año_fase", "value"),
    prevent_initial_call=False
    )
def update_diagrama_fases(semana,pila,año):
    if len(str(semana))==1:
        semana="0"+str(semana)

    bd1 = pd.read_sql("""SELECT * FROM {} WHERE "Diagrama"='NaCl-NaNO3-Na2SO4';""".format(tabla_ternarios), postgres_engine)
    fig= go.Figure()
    fig.update_layout(showlegend=True)

    fig.add_trace(go.Scatter(x=bd1[bd1['Especie']=='Na2SO4x10H20'].X, y=bd1[bd1['Especie']=='Na2SO4x10H20'].Y, fill='tonexty',mode= 'none',name='Na2SO4x10H20',opacity=0.05))
    fig.add_trace(go.Scatter(x=bd1[bd1['Especie']=='Na2SO4'].X, y=bd1[bd1['Especie']=='Na2SO4'].Y, fill='tonexty',mode= 'none',name='Na2SO4',opacity=0.05))
    fig.add_trace(go.Scatter(x=bd1[bd1['Especie']=='NaCl'].X, y=bd1[bd1['Especie']=='NaCl'].Y, fill='tonexty',mode= 'none',name='NaCl',opacity=0.05))
    fig.add_trace(go.Scatter(x=bd1[bd1['Especie']=='NaNO3'].X, y=bd1[bd1['Especie']=='NaNO3'].Y, fill='tonexty',mode= 'none',name='NaNO3',opacity=0.05))
    fig.add_trace(go.Scatter(x=bd1[bd1['Especie']=='Darapskita'].X, y=bd1[bd1['Especie']=='Darapskita'].Y, fill='tonexty',mode= 'none',name='Darapskita',opacity=0.05))
    
    bd2=pd.read_sql("""SELECT * FROM {} WHERE sistema='NaNO3-Na2SO4-NaCl'
                    AND semana_anho<='{}' AND pila='{}'
                    ;""".format(tabla_ternarios_sistemas,str(año)+"-"+str(semana),pila), postgres_engine)

    bd2=bd2[['tipo','pila','sistema','semana_anho','Na2SO4_X_sistema','NaCl_%%_sistema']]

    
    SI=bd2.loc[bd2.tipo=='SI',['pila','sistema','semana_anho','Na2SO4_X_sistema','NaCl_%%_sistema']]
    SR=bd2.loc[bd2.tipo=='SR',['pila','sistema','semana_anho','Na2SO4_X_sistema','NaCl_%%_sistema']]

    fig.add_trace(go.Scatter(y=SI["NaCl_%%_sistema"], x=SI["Na2SO4_X_sistema"],mode='markers',name='SI_drenaje',marker = {'color': 'blue','size': 10}))
    fig.add_trace(go.Scatter(y=SR["NaCl_%%_sistema"], x=SR["Na2SO4_X_sistema"],mode='markers',name='SR_drenaje',marker = {'color': 'green','size': 10}))
    
    bd_riego=pd.read_sql("""SELECT * FROM {} WHERE sistema='NaNO3-Na2SO4-NaCl'
                    AND semana_anho<='{}' AND pila='{}'
                    ;""".format(tabla_ternarios_sistemas_riego,str(año)+"-"+str(semana),pila), postgres_engine)
    
    bd_riego=bd_riego[['tipo','pila','sistema','semana_anho','Na2SO4_X_sistema','NaCl_%%_sistema']]
    
    SI_riego=bd_riego.loc[bd_riego.tipo=='SI',['pila','sistema','semana_anho','Na2SO4_X_sistema','NaCl_%%_sistema']]
    SR_riego=bd_riego.loc[bd_riego.tipo=='MEZCLA',['pila','sistema','semana_anho','Na2SO4_X_sistema','NaCl_%%_sistema']]
    AGUA_riego=bd_riego.loc[bd_riego.tipo=='Agua',['pila','sistema','semana_anho','Na2SO4_X_sistema','NaCl_%%_sistema']]
    
    fig.add_trace(go.Scatter(y=SI_riego["NaCl_%%_sistema"], x=SI_riego["Na2SO4_X_sistema"],mode='markers',name='SI_riego',marker = {'color': 'orange','size': 10}))
    fig.add_trace(go.Scatter(y=SR_riego["NaCl_%%_sistema"], x=SR_riego["Na2SO4_X_sistema"],mode='markers',name='MEZCLA_riego',marker = {'color': 'purple','size': 10}))
    fig.add_trace(go.Scatter(y=AGUA_riego["NaCl_%%_sistema"], x=AGUA_riego["Na2SO4_X_sistema"],mode='markers',name='AGua_riego',marker = {'color': 'red','size': 10}))
    
    return fig

@dash.callback(
    Output("graph2", "figure"),
    Input("slct_sem_fase", "value"),
    State("slct_pila_fase", "value"),
    State("slct_año_fase", "value"),
    prevent_initial_call=False
    )
def update_diagrama_fases(semana,pila,año):
    if len(str(semana))==1:
        semana="0"+str(semana)
    bd2 = pd.read_sql("""SELECT * FROM {} WHERE "Diagrama"='NaNO3-Na2SO4-H2O';""".format(tabla_ternarios), postgres_engine)
    fig= go.Figure()
    fig.update_layout(showlegend=True)

    fig.add_trace(go.Scatter(x=bd2[bd2['Especie']=='Na2SO4x10H20'].X, y=bd2[bd2['Especie']=='Na2SO4x10H20'].Y, fill='tonexty',mode= 'none',name='Na2SO4x10H2O',opacity=0.05))
    fig.add_trace(go.Scatter(x=bd2[bd2['Especie']=='Na2SO4'].X, y=bd2[bd2['Especie']=='Na2SO4'].Y, fill='tonexty',mode= 'none',name='Na2SO4',opacity=0.05))
    fig.add_trace(go.Scatter(x=bd2[bd2['Especie']=='Darapskita2'].X, y=bd2[bd2['Especie']=='Darapskita2'].Y, fill='tonexty',mode= 'none',name='Darapskita2',opacity=0.05))
    fig.add_trace(go.Scatter(x=bd2[bd2['Especie']=='Darapskita'].X, y=bd2[bd2['Especie']=='Darapskita'].Y, fill='tonexty',mode= 'none',name='Darapskita',opacity=0.05))
    fig.add_trace(go.Scatter(x=bd2[bd2['Especie']=='xx1'].X, y=bd2[bd2['Especie']=='xx1'].Y, fill='tonexty',mode= 'none',name='xx1',opacity=0.05))
    fig.add_trace(go.Scatter(x=bd2[bd2['Especie']=='xx2'].X, y=bd2[bd2['Especie']=='xx2'].Y, fill='tonexty',mode= 'none',name='xx2',opacity=0.05))
    fig.add_trace(go.Scatter(x=bd2[bd2['Especie']=='xx3'].X, y=bd2[bd2['Especie']=='xx3'].Y, fill='tonexty',mode= 'none',name='xx3',opacity=0.05))
    
    bd2=pd.read_sql("""SELECT * FROM {} WHERE sistema='NaNO3-Na2SO4-H2O'
                    AND semana_anho<='{}' AND pila='{}'
                    ;""".format(tabla_ternarios_sistemas,str(año)+"-"+str(semana),pila), postgres_engine)

    bd2=bd2[['tipo','pila','sistema','semana_anho','Na2SO4_X_sistema','H2O_%%_sistema']]

    SI=bd2.loc[bd2.tipo=='SI',['pila','sistema','semana_anho','Na2SO4_X_sistema','H2O_%%_sistema']]
    SR=bd2.loc[bd2.tipo=='SR',['pila','sistema','semana_anho','Na2SO4_X_sistema','H2O_%%_sistema']]

    fig.add_trace(go.Scatter(y=SI["H2O_%%_sistema"], x=SI["Na2SO4_X_sistema"],mode='markers',name='SI_drenaje',marker = {'color': 'blue','size': 10}))
    fig.add_trace(go.Scatter(y=SR["H2O_%%_sistema"], x=SR["Na2SO4_X_sistema"],mode='markers',name='SR_drenaje',marker = {'color': 'green','size': 10}))

    bd_riego=pd.read_sql("""SELECT * FROM {} WHERE sistema='NaNO3-Na2SO4-H2O'
                    AND semana_anho<='{}' AND pila='{}'
                    ;""".format(tabla_ternarios_sistemas_riego,str(año)+"-"+str(semana),pila), postgres_engine)

    bd_riego=bd_riego[['tipo','pila','sistema','semana_anho','Na2SO4_X_sistema','H2O_%%_sistema']]
    

    SI_riego=bd_riego.loc[bd_riego.tipo=='SI',['pila','sistema','semana_anho','Na2SO4_X_sistema','H2O_%%_sistema']]
    SR_riego=bd_riego.loc[bd_riego.tipo=='MEZCLA',['pila','sistema','semana_anho','Na2SO4_X_sistema','H2O_%%_sistema']]

    fig.add_trace(go.Scatter(y=SI_riego["H2O_%%_sistema"], x=SI_riego["Na2SO4_X_sistema"],mode='markers',name='SI_riego',marker = {'color': 'orange','size': 10}))
    fig.add_trace(go.Scatter(y=SR_riego["H2O_%%_sistema"], x=SR_riego["Na2SO4_X_sistema"],mode='markers',name='MEZCLA_riego',marker = {'color': 'purple','size': 10}))
    
    return fig

@dash.callback(
    Output("graph3", "figure"),
    Input("slct_sem_fase", "value"),
    State("slct_pila_fase", "value"),
    State("slct_año_fase", "value"),
    prevent_initial_call=False
    )
def update_diagrama_fases(semana,pila,año):
    if len(str(semana))==1:
        semana="0"+str(semana)
    bd3 = pd.read_sql("""SELECT * FROM {} WHERE "Diagrama"='Na2SO4-KCl-NaNO3';""".format(tabla_ternarios), postgres_engine)
    fig= go.Figure()
    fig.update_layout(showlegend=True)
    fig.add_trace(go.Scatter(x=bd3[bd3['Especie']=='Na2SO4'].X, y=bd3[bd3['Especie']=='Na2SO4'].Y, fill='tonexty',mode= 'none',name='Na2SO4',opacity=0.1))
    fig.add_trace(go.Scatter(x=bd3[bd3['Especie']=='Glaserita'].X, y=bd3[bd3['Especie']=='Glaserita'].Y, fill='tonexty',mode= 'none',name='Glaserita',opacity=0.05))
    fig.add_trace(go.Scatter(x=bd3[bd3['Especie']=='Darapskita'].X, y=bd3[bd3['Especie']=='Darapskita'].Y, fill='tonexty',mode= 'none',name='Darapskita',opacity=0.05))
    fig.add_trace(go.Scatter(x=bd3[bd3['Especie']=='xx2'].X, y=bd3[bd3['Especie']=='xx2'].Y, fill='tonexty',mode= 'none',name='xx2',opacity=0.05))
    fig.add_trace(go.Scatter(x=bd3[bd3['Especie']=='xx1'].X, y=bd3[bd3['Especie']=='xx1'].Y, fill='tonexty',mode= 'none',name='xx1',opacity=0.05))
    fig.add_trace(go.Scatter(x=bd3[bd3['Especie']=='KNO3'].X, y=bd3[bd3['Especie']=='KNO3'].Y, fill='tonexty',mode= 'none',name='KNO3',opacity=0.05))
    
    bd2=pd.read_sql("""SELECT * FROM {} WHERE sistema='NaNO3-Na2SO4-KCl'
                    AND semana_anho<='{}' AND pila='{}'
                    ;""".format(tabla_ternarios_sistemas,str(año)+"-"+str(semana),pila), postgres_engine)

    bd2=bd2[['tipo','pila','sistema','semana_anho','Na2SO4_X_sistema','KCl_%%_sistema']]

    SI=bd2.loc[bd2.tipo=='SI',['pila','sistema','semana_anho','Na2SO4_X_sistema','KCl_%%_sistema']]
    SR=bd2.loc[bd2.tipo=='SR',['pila','sistema','semana_anho','Na2SO4_X_sistema','KCl_%%_sistema']]

    fig.add_trace(go.Scatter(y=SI["KCl_%%_sistema"], x=SI["Na2SO4_X_sistema"],mode='markers',name='SI_drenaje',marker = {'color': 'blue','size': 10}))
    fig.add_trace(go.Scatter(y=SR["KCl_%%_sistema"], x=SR["Na2SO4_X_sistema"],mode='markers',name='SR_drenaje',marker = {'color': 'green','size': 10}))

    bd_riego=pd.read_sql("""SELECT * FROM {} WHERE sistema='NaNO3-Na2SO4-KCl'
                    AND semana_anho<='{}' AND pila='{}'
                    ;""".format(tabla_ternarios_sistemas_riego,str(año)+"-"+str(semana),pila), postgres_engine)

    bd_riego=bd_riego[['tipo','pila','sistema','semana_anho','Na2SO4_X_sistema','KCl_%%_sistema']]

    SI_riego=bd_riego.loc[bd_riego.tipo=='SI',['pila','sistema','semana_anho','Na2SO4_X_sistema','KCl_%%_sistema']]
    SR_riego=bd_riego.loc[bd_riego.tipo=='MEZCLA',['pila','sistema','semana_anho','Na2SO4_X_sistema','KCl_%%_sistema']]

    fig.add_trace(go.Scatter(y=SI_riego["KCl_%%_sistema"], x=SI_riego["Na2SO4_X_sistema"],mode='markers',name='SI_riego',marker = {'color': 'orange','size': 10}))
    fig.add_trace(go.Scatter(y=SR_riego["KCl_%%_sistema"], x=SR_riego["Na2SO4_X_sistema"],mode='markers',name='MEZCLA_riego',marker = {'color': 'purple','size': 10}))
    
    return fig

@dash.callback(
    Output("graph4", "figure"),
    Input("slct_sem_fase", "value"),
    State("slct_pila_fase", "value"),
    State("slct_año_fase", "value"),
    prevent_initial_call=False
    )
def update_diagrama_fases(semana,pila,año):
    if len(str(semana))==1:
        semana="0"+str(semana)
    bd4 = pd.read_sql("""SELECT * FROM {} WHERE "Diagrama" = 'Na2SO4-MgCl2-NaNO3'""".format(tabla_ternarios), postgres_engine)
    fig= go.Figure()
    fig.update_layout(showlegend=True)
    fig.add_trace(go.Scatter(x=bd4[bd4['Especie']=='Na2SO4'].X, y=bd4[bd4['Especie']=='Na2SO4'].Y, fill='tonexty',mode= 'none',name='Na2SO4',opacity=0.05))
    fig.add_trace(go.Scatter(x=bd4[bd4['Especie']=='Astrakanita'].X, y=bd4[bd4['Especie']=='Astrakanita'].Y, fill='tonexty',mode= 'none',name='Astrakanita',opacity=0.05))
    fig.add_trace(go.Scatter(x=bd4[bd4['Especie']=='Darapskita'].X, y=bd4[bd4['Especie']=='Darapskita'].Y, fill='tonexty',mode= 'none',name='Darapskita',opacity=0.05))
    fig.add_trace(go.Scatter(x=bd4[bd4['Especie']=='Epsomita'].X, y=bd4[bd4['Especie']=='Epsomita'].Y, fill='tonexty',mode= 'none',name='Epsomita',opacity=0.05))
    fig.add_trace(go.Scatter(x=bd4[bd4['Especie']=='Bischofita'].X, y=bd4[bd4['Especie']=='Bischofita'].Y, fill='tonexty',mode= 'none',name='Bischofita',opacity=0.05))
    fig.add_trace(go.Scatter(x=bd4[bd4['Especie']=='NaNO3'].X, y=bd4[bd4['Especie']=='NaNO3'].Y, fill='tonexty',mode= 'none',name='NaNO3',opacity=0.05))
    fig.add_trace(go.Scatter(x=bd4[bd4['Especie']=='NaNO3-MgCl2'].X, y=bd4[bd4['Especie']=='NaNO3-MgCl2'].Y, fill='tonexty',mode= 'none',name='NaNO3-MgCl2',opacity=0.05))
    
    bd2=pd.read_sql("""SELECT * FROM {} WHERE sistema='NaNO3-Na2SO4-MgCl2'
                    AND semana_anho<='{}' AND pila='{}'
                    ;""".format(tabla_ternarios_sistemas,str(año)+"-"+str(semana),pila), postgres_engine)

    bd2=bd2[['tipo','pila','sistema','semana_anho','Na2SO4_X_sistema','MgCl2_%%_sistema']]

    SI=bd2.loc[bd2.tipo=='SI',['pila','sistema','semana_anho','Na2SO4_X_sistema','MgCl2_%%_sistema']]
    SR=bd2.loc[bd2.tipo=='SR',['pila','sistema','semana_anho','Na2SO4_X_sistema','MgCl2_%%_sistema']]

    fig.add_trace(go.Scatter(y=SI["MgCl2_%%_sistema"], x=SI["Na2SO4_X_sistema"],mode='markers',name='SI_drenaje',marker = {'color': 'blue','size': 10}))
    fig.add_trace(go.Scatter(y=SR["MgCl2_%%_sistema"], x=SR["Na2SO4_X_sistema"],mode='markers',name='SR_drenaje',marker = {'color': 'green','size': 10}))

    bd_riego=pd.read_sql("""SELECT * FROM {} WHERE sistema='NaNO3-Na2SO4-MgCl2'
                    AND semana_anho<='{}' AND pila='{}'
                    ;""".format(tabla_ternarios_sistemas_riego,str(año)+"-"+str(semana),pila), postgres_engine)

    bd_riego=bd_riego[['tipo','pila','sistema','semana_anho','Na2SO4_X_sistema','MgCl2_%%_sistema']]

    SI_riego=bd_riego.loc[bd_riego.tipo=='SI',['pila','sistema','semana_anho','Na2SO4_X_sistema','MgCl2_%%_sistema']]
    SR_riego=bd_riego.loc[bd_riego.tipo=='MEZCLA',['pila','sistema','semana_anho','Na2SO4_X_sistema','MgCl2_%%_sistema']]

    fig.add_trace(go.Scatter(y=SI_riego["MgCl2_%%_sistema"], x=SI_riego["Na2SO4_X_sistema"],mode='markers',name='SI_riego',marker = {'color': 'orange','size': 10}))
    fig.add_trace(go.Scatter(y=SR_riego["MgCl2_%%_sistema"], x=SR_riego["Na2SO4_X_sistema"],mode='markers',name='MEZCLA_riego',marker = {'color': 'purple','size': 10}))
    
    
    return fig