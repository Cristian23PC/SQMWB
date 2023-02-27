from datetime import datetime, timedelta
import os
import pandas as pd
from celery import Celery
from celery.schedules import crontab
import numpy as np
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool
from constants import tabla_pilas_dict,tabla_sectores_pilas_dict,tabla_simulaciones_gproms,tabla_ternarios_sistemas,tabla_pila_semana_anho,tabla_ternarios,tabla_riegos,tabla_data_lims,tabla_ternarios_sistemas_riego
from urllib.parse import urlparse
import pyodbc
from google.oauth2 import service_account
import pandas_gbq
import math

#
host = os.getenv('HOST_SYNAPSE')
database = os.getenv('DB_SYNAPSE')
username = os.getenv('USUARIO_SYNAPSE')
password = os.getenv('PASSWORD_SYNAPSE')
driver= '{ODBC Driver 17 for SQL Server}'


query_sector_pilas='''SELECT t.CLASE_DES1 Pila, t.VALOR_TEXTO Sector, 
  case
      when t.clase_des1 like '472' then 'SC3'
      when t.clase_des1 like '486' then 'SC2'
      when t.clase_des1 like '487' then 'SC2'
      when t.clase_des1 like '423B' then 'SC2'
      when t.clase_des1 like '424B' then 'SC2'
      when t.clase_des1 like '475B' then 'SC2'
      else x.COP_SC
      end as COP_SC,
  case
      when t.valor_texto like '%TEA%' then 'TEA'
      when t.valor_texto like '%SC%' then 'TEA'
      else t.valor_texto
      end as Sector2
 FROM `datastream-ext-prd.ds_sipro.SPL_VA_MED_CLASES_2` t
left join
(SELECT l.CLASE_DES1 Pila, l.VALOR_TEXTO COP_SC  FROM `datastream-ext-prd.ds_sipro.SPL_VA_MED_CLASES_2` l
where l.ID_OBJETO = 6220961) x on x.Pila = t.CLASE_DES1
where t.ID_OBJETO = 6220960
'''

query_pilas_tiporiego='''select b.PILA, CONCAT(YEAR(b.fecha),'-',DATEPART( wk, b.fecha)) Semana_Anho, --b.FECHA 
	case 
		when sum(isnull(R_IMP,0))>0 then 'Impregnación' 
		when sum(isnull(R_AGUA,0))>0 then 'Agua' 
		when sum(isnull(R_SI,0))>0 then 'SI' 
		when sum(isnull(R_MEZCLA,0))>0 then 'MEZCLA' 
		else 'Sin riego' end as tipo_riego
from [sipro].[sp_pl_pilas_nv] b
join
(select t1.pila from (select PILA, FECHA from [sipro].[sp_pl_pilas_nv] where fecha >= '2021-01-01' group by pila, fecha having sum(R_IMP) > 0 or sum(R_AGUA)>0 or sum(R_SI)>0 or sum(R_MEZCLA)>0) t1) a on b.pila=a.pila
group by b.PILA, CONCAT(YEAR(b.fecha),'-',DATEPART( wk, b.fecha))
order by b.pila, CONCAT(YEAR(b.fecha),'-',DATEPART( wk, b.fecha))
'''

query_pilas_riego_tiposector='''select x.Sector_riego, x.Tipo_riego, x.Semana_anho, avg(x.I2_riego) I2_riego, avg(x.NaNO3_riego) NaNO3_riego, avg(x.K_riego) K_riego, avg(x.Na2SO4_riego) Na2SO4_riego, 
avg(x.NaCl_riego) NaCl_riego, avg(x.Mg_riego) Mg_riego
from
(select t.FECHA, concat(extract(year from date(t.fecha)),'-',extract(isoweek from date(t.fecha))) Semana_anho ,t.Sector Sector_riego, t.Tipo_riego,avg(t.I2) I2_riego ,avg(t.k) K_riego, avg(t.NaNO3) NaNO3_riego, avg(t.Na2SO4) Na2SO4_riego, avg(t.Mg) Mg_riego, avg(t.NaCl) NaCl_riego, avg(t.NaNO3)+avg(t.Na2SO4)+avg(t.NaCl) Sales_tot_riego
from 
(select T.SAMPLED_DATE Fecha, T.PRODUCT ID,
	case
		when t.PRODUCT like '903003%' then 'SI COP5'
		when t.PRODUCT like '903006%' then 'Brine COP5 - COP4'
		when t.PRODUCT like '903012%' then 'AFA NV'
		when t.PRODUCT like '903013%' then 'AFA Iris'
		when t.PRODUCT like '903014%' then 'Brine COP2 - COP1'
		when t.PRODUCT like '903017%' then 'SI SC2'
		when t.PRODUCT like '903018%' then 'SI COP2A'
		when t.PRODUCT like '903020%' then 'Mezcla COP2A'
		when t.PRODUCT like '903021%' then 'Mezcla COP4'
		when t.PRODUCT like '903022%' then 'Mezcla COP5'
		when t.PRODUCT like '903024%' then 'Mezcla SC2'
		when t.PRODUCT like '903025%' then 'SI COP4 - COP1'
		when t.PRODUCT like '903028%' then 'SI SC3'
		when t.PRODUCT like '903030%' then 'Mezcla SC3'
		when t.PRODUCT like '903031%' then 'Troncal SI Norte'
		when t.PRODUCT like '903032%' then 'Troncal SI SC3'
		when t.PRODUCT like '903033%' then 'Troncal SI SC6'
		when t.PRODUCT like '903034%' then 'Troncal SI COP5'
		when t.PRODUCT like '903035%' then 'Principal Brine 2 COP1'
		when t.PRODUCT like '904101%' then 'Brine Planta'
		when t.PRODUCT like '903047%' then 'Mezcla Poza 30-31'
		when t.PRODUCT like '904137%' then 'Traspaso COP4'
		when t.PRODUCT like '903057%' then 'SI COP7'
		when t.PRODUCT like '903054%' then 'Mezcla COP6'
		when t.PRODUCT like '904145%' then 'SI COP6'
		else null
		end as Centro,
	case
		when t.PRODUCT like '903003%' then 'SI'
		when t.PRODUCT like '903006%' then 'Brine'
		when t.PRODUCT like '903012%' then 'AFA'
		when t.PRODUCT like '903013%' then 'AFA'
		when t.PRODUCT like '903014%' then 'Brine'
		when t.PRODUCT like '903017%' then 'SI'
		when t.PRODUCT like '903018%' then 'SI'
		when t.PRODUCT like '903020%' then 'Mezcla'
		when t.PRODUCT like '903021%' then 'Mezcla'
		when t.PRODUCT like '903022%' then 'Mezcla'
		when t.PRODUCT like '903024%' then 'Mezcla'
		when t.PRODUCT like '903025%' then 'SI'
		when t.PRODUCT like '903028%' then 'SI'
		when t.PRODUCT like '903030%' then 'Mezcla'
		when t.PRODUCT like '903031%' then 'SI'
		when t.PRODUCT like '903032%' then 'SI'
		when t.PRODUCT like '903033%' then 'SI'
		when t.PRODUCT like '903034%' then 'SI'
		when t.PRODUCT like '903057%' then 'SI'
		when t.PRODUCT like '903035%' then 'Brine'
		when t.PRODUCT like '904101%' then 'Brine'
		when t.PRODUCT like '903047%' then 'Mezcla'
		when t.PRODUCT like '904137%' then 'Traspaso COP4'
		when t.PRODUCT like '903054%' then 'Mezcla'
		when t.PRODUCT like '904145%' then 'SI'
		
		else null
		end as Tipo_riego,
	case
		when t.PRODUCT like '903003%' then 'COP5'
		when t.PRODUCT like '903006%' then 'COP5'
		when t.PRODUCT like '903012%' then 'AFA NV'
		when t.PRODUCT like '903013%' then 'AFA Iris'
		when t.PRODUCT like '903014%' then 'COP2'
		when t.PRODUCT like '903017%' then 'SC2'
		when t.PRODUCT like '903018%' then 'COP2A'
		when t.PRODUCT like '903020%' then 'COP2A'
		when t.PRODUCT like '903021%' then 'COP4'
		when t.PRODUCT like '903022%' then 'COP5'
		when t.PRODUCT like '903024%' then 'SC2'
		when t.PRODUCT like '903025%' then 'COP4'
		when t.PRODUCT like '903028%' then 'SC3'
		when t.PRODUCT like '903030%' then 'SC3'
		when t.PRODUCT like '903031%' then 'Norte'
		when t.PRODUCT like '903032%' then 'SC3'
		when t.PRODUCT like '903033%' then 'SC6'
		when t.PRODUCT like '903034%' then 'COP5'
		when t.PRODUCT like '903035%' then 'COP1'
		when t.PRODUCT like '904101%' then 'Brine Planta'
		when t.PRODUCT like '903047%' then 'P30-31'
		when t.PRODUCT like '904137%' then 'COP4'
		when t.PRODUCT like '903057%' then 'COP7'
		when t.PRODUCT like '903054%' then 'COP6'
		when t.PRODUCT like '904145%' then 'COP6'
		else null
		end as Sector,
	case
		when t.Name = 'K' then cast(t.numeric_entry as float64)
		else null
		end as K,
	case
		when t.Name = 'I2' then cast(t.numeric_entry as float64)
		else null
		end as I2,
	case
		when t.Name = 'Na2SO4' then cast(t.numeric_entry as float64)
		else null
		end as Na2SO4,
	case
		when t.Name = 'NaNO3' then cast(t.numeric_entry as float64)
		else null
		end as NaNO3,
	case
		when t.Name = 'Mg' then cast(t.numeric_entry as float64)
		else null
		end as Mg,
	CASe
		when t.Name = 'NaCl' then cast(t.numeric_entry as float64)
		else null
		end as NaCl
from `datastream-prd.ds_lims_coya_sur.SQM_RESULTADOS` t
where t.PRODUCT like '%903003%' or
    t.PRODUCT like '%903006%' or
    t.PRODUCT like '%902012%' or 
    t.PRODUCT like '%903013%' or 
    t.PRODUCT like '%903014%' or 
    t.PRODUCT like '%903017%' or 
    t.PRODUCT like '%903018%' or 
    t.PRODUCT like '%903020%' or 
    t.PRODUCT like '%903021%' or 
    t.PRODUCT like '%903022%' or
    t.PRODUCT like '%903024%' or 
    t.PRODUCT like '%903025%' or 
    t.PRODUCT like '%903028%' or 
    t.PRODUCT like '%903030%' or 
    t.PRODUCT like '%903031%' or 
    t.PRODUCT like '%903032%' or 
    t.PRODUCT like '%903033%' or
    t.PRODUCT like '%903057%' or
    t.PRODUCT like '%903034%' or
    t.PRODUCT like '%903054%' or
    t.PRODUCT like '%904145%' or
    t.PRODUCT like '%903035%' or
    t.PRODUCT like '%904101%' or
    t.PRODUCT like '%903047%' or
    t.PRODUCT like '%904137%'
and t.SAMPLED_DATE >= '2021-01-01') t
group by t.FECHA, t.ID, t.Centro, t.Sector, t.Tipo_riego) x
where (x.Na2SO4_riego is not null and x.Mg_riego is not null and x.NaCl_riego is not null) and x.Tipo_riego != 'Brine'
group by x.Sector_riego, x.Tipo_riego, x.Semana_anho
'''
query_lims="""select *
from 
(select x.ID id_variable, x.descripcion Descripcion, substring(x.Descripcion,23,10) Pila2, substring(x.Descripcion,0,2) Tipo ,date(x.Fecha) fecha,
extract(isoweek from date(x.fecha)) Semana, extract(year from date(x.fecha)) Anho , concat(extract(year from date(x.fecha)),'-',extract(isoweek from date(x.fecha))) Semana_anho ,  avg(x.I2) I2, avg(x.NaNO3) NaNO3, avg(x.K) K, avg(x.Na2SO4) Na2SO4, avg(x.Mg) Mg, avg(x.NaCl) NaCl, avg(x.KClO4) KClO4, avg(x.Na2CO3) Na2CO3, avg(x.H3BO3) H3BO3, avg(x.Na) Na, avg(x.Ca) Ca, avg(x.NO3) NO3, 
case when (avg(x.Mg) > 0 and avg(x.Na2SO4) >0) then avg(x.Mg)-avg(x.Na2SO4)/11.7 else null end as MgL
from
(select t.PRODUCT ID, t.DESCRIPTION Descripcion, t.SAMPLED_DATE Fecha, 
    case when t.Name = "I2" then cast(t.numeric_entry as float64) else null end as I2,
    case when t.NAME  = 'Ca' then cast(t.numeric_entry as float64) else null end as Ca,
    case when t.NAME  = 'Na' then cast(t.numeric_entry as float64) else null end as Na,
    case when t.NAME  = 'Mg' then cast(t.numeric_entry as float64) else null end as Mg,
    case when t.NAME  = 'SO4' then cast(t.numeric_entry as float64) else null end as SO4,
    case when t.NAME  = 'H3BO3' then cast(t.numeric_entry as float64) else null end as H3BO3,
    case when t.NAME  = 'NO3' then cast(t.numeric_entry as float64) else null end as NO3,
    case when t.NAME  = 'NaCl' then cast(t.numeric_entry as float64) else null end as NaCl,
    case when t.NAME  = 'NaNO3' then cast(t.numeric_entry as float64) else null end as NaNO3,
    case when t.NAME  = 'Na2SO4' then cast(t.numeric_entry as float64) else null end as Na2SO4,
    case when t.NAME  = 'KClO4 IC' then cast(t.numeric_entry as float64) else null end as KClO4,
    case when t.NAME  = 'K' then cast(t.numeric_entry as float64) else null end as K,
    case when t.NAME  = 'Na2CO3' then cast(t.numeric_entry as float64) else null end as Na2CO3,
    t.UNIDAD Unidad
from `datastream-prd.ds_lims_coya_sur.SQM_RESULTADOS` t
where (t.Product = "904103" or t.Product = "904102")
and date(t.SAMPLED_DATE) >= '2021-01-01') x
group by x.ID, x.descripcion, x.fecha, x.Unidad) a
where a.Na2SO4 >0 and a.Mg >0 and a.NaCl >0
"""
# We define our celery broker. REDIS_URL is generated automatically on Dash Enterprise
# when your app is linked to a Redis Database
# If the app is running on Workspaces, we connect to the same Redis instance as the deployed app but a different Redis
# database
if os.environ.get("DASH_ENTERPRISE_ENV") == "WORKSPACE":
    parsed_url = urlparse(os.environ.get("REDIS_URL"))
    if parsed_url.path == "" or parsed_url.path == "/":
        i = 0
    else:
        try:
            i = int(parsed_url.path[1:])
        except:
            raise Exception("Redis database should be a number")
    parsed_url = parsed_url._replace(path="/{}".format((i + 1) % 16))

    updated_url = parsed_url.geturl()
    REDIS_URL = "redis://%s" % (updated_url.split("://")[1])
else:
    REDIS_URL = os.environ.get("REDIS_URL", "redis://127.0.0.1:6379")

celery_app = Celery(
    "Celery App", broker=REDIS_URL
)

# Create a SQLAlchemy connection string from the environment variable `DATABASE_URL`
# automatically created in your dash app when it is linked to a postgres container
# on Dash Enterprise. If you're running locally and `DATABASE_URL` is not defined,
# then this will fall back to a connection string for a local postgres instance
#  with username='postgres' and password='password'
connection_string = "postgresql+pg8000" + os.environ.get(
    "DATABASE_URL", "postgresql://postgres:password@127.0.0.1:5432"
).lstrip("postgresql")


# Create a SQLAlchemy engine object. This object initiates a connection pool
# so we create it once here and import into app.py.
# `poolclass=NullPool` prevents the Engine from using any connection more than once. You'll find more info here:
# https://docs.sqlalchemy.org/en/14/core/pooling.html#using-connection-pools-with-multiprocessing-or-os-fork
postgres_engine = create_engine(connection_string, poolclass=NullPool)

@celery_app.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):

    # This command invokes a celery task at an interval of every 5 seconds. You can change this.
    # Replace and overwrite this table every Monday at 7:30 am using df.to_sql's `if_exists` argument
    # so that this randomly generated data doesn't grow out of control.
    sender.add_periodic_task(
        crontab(hour="*/12",minute=0),
        update_data.s(if_exists="replace"),
        name="Reset data app sim pilas",
    )
    # crontab(,day_week=5)
    # hour="*/24",minute=0

@celery_app.task
def update_data(if_exists="append"):
    print("corriendo tarea")
    credentials_ext=service_account.Credentials.from_service_account_file('assets/datastream-ext-prd-cf3f594f6f43.json')
    credentials=service_account.Credentials.from_service_account_file('assets/datastream-prd-db4f1d3bf978.json')
    #INFO DE RIEGOS##################################################
    # extraer datos desde bigquery
    with pyodbc.connect('DRIVER='+driver+';SERVER=tcp:'+host+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password) as connection:
        pilas_tipo_riego = pd.read_sql(query_pilas_tiporiego,connection)

    sector_pilas = pandas_gbq.read_gbq(query_sector_pilas, credentials=credentials_ext)
    pilas_riego_tipo_sector = pandas_gbq.read_gbq(query_pilas_riego_tiposector, credentials=credentials)
    pilas_riego_tipo_sector.Tipo_riego=pilas_riego_tipo_sector.Tipo_riego.apply(lambda x: str.upper(x))    
    riegos=sector_pilas.merge(pilas_tipo_riego,left_on='Pila',right_on='PILA').drop(columns=['PILA'])
    ###EXTRAER DICCIONARIO SECTOR-PILAS ANTES DE HACER MERGE POR TIPO DE RIEGO##################
    sectores_pilas=riegos.drop_duplicates(subset=['COP_SC','Pila'])[['COP_SC','Pila']].rename(columns={'COP_SC':'Sector_riego'})
    ############################################################################################
    riegos=riegos.merge(pilas_riego_tipo_sector,how='left',right_on=["Sector_riego","Semana_anho","Tipo_riego"],left_on=["COP_SC","Semana_Anho","tipo_riego"]).drop(columns=["Semana_Anho","tipo_riego","COP_SC"])
    riegos.dropna(subset=["Sector_riego"],inplace=True)

    for i,row in riegos.iterrows():
        semana=row["Semana_anho"].split("-")[1]
        if len(semana)==1:
            semana="0"+semana
            semana_anho=row["Semana_anho"].split("-")[0]+"-"+semana
            riegos.at[i,"Semana_anho"]=semana_anho
    riegos.Pila=riegos.Pila.str.replace("-","").str.replace(" ","")
    riegos.Pila=riegos.Pila.apply(lambda x: str.upper(x))
    riegos.to_sql(tabla_riegos,postgres_engine,if_exists=if_exists,index=False)

    ##########################################################
    ### DATA DRENAJES###########################################

    data_lims=pandas_gbq.read_gbq(query_lims, credentials=credentials)
    data_lims.columns=[x+"_lims" for x in data_lims.columns]
    data_lims.Pila2_lims=data_lims.Pila2_lims.apply(lambda x: str(x))
    for i,row in data_lims.iterrows():
        semana=row["Semana_anho_lims"].split("-")[1]
        if len(semana)==1:
            semana="0"+semana
            semana_anho=row["Semana_anho_lims"].split("-")[0]+"-"+semana
            data_lims.at[i,"Semana_anho_lims"]=semana_anho
    data_lims.Pila2_lims=data_lims.Pila2_lims.str.replace("-","").str.replace(" ","")
    data_lims.Pila2_lims=data_lims.Pila2_lims.apply(lambda x: str.upper(x))
    data_lims=data_lims.loc[~(data_lims.Pila2_lims.str.contains("T"))]
    data_lims=data_lims.loc[~(data_lims.Pila2_lims.str.contains("R"))]
    data_lims.to_sql(tabla_data_lims,postgres_engine,if_exists=if_exists,index=False)

    ##################################################################################
    ########## SIMULACIONES ##########################################################

    #simulaciones_gproms=pd.read_csv('consolidado-simulaciones.csv')
    simulaciones_gproms=pandas_gbq.read_gbq('SELECT * FROM `datastream-ext-prd.ds_sim_lix.sim_lix`', credentials=credentials_ext)
    simulaciones_gproms.pila_sim=simulaciones_gproms.pila_sim.apply(lambda x: str(x))
    simulaciones_gproms.pila_sim=simulaciones_gproms.pila_sim.apply(lambda x: str.upper(x))
    simulaciones_gproms.pila_sim=simulaciones_gproms.pila_sim.str.replace(" ","").str.replace("-","")
    simulaciones_gproms.to_sql(tabla_simulaciones_gproms, postgres_engine, if_exists=if_exists, index=False)

    ########################################################################################

    pilas=pd.Series(data=simulaciones_gproms.pila_sim.unique(),name='pilas')

    pila_semana_anho=simulaciones_gproms.drop_duplicates(subset=['Semana_anho_sim','pila_sim'])[['pila_sim','Semana_anho_sim']]

    tabla_ternarios=data_lims.drop(columns=['id_variable_lims','Descripcion_lims','fecha_lims']).drop_duplicates(subset=['Pila2_lims','Semana_anho_lims'])
    
    tabla_ternarios_riego=riegos.drop_duplicates(subset=['Pila','Semana_anho'])

    ################TABLA DATOS TERNARIOS DRENAJES ####################################
    tabla_ternarios_final=pd.DataFrame()
    for index,row in tabla_ternarios.iterrows():
        sistemas=["NaNO3-Na2SO4-KCl","NaNO3-Na2SO4-MgCl2","NaNO3-Na2SO4-NaCl","NaNO3-Na2SO4-H2O"]
        for sistema in sistemas:
            df_sistema=dict()
            if sistema=="NaNO3-Na2SO4-KCl":        
                df_sistema["sistema"]=[sistema]
                df_sistema["tipo"]=row[["Tipo_lims"]]
                df_sistema["pila"]=[row["Pila2_lims"]]
                df_sistema["semana_anho"]=[row["Semana_anho_lims"]]
                df_sistema["NaNO3_sistema"]=[row["NaNO3_lims"]]
                df_sistema["Na2SO4_sistema"]=[row["Na2SO4_lims"]]
                df_sistema["KCl_sistema"]=[row["K_lims"]*(35.5+39.1)/39.1]
                df_sistema["suma_sistema"]=[df_sistema["KCl_sistema"][0]+row["Na2SO4_lims"]+row["NaNO3_lims"]]
                df_sistema["NaNO3_%_sistema"]=[(df_sistema["NaNO3_sistema"][0]/df_sistema["suma_sistema"][0])*100]
                df_sistema["Na2SO4_%_sistema"]=[(df_sistema["Na2SO4_sistema"][0]/df_sistema["suma_sistema"][0])*100]
                df_sistema["KCl_%_sistema"]=[(df_sistema["KCl_sistema"][0]/df_sistema["suma_sistema"][0])*100]
                df_sistema["Na2SO4_X_sistema"]=[((100-df_sistema["NaNO3_%_sistema"][0])*2-df_sistema["KCl_%_sistema"][0])*(2/(math.sqrt(3)))]
                df_sistema=pd.DataFrame.from_dict(df_sistema)
                tabla_ternarios_final=tabla_ternarios_final.append(df_sistema)
            if sistema=="NaNO3-Na2SO4-MgCl2":
                df_sistema["sistema"]=[sistema]
                df_sistema["tipo"]=row[["Tipo_lims"]]
                df_sistema["pila"]=[row["Pila2_lims"]]
                df_sistema["semana_anho"]=[row["Semana_anho_lims"]]
                df_sistema["NaNO3_sistema"]=[row["NaNO3_lims"]]
                df_sistema["Na2SO4_sistema"]=[row["Na2SO4_lims"]]
                df_sistema["MgCl2_sistema"]=[row["Mg_lims"]*95.3/24.3]
                df_sistema["suma_sistema"]=[df_sistema["MgCl2_sistema"][0]+row["Na2SO4_lims"]+row["NaNO3_lims"]]
                df_sistema["NaNO3_%_sistema"]=[(df_sistema["NaNO3_sistema"][0]/df_sistema["suma_sistema"][0])*100]
                df_sistema["Na2SO4_%_sistema"]=[(df_sistema["Na2SO4_sistema"][0]/df_sistema["suma_sistema"][0])*100]
                df_sistema["MgCl2_%_sistema"]=[(df_sistema["MgCl2_sistema"][0]/df_sistema["suma_sistema"][0])*100]
                df_sistema["Na2SO4_X_sistema"]=[((100-df_sistema["Na2SO4_%_sistema"][0])*2-df_sistema["MgCl2_%_sistema"][0])*(2/(math.sqrt(3)))]
                df_sistema=pd.DataFrame.from_dict(df_sistema)
                tabla_ternarios_final=tabla_ternarios_final.append(df_sistema)
            if sistema=="NaNO3-Na2SO4-NaCl":
                df_sistema["sistema"]=[sistema]
                df_sistema["tipo"]=row[["Tipo_lims"]]
                df_sistema["pila"]=[row["Pila2_lims"]]
                df_sistema["semana_anho"]=[row["Semana_anho_lims"]]
                df_sistema["NaNO3_sistema"]=[row["NaNO3_lims"]]
                df_sistema["Na2SO4_sistema"]=[row["Na2SO4_lims"]]
                df_sistema["NaCl_sistema"]=[row["NaCl_lims"]]
                df_sistema["suma_sistema"]=[df_sistema["NaCl_sistema"][0]+row["Na2SO4_lims"]+row["NaNO3_lims"]]
                df_sistema["NaNO3_%_sistema"]=[(df_sistema["NaNO3_sistema"][0]/df_sistema["suma_sistema"][0])*100]
                df_sistema["Na2SO4_%_sistema"]=[(df_sistema["Na2SO4_sistema"][0]/df_sistema["suma_sistema"][0])*100]
                df_sistema["NaCl_%_sistema"]=[(df_sistema["NaCl_sistema"][0]/df_sistema["suma_sistema"][0])*100]
                df_sistema["Na2SO4_X_sistema"]=[((100-df_sistema["Na2SO4_%_sistema"][0])*2-df_sistema["NaCl_%_sistema"][0])*(2/(math.sqrt(3)))]
                df_sistema=pd.DataFrame.from_dict(df_sistema)
                tabla_ternarios_final=tabla_ternarios_final.append(df_sistema)
            if sistema=="NaNO3-Na2SO4-H2O":
                df_sistema["sistema"]=[sistema]
                df_sistema["tipo"]=row[["Tipo_lims"]]
                df_sistema["pila"]=[row["Pila2_lims"]]
                df_sistema["semana_anho"]=[row["Semana_anho_lims"]]
                df_sistema["NaNO3_sistema"]=[row["NaNO3_lims"]]
                df_sistema["Na2SO4_sistema"]=[row["Na2SO4_lims"]]
                df_sistema["H2O_sistema"]=[(row["NaNO3_lims"]/(0.0014688*row["NaNO3_lims"]-0.0394723))]
                df_sistema["suma_sistema"]=[df_sistema["H2O_sistema"][0]+row["Na2SO4_lims"]+row["NaNO3_lims"]]
                df_sistema["NaNO3_%_sistema"]=[(df_sistema["NaNO3_sistema"][0]/df_sistema["suma_sistema"][0])*100]
                df_sistema["Na2SO4_%_sistema"]=[(df_sistema["Na2SO4_sistema"][0]/df_sistema["suma_sistema"][0])*100]
                df_sistema["H2O_%_sistema"]=[(df_sistema["H2O_sistema"][0]/df_sistema["suma_sistema"][0])*100]
                df_sistema["Na2SO4_X_sistema"]=[((100-df_sistema["Na2SO4_%_sistema"][0])*2-df_sistema["H2O_%_sistema"][0])*(2/(math.sqrt(3)))]
                df_sistema=pd.DataFrame.from_dict(df_sistema)
                tabla_ternarios_final=tabla_ternarios_final.append(df_sistema)

    ################TABLA DATOS TERNARIOS RIEGOS ####################################
    tabla_ternarios_riegos_final=pd.DataFrame()
    for index,row in tabla_ternarios_riego.iterrows():
        sistemas=["NaNO3-Na2SO4-KCl","NaNO3-Na2SO4-MgCl2","NaNO3-Na2SO4-NaCl","NaNO3-Na2SO4-H2O"]
        for sistema in sistemas:
            df_sistema=dict()
            if sistema=="NaNO3-Na2SO4-KCl":        
                df_sistema["sistema"]=[sistema]
                df_sistema["tipo"]=row[["Tipo_riego"]]
                df_sistema["pila"]=[row["Pila"]]
                df_sistema["semana_anho"]=[row["Semana_anho"]]
                df_sistema["NaNO3_sistema"]=[row["NaNO3_riego"]]
                df_sistema["Na2SO4_sistema"]=[row["Na2SO4_riego"]]
                df_sistema["KCl_sistema"]=[row["K_riego"]*(35.5+39.1)/39.1]
                df_sistema["suma_sistema"]=[df_sistema["KCl_sistema"][0]+row["Na2SO4_riego"]+row["NaNO3_riego"]]
                df_sistema["NaNO3_%_sistema"]=[(df_sistema["NaNO3_sistema"][0]/df_sistema["suma_sistema"][0])*100]
                df_sistema["Na2SO4_%_sistema"]=[(df_sistema["Na2SO4_sistema"][0]/df_sistema["suma_sistema"][0])*100]
                df_sistema["KCl_%_sistema"]=[(df_sistema["KCl_sistema"][0]/df_sistema["suma_sistema"][0])*100]
                df_sistema["Na2SO4_X_sistema"]=[((100-df_sistema["NaNO3_%_sistema"][0])*2-df_sistema["KCl_%_sistema"][0])*(2/(math.sqrt(3)))]
                df_sistema=pd.DataFrame.from_dict(df_sistema)
                tabla_ternarios_riegos_final=tabla_ternarios_riegos_final.append(df_sistema)
            if sistema=="NaNO3-Na2SO4-MgCl2":
                df_sistema["sistema"]=[sistema]
                df_sistema["tipo"]=row[["Tipo_riego"]]
                df_sistema["pila"]=[row["Pila"]]
                df_sistema["semana_anho"]=[row["Semana_anho"]]
                df_sistema["NaNO3_sistema"]=[row["NaNO3_riego"]]
                df_sistema["Na2SO4_sistema"]=[row["Na2SO4_riego"]]
                df_sistema["MgCl2_sistema"]=[row["Mg_riego"]*95.3/24.3]
                df_sistema["suma_sistema"]=[df_sistema["MgCl2_sistema"][0]+row["Na2SO4_riego"]+row["NaNO3_riego"]]
                df_sistema["NaNO3_%_sistema"]=[(df_sistema["NaNO3_sistema"][0]/df_sistema["suma_sistema"][0])*100]
                df_sistema["Na2SO4_%_sistema"]=[(df_sistema["Na2SO4_sistema"][0]/df_sistema["suma_sistema"][0])*100]
                df_sistema["MgCl2_%_sistema"]=[(df_sistema["MgCl2_sistema"][0]/df_sistema["suma_sistema"][0])*100]
                df_sistema["Na2SO4_X_sistema"]=[((100-df_sistema["Na2SO4_%_sistema"][0])*2-df_sistema["MgCl2_%_sistema"][0])*(2/(math.sqrt(3)))]
                df_sistema=pd.DataFrame.from_dict(df_sistema)
                tabla_ternarios_riegos_final=tabla_ternarios_riegos_final.append(df_sistema)
            if sistema=="NaNO3-Na2SO4-NaCl":
                df_sistema["sistema"]=[sistema]
                df_sistema["tipo"]=row[["Tipo_riego"]]
                df_sistema["pila"]=[row["Pila"]]
                df_sistema["semana_anho"]=[row["Semana_anho"]]
                df_sistema["NaNO3_sistema"]=[row["NaNO3_riego"]]
                df_sistema["Na2SO4_sistema"]=[row["Na2SO4_riego"]]
                df_sistema["NaCl_sistema"]=[row["NaCl_riego"]]
                df_sistema["suma_sistema"]=[df_sistema["NaCl_sistema"][0]+row["Na2SO4_riego"]+row["NaNO3_riego"]]
                df_sistema["NaNO3_%_sistema"]=[(df_sistema["NaNO3_sistema"][0]/df_sistema["suma_sistema"][0])*100]
                df_sistema["Na2SO4_%_sistema"]=[(df_sistema["Na2SO4_sistema"][0]/df_sistema["suma_sistema"][0])*100]
                df_sistema["NaCl_%_sistema"]=[(df_sistema["NaCl_sistema"][0]/df_sistema["suma_sistema"][0])*100]
                df_sistema["Na2SO4_X_sistema"]=[((100-df_sistema["Na2SO4_%_sistema"][0])*2-df_sistema["NaCl_%_sistema"][0])*(2/(math.sqrt(3)))]
                df_sistema=pd.DataFrame.from_dict(df_sistema)
                tabla_ternarios_riegos_final=tabla_ternarios_riegos_final.append(df_sistema)
            if sistema=="NaNO3-Na2SO4-H2O":
                df_sistema["sistema"]=[sistema]
                df_sistema["tipo"]=row[["Tipo_riego"]]
                df_sistema["pila"]=[row["Pila"]]
                df_sistema["semana_anho"]=[row["Semana_anho"]]
                df_sistema["NaNO3_sistema"]=[row["NaNO3_riego"]]
                df_sistema["Na2SO4_sistema"]=[row["Na2SO4_riego"]]
                df_sistema["H2O_sistema"]=[(row["NaNO3_riego"]/(0.0014688*row["NaNO3_riego"]-0.0394723))]
                df_sistema["suma_sistema"]=[df_sistema["H2O_sistema"][0]+row["Na2SO4_riego"]+row["NaNO3_riego"]]
                df_sistema["NaNO3_%_sistema"]=[(df_sistema["NaNO3_sistema"][0]/df_sistema["suma_sistema"][0])*100]
                df_sistema["Na2SO4_%_sistema"]=[(df_sistema["Na2SO4_sistema"][0]/df_sistema["suma_sistema"][0])*100]
                df_sistema["H2O_%_sistema"]=[(df_sistema["H2O_sistema"][0]/df_sistema["suma_sistema"][0])*100]
                df_sistema["Na2SO4_X_sistema"]=[((100-df_sistema["Na2SO4_%_sistema"][0])*2-df_sistema["H2O_%_sistema"][0])*(2/(math.sqrt(3)))]
                df_sistema=pd.DataFrame.from_dict(df_sistema)
                tabla_ternarios_riegos_final=tabla_ternarios_riegos_final.append(df_sistema)
            
    del tabla_ternarios

    tabla_ternarios_final.to_sql(tabla_ternarios_sistemas,postgres_engine,if_exists=if_exists,index=False)
    tabla_ternarios_riegos_final.to_sql(tabla_ternarios_sistemas_riego,postgres_engine,if_exists=if_exists,index=False)
    pilas.to_sql(tabla_pilas_dict, postgres_engine, if_exists=if_exists, index=False)
    sectores_pilas.to_sql(tabla_sectores_pilas_dict, postgres_engine, if_exists=if_exists, index=False)
    pila_semana_anho.to_sql(tabla_pila_semana_anho, postgres_engine, if_exists=if_exists, index=False)

    return

@celery_app.task
def cargar_ternarios_incial(if_exists='replace'):
    ternarios=pd.read_csv('datos-ternarios.csv')
    ternarios.to_sql(tabla_ternarios, postgres_engine, if_exists=if_exists, index=False)
    return