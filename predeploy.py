from tasks import update_data, cargar_ternarios_incial
import pandas as pd
import pyodbc
# Call the Celery task that pre-populates the Postgres DB with the updated dataframe
# when the app is deployed (i.e. when this file is executed)
update_data(if_exists="replace")
cargar_ternarios_incial(if_exists='replace')
