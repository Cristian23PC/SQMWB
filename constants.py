import os

tabla_sectores_pilas_dict='tabla_sectores_pilas_dict'
tabla_sectores_pilas_dict='workspace_tabla_sectores_pilas_dict' if os.environ.get('DASH_ENTERPRISE_ENV')=='WORKSPACE' else 'tabla_sectores_pilas_dict'

tabla_pilas_dict='tabla_pilas_dict'
tabla_pilas_dict='workspace_tabla_pilas_dict' if os.environ.get('DASH_ENTERPRISE_ENV')=='WORKSPACE' else 'tabla_pilas_dict'

tabla_simulaciones_gproms='tabla_simulaciones_gproms'
tabla_simulaciones_gproms='workspace_tabla_simulaciones_gproms' if os.environ.get('DASH_ENTERPRISE_ENV')=='WORKSPACE' else 'tabla_simulaciones_gproms'

tabla_ternarios_sistemas='tabla_ternarios_sistemas'
tabla_ternarios_sistemas='workspace_tabla_ternarios_sistemas' if os.environ.get('DASH_ENTERPRISE_ENV')=='WORKSPACE' else 'tabla_ternarios_sistemas'

tabla_pila_semana_anho='tabla_pila_semana_anho'
tabla_pila_semana_anho='workspace_tabla_pila_semana_anho' if os.environ.get('DASH_ENTERPRISE_ENV')=='WORKSPACE' else 'tabla_pila_semana_anho'

tabla_ternarios='tabla_ternarios'
tabla_ternarios='workspace_tabla_ternarios' if os.environ.get('DASH_ENTERPRISE_ENV')=='WORKSPACE' else 'tabla_ternarios'

tabla_data_lims='tabla_data_lims'
tabla_data_lims='workspace_tabla_data_lims' if os.environ.get('DASH_ENTERPRISE_ENV')=='WORKSPACE' else 'tabla_data_lims'

tabla_riegos='tabla_riegos'
tabla_riegos='workspace_tabla_riegos' if os.environ.get('DASH_ENTERPRISE_ENV')=='WORKSPACE' else 'tabla_riegos'

tabla_ternarios_sistemas_riego='tabla_ternarios_sistemas_riego'
tabla_ternarios_sistemas_riego='workspace_tabla_ternarios_sistemas_riego' if os.environ.get('DASH_ENTERPRISE_ENV')=='WORKSPACE' else 'tabla_ternarios_sistemas_riego'