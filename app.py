from dash import Dash, html, dcc, page_container,page_registry
import dash_design_kit as ddk
import plotly.express as px
from theme import theme

app = Dash(__name__,use_pages=True,suppress_callback_exceptions=True, 
    prevent_initial_callbacks=True,
    update_title='Cargando...')
server = app.server  # expose server variable for Procfile


menu = ddk.Menu(
    [
            dcc.Link(
                    f"{page['name']}", href=page["relative_path"]
                )
            for page in page_registry.values()
        ]
)



app.layout = ddk.App(theme=theme,children=[

    ddk.Sidebar([
        ddk.Logo(
            src=app.get_asset_url('logo_sqm_black.png'),
        ),
        ddk.Title('Pilas Lixiviación'),
        menu
    ], foldable=True),

    page_container

])


if __name__ == '__main__':
    app.run_server(debug=True)
