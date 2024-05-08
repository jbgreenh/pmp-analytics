import tableauserverclient as TSC
import polars as pl
import toml
from io import BytesIO
from typing import Optional


def lazyframe_from_view_id(view_id:str, filters:Optional[dict]=None) -> pl.LazyFrame: 
    with open('../secrets.toml', 'r') as f:
        secrets = toml.load(f)

    server = secrets['tableau']['server']
    site = secrets['tableau']['site']
    token_name = secrets['tableau']['token_name']
    token_value = secrets['tableau']['token_value']

    tableau_auth = TSC.PersonalAccessTokenAuth(token_name, token_value, site)
    tableau_server = TSC.Server(server, use_server_version=True, http_options={'verify':False})

    with tableau_server.auth.sign_in(tableau_auth):
        if filters:
            options = TSC.CSVRequestOptions()
            for k,v in filters.items():
                options.vf(k,v)
        else:
            options = None
        view = tableau_server.views.get_by_id(view_id)
        tableau_server.views.populate_csv(view, options)
        buffer = BytesIO()
        buffer.write(b''.join(view.csv))
        buffer.seek(0)
        return pl.read_csv(buffer, infer_schema_length=10000).lazy()

def find_view_luid(view_name:str, workbook_name:str) -> str:
    with open('../secrets.toml', 'r') as f:
        secrets = toml.load(f)

    server = secrets['tableau']['server']
    site = secrets['tableau']['site']
    token_name = secrets['tableau']['token_name']
    token_value = secrets['tableau']['token_value']

    tableau_auth = TSC.PersonalAccessTokenAuth(token_name, token_value, site)
    tableau_server = TSC.Server(server, use_server_version=True, http_options={'verify':False})
    
    with tableau_server.auth.sign_in(tableau_auth):
        all_workbooks = list(TSC.Pager(tableau_server.workbooks))
        searched_workbook = [workbook for workbook in all_workbooks if workbook.name==workbook_name][0]
        tableau_server.workbooks.populate_views(searched_workbook)
        views = searched_workbook.views
        searched_view = [view for view in views if view.name==view_name][0]
        return searched_view.id
