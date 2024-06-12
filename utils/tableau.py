from io import BytesIO
from typing import Any

import polars as pl
import toml
from tableauserverclient.models.tableau_auth import PersonalAccessTokenAuth
from tableauserverclient.server.pager import Pager
from tableauserverclient.server.request_options import CSVRequestOptions
from tableauserverclient.server.server import Server


def lazyframe_from_view_id(view_id:str, filters:dict|None=None, **kwargs:Any) -> pl.LazyFrame:
    """
    pulls a lazyframe from the specified view in tableau    

    args:
        view_id: a string, luid of the target view, can be found with `find_luid()`
        filters: optional filters to apply before pulling the lazyframe
        kwargs:  optional kwargs to pass to polars `read_csv()`

    returns:
        a LazyFrame containing the data from the specified view, filtered if 
        filters are specified 
    """
    with open('../secrets.toml', 'r') as f:
        secrets = toml.load(f)

    server = secrets['tableau']['server']
    site = secrets['tableau']['site']
    token_name = secrets['tableau']['token_name']
    token_value = secrets['tableau']['token_value']

    tableau_auth = PersonalAccessTokenAuth(token_name, token_value, site)
    tableau_server = Server(server, use_server_version=True, http_options={'verify':False})

    with tableau_server.auth.sign_in(tableau_auth):
        if filters:
            options = CSVRequestOptions()
            for k,v in filters.items():
                options.vf(k,v)
        else:
            options = None
        view = tableau_server.views.get_by_id(view_id)
        tableau_server.views.populate_csv(view, options)
        buffer = BytesIO()
        buffer.write(b''.join(view.csv))
        buffer.seek(0)
        return pl.read_csv(buffer, **kwargs).lazy()

def find_view_luid(view_name:str, workbook_name:str) -> str:
    """
    gets the luid from the `view_name` in `workbook_name`

    args:
        view_name: string name of the target view
        workbook_name: string name of the workbook the view is in

    returns:
        string luid of the target view
    """
    with open('../secrets.toml', 'r') as f:
        secrets = toml.load(f)

    server = secrets['tableau']['server']
    site = secrets['tableau']['site']
    token_name = secrets['tableau']['token_name']
    token_value = secrets['tableau']['token_value']

    tableau_auth = PersonalAccessTokenAuth(token_name, token_value, site)
    tableau_server = Server(server, use_server_version=True, http_options={'verify':False})

    with tableau_server.auth.sign_in(tableau_auth):
        all_workbooks = list(Pager(tableau_server.workbooks))
        searched_workbook = [workbook for workbook in all_workbooks if workbook.name==workbook_name][0]
        tableau_server.workbooks.populate_views(searched_workbook)
        views = searched_workbook.views
        searched_view = [view for view in views if view.name==view_name][0]
        return searched_view.id
