import os
from io import BytesIO

import polars as pl
from dotenv import load_dotenv
from tableauserverclient.models.tableau_auth import PersonalAccessTokenAuth
from tableauserverclient.server.pager import Pager
from tableauserverclient.server.request_options import CSVRequestOptions
from tableauserverclient.server.server import Server


class TableauNoDataError(Exception):
    """custom exception for when there is no data in the view"""
    def __init__(self, message: str = 'no data in view') -> None:
        """initializes the error"""
        self.message = message
        super().__init__(self.message)


def lazyframe_from_view_id(view_id: str, filters: dict | None = None, **kwargs) -> pl.LazyFrame:
    """
    pulls a lazyframe from the specified view in tableau

    args:
        view_id: a string, luid of the target view, can be found with `find_luid()`
        filters: optional filters to apply before pulling the lazyframe
        kwargs:  optional kwargs to pass to polars `scan_csv()`

    raises:
        TableauNoDataError: for when the result is an empty LazyFrame

    returns:
        a LazyFrame containing the data from the specified view, filtered if
        filters are specified
    """
    load_dotenv()

    server = os.environ['TABLEAU_SERVER']
    site = os.environ['TABLEAU_SITE']
    token_name = os.environ['TABLEAU_TOKEN_NAME']
    token_value = os.environ['TABLEAU_TOKEN_VALUE']

    tableau_auth = PersonalAccessTokenAuth(token_name, token_value, site)
    tableau_server = Server(server, use_server_version=True, http_options={'verify': False})

    with tableau_server.auth.sign_in(tableau_auth):
        if filters:
            options = CSVRequestOptions()
            for k, v in filters.items():
                options.vf(k, v)
        else:
            options = None
        view = tableau_server.views.get_by_id(view_id)
        tableau_server.views.populate_csv(view, options)
        buffer = BytesIO()
        buffer.write(b''.join(view.csv))
        buffer.seek(0)
        if len(buffer.getvalue()) <= 1:
            msg = f'{view_id} with {filters} has no data'
            raise TableauNoDataError(msg)
        return pl.scan_csv(buffer, **kwargs)


class TableauLUIDNotFoundError(Exception):
    """custom exception for when luid is not found"""
    def __init__(self, message: str = 'luid not found') -> None:
        """initializes the error"""
        self.message = message
        super().__init__(self.message)


def find_view_luid(view_name: str, workbook_name: str) -> str:
    """
    gets the luid from the `view_name` in `workbook_name`

    args:
        view_name: string name of the target view
        workbook_name: string name of the workbook the view is in

    raises:
    TableauLUIDNotFoundError: raised when luid could not be found

    returns:
        string luid of the target view
    """
    load_dotenv()

    server = os.environ['TABLEAU_SERVER']
    site = os.environ['TABLEAU_SITE']
    token_name = os.environ['TABLEAU_TOKEN_NAME']
    token_value = os.environ['TABLEAU_TOKEN_VALUE']

    tableau_auth = PersonalAccessTokenAuth(token_name, token_value, site)
    tableau_server = Server(server, use_server_version=True, http_options={'verify': False})

    with tableau_server.auth.sign_in(tableau_auth):
        all_workbooks = list(Pager(tableau_server.workbooks))
        try:
            searched_workbook = next(workbook for workbook in all_workbooks if workbook.name == workbook_name)
        except StopIteration as error:
            msg = f'workbook {workbook_name!r} not found'
            raise TableauLUIDNotFoundError(msg) from error
        tableau_server.workbooks.populate_views(searched_workbook)
        views = searched_workbook.views
        try:
            searched_view = next(view for view in views if view.name == view_name)
        except StopIteration as error:
            msg = f'view {view_name!r} not found in workbook {workbook_name!r}'
            raise TableauLUIDNotFoundError(msg) from error
        if searched_view.id is None:
            msg = f'{searched_view} has None for id'
            raise TableauLUIDNotFoundError(msg)
        return searched_view.id
