import os

import polars as pl
import pytest
from dotenv import load_dotenv

from utils import tableau

load_dotenv()


def test_find_view_luid() -> None:
    """test find_view_luid function"""
    luid = tableau.find_view_luid(view_name='testdisp', workbook_name='testing')
    assert luid == os.environ['TESTDISP_LUID']

    with pytest.raises(tableau.TableauLUIDNotFoundError):
        _luid = tableau.find_view_luid(view_name='fake view', workbook_name='testing')


def test_lazyframe_from_view_id() -> None:
    """test lazyframe_from_view_id"""
    lf = tableau.lazyframe_from_view_id(os.environ['TESTDISP_LUID'])
    assert isinstance(lf, pl.LazyFrame)
    df = lf.collect()
    assert df.columns == ['test_var']
    assert df['test_var'].first() == 64_209

    with pytest.raises(tableau.TableauNoDataError):
        _lf = tableau.lazyframe_from_view_id(os.environ['TESTEMPTY_LUID'])
