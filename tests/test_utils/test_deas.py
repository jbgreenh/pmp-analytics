import polars as pl

from utils import deas


def test_deas() -> None:
    """tests that deas util leads to a non-empty lazyframe for all valid p values"""
    p_list = ['all', 'az', 'pharm', 'presc']
    for p in p_list:
        deas_lf = deas.deas(p)  # type: ignore[reportArgumentType]
        assert isinstance(deas_lf, pl.LazyFrame)
        assert not deas_lf.collect().is_empty()
