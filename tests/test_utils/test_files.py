import io
from pathlib import Path

import pytest

from utils import files


def test_warn_file_age(monkeypatch: pytest.MonkeyPatch) -> None:
    """test warn_file_age function"""
    monkeypatch.setattr('sys.stdin', io.StringIO('y'))
    fp = Path('LICENSE')
    with pytest.warns(UserWarning, match=r'has not been updated recently!'):
        files.warn_file_age(fp)
    monkeypatch.setattr('sys.stdin', io.StringIO('n'))
    with pytest.warns(UserWarning, match=r'has not been updated recently!'), pytest.raises(SystemExit) as se:
        files.warn_file_age(fp)
    assert se.value.code == 'update files and run again'
