"""Public automatic downloads must not write outside the research workspace."""
import importlib.util
import io
from pathlib import Path
import tarfile
import pytest

source=Path(__file__).resolve().parents[1]/'scripts/fetch_data.py'
spec=importlib.util.spec_from_file_location('bundle_fetch',source)
fetch=importlib.util.module_from_spec(spec);spec.loader.exec_module(fetch)

def archive(path,name,kind=None):
    with tarfile.open(path,'w:gz') as tf:
        item=tarfile.TarInfo(name)
        if kind is not None:item.type=kind;item.linkname='../escape';tf.addfile(item)
        else:item.size=2;tf.addfile(item,io.BytesIO(b'ok'))

def test_regular_nested_file(tmp_path):
    path=tmp_path/'input.tar.gz';archive(path,'data/example.txt')
    assert fetch.extract(path,tmp_path/'out')==['data/example.txt']
    assert (tmp_path/'out/data/example.txt').read_bytes()==b'ok'

@pytest.mark.parametrize('name,kind',[('../escape',None),('/outside',None),('data/link',tarfile.SYMTYPE)])
def test_forbidden_archive_members(tmp_path,name,kind):
    path=tmp_path/'input.tar.gz';archive(path,name,kind)
    with pytest.raises(ValueError,match='Unsafe archive member'):fetch.extract(path,tmp_path/'out')
    assert not (tmp_path/'escape').exists()
