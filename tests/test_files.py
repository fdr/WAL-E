import errno
import pytest

from wal_e import files


def test_no_error(tmpdir):
    p = unicode(tmpdir.join('somefile'))
    with files.DeleteOnError(p) as f:
        f.write('hello')

    with open(p) as f:
        assert f.read() == 'hello'


def test_clear_on_error(tmpdir):
    p = unicode(tmpdir.join('somefile'))

    boom = StandardError('Boom')
    with pytest.raises(StandardError) as e:
        with files.DeleteOnError(p) as f:
            f.write('hello')
            raise boom
    assert e.value == boom

    with pytest.raises(IOError) as e:
        open(p)

    assert e.value.errno == errno.ENOENT
