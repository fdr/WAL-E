import pytest

from wal_e.worker import prefetch
from wal_e import worker


@pytest.fixture
def pd(tmpdir):
    d = prefetch.Dirs(unicode(tmpdir))
    return d


@pytest.fixture
def seg():
    return worker.WalSegment('0' * 8 * 3)


def test_double_create(pd, seg):
    pd.create(seg)
    pd.create(seg)


def test_atomic_download(pd, seg, tmpdir):
    assert not pd.is_running(seg)

    pd.create(seg)
    assert pd.is_running(seg)

    with pd.download(seg) as ad:
        s = 'hello'
        ad.tf.write(s)
        ad.tf.flush()
        assert pd.running_size(seg) == len(s)

    assert pd.contains(seg)
    assert not pd.is_running(seg)

    promote_target = tmpdir.join('another-spot')
    pd.promote(seg, unicode(promote_target))

    pd.clear()
    assert not pd.contains(seg)


def test_atomic_download_failure(pd, seg):
    "Ensure a raised exception doesn't move WAL into place"
    pd.create(seg)
    e = Exception('Anything')

    with pytest.raises(Exception) as err:
        with pd.download(seg):
            raise e

    assert err.value is e
    assert not pd.is_running(seg)
    assert not pd.contains(seg)


def test_cleanup_running(pd, seg):
    pd.create(seg)
    assert pd.is_running(seg)

    nxt = seg.future_segment_stream().next()
    pd.clear_except([nxt])
    assert not pd.is_running(seg)


def test_cleanup_promoted(pd, seg):
    pd.create(seg)
    assert pd.is_running(seg)

    with pd.download(seg):
        pass

    assert not pd.is_running(seg)
    assert pd.contains(seg)

    nxt = seg.future_segment_stream().next()
    pd.clear_except([nxt])
    assert not pd.contains(seg)
