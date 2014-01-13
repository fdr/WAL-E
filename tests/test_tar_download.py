import pytest

from wal_e import retries
from wal_e import storage
from wal_e.operator import s3_operator
from wal_e.storage import base


class Explosion(Exception):
    """To mark injected faults."""
    pass


@pytest.fixture
def cripple_retry(monkeypatch):
    def noop_decorate(f):
        return f

    def always_noop_decorates(*args, **kwargs):
        return noop_decorate

    monkeypatch.setattr(retries, 'retry', always_noop_decorates)


def test_backup_fetch_explosion(cripple_retry, monkeypatch, tmpdir):
    store = storage.StorageLayout('s3://whatever/bogus')
    backup_cxt = s3_operator.S3Backup(store, None, None)
    backup_info = base.BackupInfo(
        name='base_{0}_{1}'.format('0' * 8 * 3, '0' * 8), layout=store)

    monkeypatch.setattr(s3_operator.S3Backup, 'new_connection',
                        lambda *args: None)
    monkeypatch.setattr(base.BackupInfo, 'load_detail', lambda *args: None)

    exception = Explosion('Boom')

    class FakeBackupFetcher(object):
        def __init__(self, *args, **kwargs):
            pass

        def fetch_partition(self, *args, **kwargs):
            raise exception

    monkeypatch.setattr(backup_cxt.worker, 'BackupFetcher', FakeBackupFetcher)

    class FakePartitionLister(object):
        def __init__(self, *args, **kwargs):
            pass

        def __iter__(self):
            for i in xrange(30):
                yield unicode(i)

    monkeypatch.setattr(backup_cxt.worker, 'TarPartitionLister',
                        FakePartitionLister)

    with pytest.raises(Exception) as e:
        backup_cxt._database_fetch_from_backup_info(
            backup_info, unicode(tmpdir), True, None, 2)

    assert e.value is exception
