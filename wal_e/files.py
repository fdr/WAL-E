import os

class DeleteOnError(object):
    def __init__(self, where):
        self.where = where
        self.f = None

    def __enter__(self):
        self.f = open(self.where, 'wb')
        return self.f

    def __exit__(self, typ, value, traceback):
        try:
            if typ is not None:
                os.unlink(self.where)
        finally:
            if self.f:
                self.f.close()
