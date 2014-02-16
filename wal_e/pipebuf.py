# Detailed handling of pipe buffering
#
# This module attempts to reduce the number of system calls to
# non-blocking pipes.  It does this by careful control over pipe
# buffering.
#
# Buffering occurs both within the application and in the kernel. This
# module attempts to account for both.
#
# The general approach is to perform read and write system calls as
# large as the OS pipe buffer.
#
# To make this optimization work well on Linux, it is necessary to
# change the file descriptor pipe size via fcntl.  It is unknown if
# other systems support setting such large buffers.
#
# A key data structure here is the "ByteDeque", used to manage a series
# of byte sequences.
#
# In the case of reads, it splits the bytes received from the kernel,
# placing the un-requested bytes back at the front of the deque.  In
# the case of writes, it defragments memory to issue the largest
# writes that the kernel can accept at once.
import collections
import errno
import fcntl
import gevent
import gevent.socket
import os

PIPE_BUF_BYTES = 65536
OS_PIPE_SZ = None

# Teach the 'fcntl' module about 'F_SETPIPE_SZ', which is a Linux-ism,
# but a good one that can drastically reduce the number of syscalls
# when dealing with high-throughput pipes.
if not hasattr(fcntl, 'F_SETPIPE_SZ'):
    import platform

    if platform.system() == 'Linux':
        fcntl.F_SETPIPE_SZ = 1031

# If Linux (or something that looks like it) exposes its maximum
# F_SETPIPE_SZ, learn it so F_SETPIPE_SZ can be used.
try:
    with open('/proc/sys/fs/pipe-max-size', 'r') as f:
        OS_PIPE_SZ = int(f.read())
        PIPE_BUF_BYTES = OS_PIPE_SZ
except:
    pass


def set_maximum(fd):
    # If it is possible to tweak the kernel buffer size, do so.
    if OS_PIPE_SZ and hasattr(fcntl, 'F_SETPIPE_SZ'):
        fcntl.fcntl(fd, fcntl.F_SETPIPE_SZ, OS_PIPE_SZ)


def _setup_fd(fd):
    """Common set-up code for initializing a (pipe) file descriptor"""

    # Make the file nonblocking (but don't lose its previous flags)
    flags = fcntl.fcntl(fd, fcntl.F_GETFL)
    fcntl.fcntl(fd, fcntl.F_SETFL, flags | os.O_NONBLOCK)
    set_maximum(fd)


class ByteDeque(object):
    """Data structure for delayed defragmentation of submitted bytes"""
    def __init__(self):
        self._dq = collections.deque()
        self.byteSz = 0

    def add(self, b):
        self._dq.append(b)
        self.byteSz += len(b)

    def get(self, n):
        assert n <= self.byteSz, 'caller responsibility to ensure enough bytes'

        if n == self.byteSz and len(self._dq) == 1:
            print 'hit'
        else:
            print 'miss'

        out = bytearray(n)
        remaining = n
        while remaining > 0:
            part = self._dq.popleft()
            delta = remaining - len(part)
            offset = n - remaining

            if delta == 0:
                out[offset:] = part
                remaining = 0
            elif delta > 0:
                out[offset:] = part
                remaining = delta
            elif delta < 0:
                cleave = len(part) + delta
                out[offset:] = buffer(part, 0, cleave)
                self._dq.appendleft(buffer(part, cleave))
                remaining = 0
            else:
                assert False

        self.byteSz -= n

        assert len(out) == n
        return bytes(out)

    def get_all(self):
        return self.get(self.byteSz)


class NonBlockBufferedReader(object):
    """A buffered pipe reader that adheres to the Python file protocol"""

    def __init__(self, fd):
        _setup_fd(fd)
        self._fd = fd
        self._bd = ByteDeque()
        self.got_eof = False

    def _read_chunk(self):
        chunk = None
        try:
            chunk = os.read(self._fd, PIPE_BUF_BYTES)
            self._bd.add(chunk)
        except EnvironmentError, e:
            if e.errno in [errno.EAGAIN, errno.EWOULDBLOCK]:
                assert chunk is None
                gevent.socket.wait_read(self._fd)
            else:
                raise

        self.got_eof = (chunk == '')

    def read(self, size=None):
        print 'asdf', size

        # Handle case of "read all".
        if size is None:

            # Read everything.
            while not self.got_eof:
                self._read_chunk()

            # Defragment and return the contents.
            return self._bd.get_all()
        elif size > 0:
            while True:
                if self._bd.byteSz >= size:
                    # Enough bytes already buffered.
                    return self._bd.get(size)
                elif self._bd.byteSz <= size and self.got_eof:
                    # Not enough bytes buffered, but the stream is
                    # over, so return what has been gotten.
                    return self._bd.get_all()
                else:
                    # Not enough bytes buffered and stream is still
                    # open: read more bytes.
                    assert not self.got_eof
                    self._read_chunk()
        else:
            assert False

    def close(self):
        os.close(self._fd)

        self._fd = -1
        del self._bd

    def fileno(self):
        return self._fd

    @property
    def closed(self):
        return self._fd == -1


class NonBlockBufferedWriter(object):
    """A buffered pipe writer that adheres to the Python file protocol"""

    def __init__(self, fd):
        _setup_fd(fd)
        self._fd = fd
        self._bd = ByteDeque()

    def _partial_flush(self, max_retain):
        byts = self._bd.get_all()
        cursor = buffer(byts)

        while len(cursor) > max_retain:
            try:
                n = os.write(self._fd, cursor)
                cursor = buffer(cursor, n)
            except EnvironmentError, e:
                if e.errno in [errno.EAGAIN, errno.EWOULDBLOCK]:
                    gevent.socket.wait_write(self._fd)
                else:
                    raise

        assert self._bd.byteSz == 0
        if len(cursor) > 0:
            self._bd.add(cursor)

    def write(self, data):
        self._bd.add(data)

        flush_until_less_than = PIPE_BUF_BYTES + 1
        while self._bd.byteSz > PIPE_BUF_BYTES:
            self._partial_flush(flush_until_less_than)

    def flush(self):
        while self._bd.byteSz > 0:
            self._partial_flush(0)

    def fileno(self):
        return self._fd

    def close(self):
        os.close(self._fd)

        self._fd = -1
        del self._bd

    @property
    def closed(self):
        return self._fd == -1
