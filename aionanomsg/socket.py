"""
AIONanomsg socket library.

This is probably what you're looking for.
"""

import asyncio
import collections
import logging
import warnings
from . import _nanomsg, symbols

logger = logging.getLogger('aionanomsg')


class NNSocket(_nanomsg.NNSocket):

    def __init__(self, nn_type, domain=_nanomsg.AF_SP, recvq_maxsize=1000,
                 sendq_maxsize=1000, loop=None):
        if loop is None:
            self._loop = asyncio.get_event_loop()
        self.recvq_maxsize = recvq_maxsize
        self.sendq_maxsize = sendq_maxsize
        self._recv_queue = collections.deque()
        self._send_queue = collections.deque()
        self._has_recv_handler = False
        self._has_send_handler = False
        self._recv_waiter = None
        self._eids = []
        self._closing = False
        self._closed = False
        try:
            self._create_future = self._loop.create_future
        except AttributeError:
            self._create_future = lambda: asyncio.Future(loop=self._loop)
        super().__init__(domain, nn_type)

    def __del__(self):
        if self._eids:
            warnings.warn("NNSocket not properly closed")
            self.shutdown()

    @property
    def recv_poll_fd(self):
        try:
            return self._recv_poll_fd
        except AttributeError:
            self._recv_poll_fd = self.getsockopt(symbols.NN_SOL_SOCKET,
                                                 symbols.NN_RCVFD)
            return self._recv_poll_fd

    @property
    def send_poll_fd(self):
        try:
            return self._send_poll_fd
        except AttributeError:
            self._send_poll_fd = self.getsockopt(symbols.NN_SOL_SOCKET,
                                                 symbols.NN_SNDFD)
            return self._send_poll_fd

    def bind(self, addr):
        eid = self._nn_bind(addr)
        self._eids.append(eid)

    def connect(self, addr):
        eid = self._nn_connect(addr)
        self._eids.append(eid)

    def getsockopt(self, level, option):
        assert isinstance(level, symbols.NNSymbol), 'level must be NNSymbol'
        assert isinstance(option, symbols.NNSymbol), 'level must be NNSymbol'
        return self._nn_getsockopt(level, option, option.type)

    async def send(self, data):
        try:
            self._send(data)
        except BlockingIOError:
            if len(self._send_queue) >= self.sendq_maxsize:
                raise IOError('send_queue overflow: %d' % self.sendq_maxsize)
            else:
                f = self._create_future()
                self._send_queue.append((f, data))
                self._maybe_add_send_handler()
                await f

    async def recv(self):
        if self._recv_queue:
            return self._recv_queue.popleft()
        else:
            try:
                return self._recv()
            except BlockingIOError:
                f = self._create_future()
                assert not self._recv_waiter, "recv() already called"
                self._recv_waiter = f
                self._add_recv_handler()
                await f
                return f.result()

    def shutdown(self):
        """ Tell nanomsg core to shutdown our connections. """
        while self._eids:
            try:
                self._nn_shutdown(self._eids.pop())
            except Exception as e:
                warnings.warn("NNSocket shutdown error: %s(%s)" % (
                              type(e).__name__, e))

    def close(self):
        self._closing = True
        self._remove_recv_handler()
        self._remove_send_handler()
        self.shutdown()
        self._closed = True

    def _recvable_event(self):
        while True:
            try:
                data = self._recv()
            except BlockingIOError:
                self._remove_recv_handler()
                break
            if self._recv_waiter is not None:
                self._recv_waiter.set_result(data)
                self._recv_waiter = None
            else:
                self._recv_queue.append(data)
                if len(self._recv_queue) >= self.recvq_maxsize:
                    self._remove_recv_handler()
                    break

    def _sendable_event(self):
        while True:
            if not self._send_queue:
                self._remove_send_handler()
                return
            f, data = self._send_queue.popleft()
            try:
                self._send(data)
            except BlockingIOError:
                self._send_queue.appendleft((f, data))
                return
            else:
                f.set_result(None)

    def _send(self, data, _flags=_nanomsg.NN_DONTWAIT):
        return self._nn_send(data, _flags)

    def _recv(self, _flags=_nanomsg.NN_DONTWAIT):
        return self._nn_recv(_flags)

    def _maybe_add_send_handler(self):
        if not self._has_send_handler:
            # Even the send fd signals via read events.
            self._loop.add_reader(self.send_poll_fd, self._sendable_event)
            self._has_send_handler = True

    def _add_recv_handler(self):
        assert not self._has_recv_handler
        self._loop.add_reader(self.recv_poll_fd, self._recvable_event)
        self._has_recv_handler = True

    def _remove_send_handler(self):
        if self._has_send_handler:
            self._loop.remove_writer(self.send_poll_fd)
            self._has_send_handler = False

    def _remove_recv_handler(self):
        if self._has_recv_handler:
            self._loop.remove_reader(self.recv_poll_fd)
            self._has_recv_handler = False
