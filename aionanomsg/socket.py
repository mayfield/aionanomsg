"""
aionanomsg socket library.

This is probably what you're looking for.
"""

import asyncio
import warnings
from . import _nanomsg, symbols


class NNSocket(_nanomsg.NNSocket):
    """ Public interface for nanomsg operations. """

    def __init__(self, nn_type, domain=_nanomsg.AF_SP, loop=None):
        if loop is None:
            self._loop = asyncio.get_event_loop()
        self._sending = False
        self._receiving = False
        self._send_waiter = None
        self._recv_waiter = None
        self._eids = []
        try:
            self._create_future = self._loop.create_future
        except AttributeError:
            self._create_future = lambda: asyncio.Future(loop=self._loop)
        super().__init__(domain, nn_type)

    def __del__(self):
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
        assert isinstance(option, symbols.NNSymbol), 'option must be NNSymbol'
        return self._nn_getsockopt(level, option, option.type)

    async def send(self, data):
        assert not self._sending, 'send() is already running'
        self._sending = True
        try:
            self._send(data)
        except BlockingIOError:
            await self._send_on_ready(data)
        finally:
            self._sending = False

    async def recv(self):
        assert not self._receiving, 'recv() is already running'
        self._receiving = True
        try:
            return self._recv()
        except BlockingIOError:
            return await self._recv_on_ready()
        finally:
            self._receiving = False

    def shutdown(self):
        """ Tell nanomsg core to shutdown our connections. """
        while self._eids:
            try:
                self._nn_shutdown(self._eids.pop())
            except Exception as e:
                warnings.warn("NNSocket shutdown error: %s(%s)" % (
                              type(e).__name__, e))

    def close(self):
        self.shutdown()

    def _recvable_event(self):
        assert self._recv_waiter is not None, 'spurious recv event'
        waiter = self._recv_waiter
        self._recv_waiter = None
        try:
            waiter.set_result(self._recv())
        except Exception as e:
            waiter.set_exception(e)

    def _sendable_event(self):
        assert self._send_waiter is not None, 'spurious send event'
        waiter = self._send_waiter
        self._send_waiter = None
        try:
            waiter.set_result(self._send(waiter.data))
        except Exception as e:
            waiter.set_exception(e)

    def _send(self, data, _flags=_nanomsg.NN_DONTWAIT):
        return self._nn_send(data, _flags)

    def _recv(self, _flags=_nanomsg.NN_DONTWAIT):
        return self._nn_recv(_flags)

    def _send_on_ready(self, data):
        """ Wait for socket to be writable before sending. """
        assert self._send_waiter is None or self._send_waiter.cancelled(), \
            'send_waiter already exists'
        self._send_waiter = f = self._create_future()
        f.data = data
        # Even the send fd notifies via read events.
        self._loop.add_reader(self.send_poll_fd, self._sendable_event)
        f.add_done_callback(self._remove_send_handler)
        return f

    def _recv_on_ready(self):
        assert self._recv_waiter is None or self._recv_waiter.cancelled(), \
            'recv_waiter already exists'
        self._recv_waiter = f = self._create_future()
        self._loop.add_reader(self.recv_poll_fd, self._recvable_event)
        f.add_done_callback(self._remove_recv_handler)
        return f

    def _remove_send_handler(self, f):
        self._loop.remove_reader(self.send_poll_fd)

    def _remove_recv_handler(self, f):
        self._loop.remove_reader(self.recv_poll_fd)
