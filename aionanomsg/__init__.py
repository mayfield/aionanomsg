
import asyncio
import collections
import errno
import logging
import nnpy

logger = logging.getLogger('aionanomsg')


class Socket(nnpy.Socket):

    def __init__(self, nn_type, family=nnpy.AF_SP, recvq_maxsize=1000,
                 sendq_maxsize=1000, loop=None):
        if loop is None:
            self._loop = asyncio.get_event_loop()
        self.recvq_maxsize = recvq_maxsize
        self.sendq_maxsize = sendq_maxsize
        self._recv_queue = collections.deque()
        self._send_queue = collections.deque()
        self._has_recv_handler = False
        self._has_send_handler = False
        self._recv_fd = None
        self._send_fd = None
        self._recv_waiter = None
        try:
            self._create_future = self._loop.create_future
        except AttributeError:
            self._create_future = lambda: asyncio.Future(loop=self._loop)
        super().__init__(family, nn_type)

    def _send(self, data):
        try:
            return super().send(data, flags=nnpy.DONTWAIT)
        except nnpy.NNError as e:
            if e.error_no == errno.EAGAIN:
                raise BlockingIOError()
            else:
                raise

    def _recv(self):
        try:
            return super().recv(flags=nnpy.DONTWAIT)
        except nnpy.NNError as e:
            if e.error_no == errno.EAGAIN:
                raise BlockingIOError()
            else:
                raise

    def maybe_add_send_handler(self):
        if not self._has_send_handler:
            if self._send_fd is None:
                self._send_fd = self.getsockopt(nnpy.SOL_SOCKET, nnpy.SNDFD)
            # Even the send fd signals via read events.
            self._loop.add_reader(self._send_fd, self._sendable_event)
            self._has_send_handler = True

    def add_recv_handler(self):
        assert not self._has_recv_handler
        if self._recv_fd is None:
            self._recv_fd = self.getsockopt(nnpy.SOL_SOCKET, nnpy.RCVFD)
        self._loop.add_reader(self._recv_fd, self._recvable_event)
        self._has_recv_handler = True

    def remove_send_handler(self):
        if self._has_send_handler:
            self._loop.remove_writer(self._send_fd)
            self._has_send_handler = False

    def remove_recv_handler(self):
        if self._has_recv_handler:
            self._loop.remove_reader(self._recv_fd)
            self._has_recv_handler = False

    async def send(self, data):
        try:
            self._send(data)
        except BlockingIOError:
            if len(self._send_queue) >= self.sendq_maxsize:
                raise IOError('send_queue overflow: %d' % self.sendq_maxsize)
            else:
                f = self._create_future()
                self._send_queue.append((f, data))
                self.maybe_add_send_handler()
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
                self.add_recv_handler()
                await f
                return f.result()

    def _recvable_event(self):
        while True:
            try:
                data = self._recv()
            except BlockingIOError:
                self.remove_recv_handler()
                break
            if self._recv_waiter is not None:
                self._recv_waiter.set_result(data)
                self._recv_waiter = None
            else:
                self._recv_queue.append(data)
                if len(self._recv_queue) >= self.recvq_maxsize:
                    self.remove_recv_handler()
                    break

    def _sendable_event(self):
        while True:
            if not self._send_queue:
                self.remove_send_handler()
                return
            f, data = self._send_queue.popleft()
            try:
                self._send(data)
            except BlockingIOError:
                self._send_queue.appendleft((f, data))
                return
            else:
                f.set_result(None)

    def close(self):
        self.remove_recv_handler()
        self.remove_send_handler()
        super().close()
