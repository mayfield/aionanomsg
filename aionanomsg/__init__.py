
import asyncio
import collections
import errno
import itertools
import logging
import nnpy

logger = logging.getLogger('aionanomsg')


fgrecv = itertools.count()
bgrecv = itertools.count()
bgrecv_atonce = itertools.count()
fgsend = itertools.count()
bgsend = itertools.count()
bgsend_atonce = itertools.count()


class Socket(nnpy.Socket):

    def __init__(self, nn_type, family=nnpy.AF_SP, recvq_maxsize=500,
                 sendq_maxsize=500, loop=None):
        if loop is None:
            self._loop = asyncio.get_event_loop()
        self._recv_queue = asyncio.Queue(recvq_maxsize, loop=self._loop)
        self._send_queue = collections.deque(maxlen=sendq_maxsize) # asyncio.Queue(sendq_maxsize, loop=self._loop)
        self._has_recv_handler = False
        self._has_send_handler = False
        self._recv_fd = None
        self._send_fd = None
        super().__init__(family, nn_type)

    def _create_future(self):
        try:
            return self._loop.create_future()
        except AttributeError:
            return asyncio.Future(loop=self._loop)

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

    def maybe_add_recv_handler(self):
        if not self._has_recv_handler:
            if self._recv_fd is None:
                self._recv_fd = self.getsockopt(nnpy.SOL_SOCKET, nnpy.RCVFD)
            self._loop.add_reader(self._recv_fd, self._recvable_event)
            self._has_recv_handler = True
        else:
            logger.warning("look it's already registered!")

    def remove_send_handler(self):
        if self._has_send_handler:
            self._loop.remove_writer(self._send_fd)
            self._has_send_handler = False

    def remove_recv_handler(self):
        if self._has_recv_handler:
            self._loop.remove_reader(self._recv_fd)
            self._has_recv_handler = False

    def send(self, data):
        f = self._create_future()
        try:
            self._send(data)
        except BlockingIOError:
            f = self._create_future()
            self._send_queue.append((f, data))
            self.maybe_add_send_handler()
        else:
            next(fgsend)
            f.set_result(None)
        return f

    async def recv(self):
        q = self._recv_queue
        if not q.empty():
            return q.get_nowait()
        else:
            try:
                data = self._recv()
                next(fgrecv)
                return data
            except BlockingIOError:
                logger.warning("RECV BLOCKED, wait!")
                self.maybe_add_recv_handler()
                data = await q.get()
                q.task_done()
                return data

    def _recvable_event(self):
        i = 0
        while True:
            if i:
                next(bgrecv_atonce)
            try:
                self._recv_queue.put_nowait(self._recv())
            except BlockingIOError:
                logger.warning("RECV DRAINED to BLOCK: %d" % i)
                self.remove_recv_handler()
                break
            except asyncio.QueueFull:
                logger.error("QUEUE FULL: removing recv handler")
                self.remove_recv_handler()
                break
            else:
                logger.error("BG FILL: via recv handler")
                next(bgrecv)

    def _sendable_event(self):
        i = 0
        while True:
            if i:
                next(bgsend_atonce)
            try:
                f, data = self._send_queue.popleft()
            except IndexError:
                self.remove_send_handler()
                logger.info("Sender Drained to empty!")
                return
            else:
                try:
                    self._send(data)
                except BlockingIOError:
                    self._send_queue.appendleft((f, data))
                    logger.warning("Sender Drained to BLOCK!")
                    return
                else:
                    next(bgsend)
                    f.set_result(None)
            i += 1

    def close(self):
        self.remove_recv_handler()
        self.remove_send_handler()
        super().close()
