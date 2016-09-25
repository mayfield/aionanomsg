
import asyncio
import collections
import errno
import itertools
import logging
import nnpy

logger = logging.getLogger('aionanomsg')


bgrecv = itertools.count()
fgrecv = itertools.count()
bgsend = itertools.count()
bgsend_atonce = itertools.count()
fgsend = itertools.count()


class Socket(nnpy.Socket):

    def __init__(self, nn_type, family=nnpy.AF_SP, recvq_maxsize=1,
                 sendq_maxsize=1, loop=None):
        if loop is None:
            self._loop = asyncio.get_event_loop()
        self._recv_queue = asyncio.Queue(recvq_maxsize, loop=self._loop)
        self._send_queue = collections.deque(maxlen=sendq_maxsize) # asyncio.Queue(sendq_maxsize, loop=self._loop)
        self._has_recv_handler = False
        self._has_send_handler = False
        super().__init__(family, nn_type)
        self._fd = self.getsockopt(nnpy.SOL_SOCKET, nnpy.RCVFD)

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
            self._loop.add_writer(self._fd, self._writable_event)
            self._has_send_handler = True

    def maybe_add_recv_handler(self):
        if not self._has_recv_handler:
            self._loop.add_reader(self._fd, self._readable_event)
            self._has_recv_handler = True

    def remove_send_handler(self):
        self._loop.remove_writer(self._fd)
        self._has_send_handler = False

    async def send(self, data):
        return self.send_nowait(data)

    def send_nowait(self, data):
        try:
            self._send(data)
            next(fgsend)
        except BlockingIOError:
            f = self._create_future()
            self._send_queue.append((f, data))
            self.maybe_add_send_handler()
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
                self.maybe_add_recv_handler()
                data = await q.get()
                q.task_done()
                return data

    def _readable_event(self):
        if self._recv_queue.full():
            self._loop.remove_reader(self._fd)
            self._has_recv_handler = False
            logger.error("QUEUE FULL: removing read handler")
        else:
            logger.error("BG FILL: via read handler")
            next(bgrecv)
            self._recv_queue.put_nowait(self._recv())

    def _writable_event(self):
        i = 0
        while True:
            if i:
                next(bgsend_atonce)
            try:
                f, data = self._send_queue.popleft()
            except IndexError:
                self.remove_send_handler()
                logger.info("Drained to empty!")
                return
            else:
                try:
                    self._send(data)
                except BlockingIOError:
                    self._send_queue.appendleft((f, data))
                    logger.warning("Drained to BLOCK!")
                    return
                else:
                    next(bgsend)
                    f.set_result(None)
            i += 1

    def close(self):
        if self._has_read_handler:
            self._loop.remove_reader(self._fd)
            self._has_read_handler = False
        self.remove_recv_handler()
        self.remove_send_handler()
        super().close()
