"""
RPC facilities built on top of core nanomsg functions.
"""

import asyncio
import logging
import traceback
import msgpack
from . import socket

logger = logging.getLogger('aionanomsg.rpc')


class RPCSocket(socket.NNSocket):

    def encode(self, value, _dumps=msgpack.packb):
        return _dumps(value, use_bin_type=True)

    def decode(self, data, encoding='utf-8', _loads=msgpack.unpackb):
        return _loads(data, encoding=encoding)

    async def send(self, value):
        await super().send(self.encode(value))

    async def recv(self):
        return self.decode(await super().recv())


class RemoteException(Exception):

    def __init__(self, type=None, message=None, traceback=None):
        self.type = type
        self.message = message
        self.traceback = traceback
        super().__init__(message)

    def __str__(self):
        return '<%s %s(%s)>' % (type(self).__name__, self.type, self.message)


class RPCServer(RPCSocket):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._stopping = False
        self._stopped = asyncio.Event(loop=self._loop)
        self._calls = {}

    def add_call(self, coro_func, name=None):
        if name is None:
            name = coro_func.__name__
        assert name not in self._calls
        self._calls[name] = coro_func

    def remove_call(self, func_or_name):
        for name, func in self._calls.items():
            if func_or_name in (name, func):
                del self._calls[name]
                break
        else:
            raise ValueError('call not found: %s' % func_or_name)

    async def start(self):
        while not self._stopping:
            try:
                call, args, kwargs = await asyncio.wait_for(self.recv(), 0.100,
                                                            loop=self._loop)
            except asyncio.TimeoutError:
                continue
            resp = {
                "success": True,
                "data": None
            }
            try:
                coro = self._calls[call]
                resp['data'] = await coro(*args, **kwargs)
            except Exception as e:
                tb = traceback.format_exc()
                resp.update({
                    "success": False,
                    "exception": {
                        "type": type(e).__name__,
                        "message": str(e),
                        "traceback": tb
                    }
                })
            finally:
                await self.send(resp)
        self._stopped.set()

    def stop(self):
        self._stopping = True

    async def wait_stopped(self):
        await self._stopped.wait()


class RPCClient(RPCSocket):

    async def call(self, name, *args, **kwargs):
        await self.send((name, args, kwargs))
        resp = await self.recv()
        if resp['success']:
            return resp['data']
        else:
            raise RemoteException(**resp['exception'])
