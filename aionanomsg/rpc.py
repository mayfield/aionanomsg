"""
RPC facilities built on top of core nanomsg functions.

This facility is based on PUB/SUB sockets as a means for maximum flexibility.
Direct 1:1 RPC is of course perfectly fine but the comms can easily extend
to multiple peers without use of a broker. """

import asyncio
import logging
import traceback
import msgpack
from . import socket, pubsub

logger = logging.getLogger('aionanomsg.rpc')


class RemoteException(Exception):
    """ Encapsulates an exception that took place on the remote end of an RPC
    call. """

    def __init__(self, type=None, message=None, traceback=None):
        self.type = type
        self.message = message
        self.traceback = traceback
        super().__init__(message)

    def __str__(self):
        return '<%s %s(%s)>' % (type(self).__name__, self.type, self.message)


class Node(object):
    """ This serves as both caller and listener for RPC. """

    def __init__(self, peers=None, bind_url='tcp://0.0.0.0:1978'):
        self._stopping = False
        self._stopped = asyncio.Event(loop=self._loop)
        self._calls = {}
        self._peers = set(peers or ())

    def _encode(self, value, _dumps=msgpack.packb):
        return _dumps(value, use_bin_type=True)

    def _decode(self, data, encoding='utf-8', _loads=msgpack.unpackb):
        return _loads(data, encoding=encoding, use_list=False)

    async def _send(self, value):
        await super().send(self.encode(value))

    async def _recv(self):
        return self.decode(await super().recv())

    def bind_call(self, channel:str, call):
        """ Associate a callable (function or coro function) with a channel.
        The channel name can be any string to identify how other callers can
        reach the callable.  For 1:1 communication the channel should be
        unique to the entire cluster;  An exercise left to the user. """
        assert channel not in self._calls
        self._calls[name] = call
        if not asyncio.iscoroutinefunction(call):
            assert callable(call), "
                raise 
        self.sub.subscribe(channel, call)

    def unbind_call(self, func_or_name):
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
            with embed_rpc_response(self._calls[call], args, kwargs)
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
                try:
                    await self.send(resp)
                except Exception as e:
                    logger.exception("Unhandled RPC Error")
                    await self.send({
                        "success": False,
                        "exception": {
                            "type": "internal",
                            "message": "rpc error"
                        }
                    })
        self._stopped.set()

    def stop(self):
        self._stopping = True

    async def wait_stopped(self):
        await self._stopped.wait()


class RPCClient(SerializationMixin, socket.NNSocket):

    async def call(self, name, *args, **kwargs):
        await self.send((name, args, kwargs))
        resp = await self.recv()
        if resp['success']:
            return resp['data']
        else:
            raise RemoteException(**resp['exception'])
