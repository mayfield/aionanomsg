"""
aionanomsg pubsub high-level wrapper for NNSocket.

While possible to use aionanomsg.NNSocket directly for pubsub this module
provides a higher level interface for doing pubsub that allows more natural
interaction from a python type program.
"""

import asyncio
from . import socket, symbols


class Subscriber(socket.NNSocket):
    """ An NN_SUB based NNSocket with extra sauce for easy living. """

    def __init__(self, **kwargs):
        self._listeners = {}
        self._stopping = False
        self._router_task = None
        super().__init__(symbols.NN_SUB, **kwargs)
        self._stopped = asyncio.Event(loop=self._loop)

    def subscribe(self, topic:str, listener):
        """ Bind this socket to a topic and hook the listener func/coro to
        that topic so it will run on any published data addressed to it. """
        topic = topic.encode()
        if topic not in self._listeners:
            self.setsockopt(symbols.NN_SUB, symbols.NN_SUB_SUBSCRIBE, topic)
            self._listeners[topic] = []
        self._listeners[topic].append(listener)

    def unsubscribe(self, topic:str, listener):
        topic = topic.encode()
        self._listeners[topic].remove(listener)

    def start(self):
        """ Start handling published data to our topics. """
        assert not self._stopping
        self._router_task = self._loop.create_task(self._router())

    def stop(self):
        if self._stopping:
            return
        self._stopping = True

    async def wait_stopped(self):
        """ Block until the subscriber is completely shutdown. """
        if self._router_task is None:
            return
        await self._stopped.wait()

    async def _router(self):
        """ Route messages to the correct listeners. """
        try:
            while not self._stopping:
                data = await self.recv()
                topic, msg = data.split(b'|', 1)
                listeners = self._listeners[topic]
                for x in listeners:
                    asyncio.ensure_future(x(msg), loop=self._loop)
        finally:
            self._stopped.set()


class Publisher(socket.NNSocket):
    """ An NN_PUB based NNSocket with extra sauce for easy living. """

    def __init__(self, **kwargs):
        super().__init__(symbols.NN_PUB, **kwargs)

    async def publish(self, topic:str, msg:bytes):
        """ Publish a pre-encoded (bytes) message to a topic. """
        await self.send(b'%s|%s' % (topic.encode(), msg))
