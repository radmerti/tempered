from abc import ABC, abstractmethod
import asyncio
import aiohttp
from time import time

from .limits import RateLimit


class RequestManager():
    def __init__(self, headers_and_limits: (dict, (RateLimit,))):

        self._loop = asyncio.get_event_loop()
        self._request_queue = asyncio.PriorityQueue(loop=self._loop)

        for headers, limits in headers_and_limits:
            self._loop.create_task(self._request_handler(headers, limits))

    async def schedule(
            self,
            url: str,
            callback,  # : asyncio.types.CoroutineType,
            priority: int = 0):
        '''Schedule the `url` with the `priority`. The `callback`´
        will be called with `callback(self, result)`. The `result`
        is a `aiohttp.ClientResponse`.'''
        await self._request_queue.put((priority, time(), url, callback))


    async def _request_handler(self, headers: dict, limits: (RateLimit,)):

        async with aiohttp.ClientSession(headers=headers) as session:

            while True:
                # get a new request from the priority queue
                _, _, url, callback = await self._request_queue.get()

                response_body = None
                while response_body is None:
                    async with session.get(url, headers=headers) as response:
                        response_body = await self._handle_response(response)


                # schedule the callback but don't await the reuslt
                self._loop.create_task(callback(response_body))

                # ensure the rate limits by awaiting all of them
                await asyncio.gather(
                    *(limit() for limit in limits),
                    loop=self._loop)

    @staticmethod
    @abstractmethod
    async def _handle_response(response: aiohttp.ClientResponse):
        pass
