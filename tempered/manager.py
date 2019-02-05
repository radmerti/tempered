from abc import ABC, abstractmethod
import asyncio
import aiohttp
from time import time

from .limits import RateLimit

class KeyExpiredError(Exception):
    pass

class InternalServerError(Exception):
    pass


class RequestManager():
    def __init__(self, headers_and_limits: (dict, (RateLimit,)), max_requests_inflight: int = 0):

        self.max_requests_inflight = max_requests_inflight

        self._loop = asyncio.get_event_loop()
        self._request_queue = asyncio.PriorityQueue(
            maxsize=max_requests_inflight,
            loop=self._loop)

        self.tasks = tuple(
            self._request_handler(headers, limits)
            for headers, limits in headers_and_limits
        )

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
                try:
                    _, _, url, callback = await self._request_queue.get()
                except TypeError:
                    print(f"type error in _request_queue with items {self._request_queue._queue}")
                    with open("_request_queue_queue.pickle", 'w') as crash_file:
                        from pickle import dump
                        dump(self._request_queue._queue, crash_file)
                    raise

                response_body = None
                while response_body is None:
                    try:
                        async with session.get(url, headers=headers) as response:
                            response_body = await self._handle_response(response)
                    except aiohttp.ClientOSError as os_error:
                        print('ClientOSError - trying again in a few seconds')
                        await asyncio.sleep(10.0)

                # schedule the callback but don't await the reuslt
                self._loop.create_task(callback(response_body))

                # tell the priority queue that the task is finished
                self._request_queue.task_done()

                # ensure the rate limits by awaiting all of them
                await asyncio.gather(
                    *(limit() for limit in limits),
                    loop=self._loop)

    @staticmethod
    @abstractmethod
    async def _handle_response(response: aiohttp.ClientResponse):
        pass
