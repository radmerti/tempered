import asyncio
from abc import ABC, abstractmethod

import aiohttp

from .limits import RateLimit


class DownloadManager(ABC):
    def __init__(self, rate_limits: [RateLimit]):
        self.rate_limits = rate_limits

        self._loop = asyncio.get_event_loop()
        self._tasks = asyncio.Queue(loop=self._loop)
            

    def start(self):
        try:
            self._loop.run_until_complete(self._main())
        except Exception as e:
            print(f"error: {e}")
            self._loop.run_until_complete(self._session.close())
            self._loop.close()

    async def _main(self):
        self._session = aiohttp.ClientSession()

        await self._prologue()

        while True:
            task = await self._tasks.get()

            await self._handle_task(task)

        await self._epilogue()

    async def _ensure_rate_limits(self):
        for limit in self.rate_limits:
            await limit()

    async def get_json(self, url: str, headers: dict = None):
        print(f"get_json({url})")

        await self._ensure_rate_limits()

        while True:
            async with self._session.get(url, headers=headers) as response:
                if response.status == 200:
                    return await response.json()
                elif response.status == 429:
                    if 'Retry-After' in response.headers:
                        timeout = float(response.headers['Retry-After'])
                    else:
                        timeout = 10.0
                    print(f"response 429 limited for {timeout} seconds")
                    await asyncio.sleep(timeout)
                elif response.status == 403:
                    raise RuntimeError("access key expired")
                else:
                    print(f"response: {response.status} - {response}")

    @abstractmethod
    async def _prologue(self):
        pass

    @abstractmethod
    async def _epilogue(self):
        pass

    @abstractmethod
    async def _handle_task(self, task):
        pass
