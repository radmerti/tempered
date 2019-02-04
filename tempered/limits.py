import asyncio
from abc import ABC, abstractmethod
from random import randint, uniform
from time import time


class RateLimit(ABC):
    @abstractmethod
    async def __call__(self):
        pass


class RandomizedDurationRateLimit(RateLimit):
    def __init__(self, duration: float, count: int):
        self.duration = duration
        self.count = count

        self._start = time()
        self._count = 0

    async def __call__(self):
        '''Set the start timestamp to the previous timestamp plus
        the duration until the current time is smaller than the new
        start timestamp plus the duration.
        '''
        if self._count >= self.count:
            await asyncio.sleep(self._start+self.duration-time())  # +0.1 (?)

        # Advance to the current rate limited block
        current_time = time()
        while current_time > self._start+self.duration:
            self._start += self.duration
            self._count = 0


        seconds_per_request = (
            (self._start+self.duration-current_time)/(self.count-self._count)
        )
        await asyncio.sleep(seconds_per_request)

        self._count += 1

    def __repr__(self):
        return f"{type(self).__name__}({self.duration}, {self.count})"
