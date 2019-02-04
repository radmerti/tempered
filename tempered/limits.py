import asyncio
from abc import ABC, abstractmethod
from random import randint, uniform
from time import time


class RateLimitException(Exception):
    def __init__(self, message):
        super().__init__(message)


class RateLimit(ABC):
    @abstractmethod
    def reset(self):
        pass

    @abstractmethod
    async def __call__(self):
        pass


class AbsoluteRateLimit(RateLimit):
    def __init__(self, count: int):
        self.count = count
        self._count = 0

    def reset(self):
        self._count = 0

    async def __call__(self):
        if self._count >= self.count:
            raise RateLimitException("absolute rate limit reached")

        self._count += 1


class DurationRateLimit(RateLimit):
    def __init__(self, duration: float, count: int):
        self.duration = duration
        self.count = count

        self._time = time()
        self._count = 0

    def reset(self, current_time: float):
        self._time = current_time
        self._count = 0

    async def __call__(self):
        current_time = time()

        if (current_time-self._time) > self.duration:
            self.reset(current_time)

        if self._count >= self.count:
            current_duration = (current_time-self._time)
            # print(f"{self} limited for {self.duration-current_duration} seconds")
            await asyncio.sleep(self.duration-current_duration)
            self.reset(time())

        self._count += 1

    def __repr__(self):
        return f"{type(self).__name__}({self.duration}, {self.count})"


class RandomizedDurationRateLimit(RateLimit):
    def __init__(self, duration: float, count: int):
        self.duration = duration
        self.count = count

        self._time = time()
        self._count = 0
        self._request_times = self._new_request_times(self._time)

        # print(f"{self} initialized at {self._time} and {len(self._request_times)} request_times")

    def _new_request_times(self, timestamp: float) -> [float]:
        rand_fewer_elements = 0  # randint(0, self.count/10)
        n_requests = self.count-rand_fewer_elements
        request_times = [
            timestamp+uniform(0.0, self.duration)
            for _ in range(n_requests)]
        request_times.sort()
        return request_times

    def reset(self, current_time: float):
        self._time = current_time
        self._count = 0
        self._request_times = self._new_request_times(current_time)

        # print(f"{self} resetting at {self._time} and {len(self._request_times)} request_times")

    async def __call__(self):
        current_time = time()

        if self._count >= len(self._request_times):
            # Wait for reset if the limit is reached, then reset.
            current_duration = (current_time-self._time)
            # print(f"{self} limited for {self.duration-current_duration} seconds")
            await asyncio.sleep(self.duration-current_duration)
            self.reset(time())
            # Reset the current time as we waited.
            current_time = time()
        else:
            # If the limit is not reached check if the duration is
            # reached and we can reset the count.
            if (current_time-self._time) > self.duration:
                # print(f"{self} resetting because duration exceeded")
                self.reset(current_time)

        next_request_time = self._request_times[self._count]
        self._count += 1

        if next_request_time > current_time:
            # print(f"{self} waiting {next_request_time-current_time} seconds for next request")
            await asyncio.sleep(next_request_time-current_time)

    def __repr__(self):
        return f"{type(self).__name__}({self.duration}, {self.count})"
