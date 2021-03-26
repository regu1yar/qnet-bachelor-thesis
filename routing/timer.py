import asyncio
import random


class RandomShiftedTimer:
    def __init__(self, timeout, max_timeout_shift, callback):
        self.__timeout = timeout
        self.__max_timeout_shift = max_timeout_shift
        self.__callback = callback
        self.__task = asyncio.ensure_future(self.__job())
        self.__active = True

    async def __job(self):
        await asyncio.sleep(self.__generate_timeout())
        await self.__callback()

    def __generate_timeout(self):
        return self.__timeout + random.randint(0, self.__max_timeout_shift)

    def cancel(self):
        self.__task.cancel()
        self.__active = False

    def is_active(self):
        return self.__active
