import asyncio
import random
import typing as tp


class TimerSyncCallback:
    def __init__(self, timeout: int, callback: tp.Callable[[], None]):
        self.__timeout = timeout
        self.__callback = callback
        self.__task = asyncio.ensure_future(self.__job())

    async def __job(self) -> None:
        await asyncio.sleep(self.__timeout)
        self.__callback()

    def cancel(self) -> None:
        self.__task.cancel()

    def restart(self) -> None:
        self.cancel()
        self.__task = asyncio.ensure_future(self.__job())


class Repeater:
    def __init__(self, timeout: int, callback: tp.Callable[[], tp.Awaitable[None]], run_on_start: bool = False):
        self.__timeout = timeout
        self.__callback = callback
        self.__run_on_start = run_on_start
        self.__task = asyncio.ensure_future(self.__job())

    async def __job(self) -> None:
        if self.__run_on_start:
            await self.__callback()

        while True:
            await asyncio.sleep(self.__timeout)
            await self.__callback()

    def cancel(self) -> None:
        self.__task.cancel()


class RepeaterSyncCallback:
    def __init__(self, timeout: int, callback: tp.Callable[[], None], run_on_start: bool = False):
        self.__timeout = timeout
        self.__callback = callback
        self.__run_on_start = run_on_start
        self.__task = asyncio.ensure_future(self.__job())

    async def __job(self) -> None:
        if self.__run_on_start:
            self.__callback()

        while True:
            await asyncio.sleep(self.__timeout)
            self.__callback()

    def cancel(self) -> None:
        self.__task.cancel()


class RandomShiftedRepeater:
    def __init__(self, timeout: int, min_timeout_shift: int, max_timeout_shift: int,
                 callback: tp.Callable[[], tp.Awaitable[None]], run_on_start: bool = False):
        self.__timeout = timeout
        self.__min_timeout_shift = min_timeout_shift
        self.__max_timeout_shift = max_timeout_shift
        self.__callback = callback
        self.__run_on_start = run_on_start
        self.__task = asyncio.ensure_future(self.__job())

    async def __job(self) -> None:
        if self.__run_on_start:
            await self.__callback()

        while True:
            await asyncio.sleep(self.__generate_timeout())
            await self.__callback()

    def __generate_timeout(self) -> int:
        return self.__timeout + random.randint(self.__min_timeout_shift, self.__max_timeout_shift)

    def cancel(self) -> None:
        self.__task.cancel()
