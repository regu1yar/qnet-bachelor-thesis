import asyncio
import typing as tp


class DataHandler:
    def __init__(self) -> None:
        pass

    async def handle(self, data: bytes) -> None:
        pass


class UDPServer(asyncio.DatagramProtocol):
    def __init__(self, handler: DataHandler) -> None:
        self.__handler = handler
        self.__transport: tp.Optional[asyncio.transports.DatagramTransport] = None

    def connection_made(self, transport: asyncio.transports.BaseTransport) -> None:
        self.__transport = tp.cast(asyncio.transports.DatagramTransport, transport)

    def connection_lost(self, exc: tp.Optional[Exception]) -> None:
        print('Server\'s connection closed')

    def datagram_received(self, data: bytes, addr: tp.Tuple[str, int]) -> None:
        loop = asyncio.get_event_loop()
        loop.create_task(self.__handler.handle(data))

    def error_received(self, exc: Exception) -> None:
        print('Error received:', exc)


class UDPClient(asyncio.DatagramProtocol):
    def __init__(self, data: bytes) -> None:
        self.__data = data
        self.__transport: tp.Optional[asyncio.transports.DatagramTransport] = None

    def connection_made(self, transport: asyncio.transports.BaseTransport) -> None:
        self.__transport = tp.cast(asyncio.transports.DatagramTransport, transport)
        self.__transport.sendto(self.__data)
        self.__transport.close()

    def connection_lost(self, exc: tp.Optional[Exception]) -> None:
        print('Client\'s connection closed')

    def datagram_received(self, data: bytes, addr: tp.Tuple[str, int]) -> None:
        print('Data from {} received: {}'.format(addr, str(data)))

    def error_received(self, exc: Exception) -> None:
        print('Error received:', exc)


async def send_data(ip_addr: str, port: int, data: bytes) -> None:
    loop = asyncio.get_running_loop()
    await loop.create_datagram_endpoint(
        lambda: UDPClient(data),
        remote_addr=(ip_addr, port))
