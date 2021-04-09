import typing as tp


class Config:
    def __init__(self) -> None:
        pass

    def get_ip_addr_by_node_id(self, node_id: int) -> str:
        pass

    def get_app_port(self) -> int:
        pass
