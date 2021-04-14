import typing as tp


class Config:
    def __init__(self) -> None:
        pass

    def get_ip_addr_by_node_id(self, node_id: int) -> str:
        pass

    def get_groups_to_nodes_dict(self) -> tp.Dict[int, tp.Set[int]]:
        pass

    def get_local_node_id(self) -> int:
        pass

    def get_local_group_id(self) -> int:
        pass
