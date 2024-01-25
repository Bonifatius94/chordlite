from dataclasses import dataclass, field
from typing import Dict, Callable, Any
from json import loads, dumps
from chordlite import NetworkedChordNode, ResourceKey


@dataclass
class DHTService:
    # TODO: store data to file
    node: NetworkedChordNode
    send_request: Callable[[str, bytes], bytes]
    make_response: Callable[[bytes, int], Any]
    dht_port: int = 5556
    local_data: Dict[int, str] = field(default_factory=dict)
    is_active: bool = field(init=False, default=False)

    def activate(self):
        self.is_active = True

    def lookup(self, request: bytes):
        if not self.is_active:
            return self.make_response("Bad request!".encode("utf-8"), 400)

        data_dict = loads(request)
        key = ResourceKey(data_dict["resource_id"])
        endpoint_id = self.node.lookup(key)

        response: bytes
        if endpoint_id != self.node.node_id:
            response = self.send_request(
                f"http://{endpoint_id.ip_address}:{self.dht_port}/lookup",
                request)
        else:
            response = dumps({
                "resource_id": key,
                "value": self.local_data[key.value]
            }).encode("utf-8")
        return response

    def insert(self, request: bytes):
        if not self.is_active:
            return self.make_response("Bad request!".encode("utf-8"), 400)

        data_dict = loads(request)
        key = ResourceKey(data_dict["resource_id"])
        endpoint_id = self.node.lookup(key)

        response: bytes
        if endpoint_id != self.node.node_id:
            response = self.send_request(
                f"http://{endpoint_id.ip_address}:{self.dht_port}/insert",
                request)
        else:
            response = request
            self.local_data[data_dict["resource_id"]] = data_dict["value"]
        return response

    def delete(self, request: bytes):
        if not self.is_active:
            return self.make_response("Bad request!".encode("utf-8"), 400)

        data_dict = loads(request)
        key = ResourceKey(data_dict["resource_id"])
        endpoint_id = self.node.lookup(key)

        response: bytes
        if endpoint_id != self.node.node_id:
            response = self.send_request(
                f"http://{endpoint_id.ip_address}:{self.dht_port}/delete",
                request)
        else:
            response = request
            self.local_data.pop(data_dict["resource_id"], None)
        return response
