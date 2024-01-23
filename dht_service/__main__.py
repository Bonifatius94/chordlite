import socket
from typing import Callable, Dict
from dataclasses import dataclass, field
from json import loads, dumps
from time import sleep
from threading import Thread

from requests import post as send_request
from flask import Flask, request as flask_request, Response as HttpResponse

from chordlite import \
    NetworkedChordNode, IPEndpointId, ChordStatus, \
    ChordRequest, ChordResponse, ChordRequestType, \
    ChordRemoteEndpoint, ChordKey


def local_ip() -> str:
    hostname = socket.gethostname()
    return socket.gethostbyname(hostname)


def local_endpoint_factory() -> IPEndpointId:
    ip_address = local_ip()
    return IPEndpointId(ip_address, "5555")


def deserialize_endpoint(data: str, keyspace: int) -> IPEndpointId:
    if data:
        parts = data.split(":")
        return IPEndpointId(parts[0], parts[1], keyspace)
    else:
        return None


def deserialize_request(data: bytes, keyspace: int) -> ChordRequest:
    data_dict = loads(data)
    new_endpoint = lambda d: deserialize_endpoint(d, keyspace)

    return ChordRequest(
        ChordRequestType(data_dict["request_type"]),
        new_endpoint(data_dict["forward_id"]),
        new_endpoint(data_dict["receiver_id"]),
        new_endpoint(data_dict["requester_id"]),
        new_endpoint(data_dict["requested_resource_id"]),
        new_endpoint(data_dict["new_successor_id"]),
        new_endpoint(data_dict["new_predecessor_id"])
    )


def serialize_request(request: ChordRequest) -> dict:
    return {
        "request_type": request.request_type,
        "forward_id": request.forward_id,
        "receiver_id": request.receiver_id,
        "requester_id": request.requester_id,
        "requested_resource_id": request.requested_resource_id,
        "new_successor_id": request.new_successor_id,
        "new_predecessor_id": request.new_predecessor_id
    }


def deserialize_response(data: bytes, keyspace: int) -> ChordResponse:
    data_dict = loads(data)
    new_endpoint = lambda d: deserialize_endpoint(d, keyspace)

    return ChordResponse(
        new_endpoint(data_dict["responder_id"]),
        new_endpoint(data_dict["successor_id"]),
        new_endpoint(data_dict["predecessor_id"]),
        ChordStatus(data_dict["status"])
    )


def serialize_response(response: ChordResponse) -> bytes:
    data = {
        "responder_id": response.responder_id,
        "successor_id": response.successor_id,
        "predecessor_id": response.predecessor_id,
        "status": int(response.status)
    }
    return dumps(data).encode("utf-8")


@dataclass
class NetworkBootstrapper:
    endpoint_id: IPEndpointId
    repetitions: int=10
    min_id: IPEndpointId = field(init=False)
    listen: bool = False

    def __post_init__(self):
        self.min_id = self.endpoint_id

    def find_bootstrap(self) -> IPEndpointId:
        self.listen = True
        sender_thread = Thread(target=self.send_nodeid)
        receiver_thread = Thread(target=self.send_nodeid)

        sender_thread.start()
        receiver_thread.start()
        sender_thread.join()
        self.listen = False
        receiver_thread.join()

        return self.min_id

    def send_nodeid(self):
        interfaces = socket.getaddrinfo(
            host=socket.gethostname(), port=None, family=socket.AF_INET)
        allips = [ip[-1][0] for ip in interfaces]
        msg = dumps({ "node_id": self.endpoint_id }).encode("utf-8")
        for i in range(self.repetitions):
            for ip in allips:
                print(f'sending on {ip}')
                sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
                sock.bind((ip, 0))
                sock.sendto(msg, ("255.255.255.255", 5555))
                sock.close()
            sleep(2)

    def receive_nodeid(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.bind(('', 5555))
        remainder = ""
        while self.listen:
            msg = sock.recvfrom(1024)[0].decode("utf-8")
            offset = msg.index("}") + 1 if "}" in msg else -1
            remainder += msg[:offset]
            if "}" in remainder:
                raw_node_id: str = loads(remainder)["node_id"]
                ip_addr, port = raw_node_id.split(":")[0], raw_node_id.split(":")[1]
                node_id = IPEndpointId(ip_addr, port)
                self.min_id = min(self.min_id, node_id)
                remainder = msg[offset:]


@dataclass
class DHTService:
    node: NetworkedChordNode
    local_data: Dict[int, str] = field(default_factory=dict)
    is_active: bool = field(init=False, default=False)

    def activate(self):
        self.is_active = True

    def lookup(self):
        if not self.is_active:
            return HttpResponse(status=400)

        data_dict = loads(flask_request.data)
        key = ChordKey(data_dict["resource_id"], 1 << 256)
        endpoint = self.node.lookup(key)

        response: str
        if endpoint.node_id != self.node.node_id:
            response = send_request(
                f"http://{endpoint.node_id}/lookup",
                data=flask_request.data).raw
        else:
            response = dumps({
                "resource_id": key,
                "value": self.local_data[key.value]
            })
        return response

    def insert(self):
        if not self.is_active:
            return HttpResponse(status=400)

        data_dict = loads(flask_request.data)
        key = ChordKey(data_dict["resource_id"], 1 << 256)
        endpoint = self.node.lookup(key)

        response: str
        if endpoint.node_id != self.node.node_id:
            response = send_request(
                f"http://{endpoint.node_id}/insert",
                data=flask_request.data).raw
        else:
            response = flask_request.data
            self.local_data[data_dict["resource_id"]] = data_dict["value"]
        return response

    def delete(self):
        if not self.is_active:
            return HttpResponse(status=400)

        data_dict = loads(flask_request.data)
        key = ChordKey(data_dict["resource_id"], 1 << 256)
        endpoint = self.node.lookup(key)

        response: str
        if endpoint.node_id != self.node.node_id:
            response = send_request(
                f"http://{endpoint.node_id}/delete",
                data=flask_request.data).raw
        else:
            response = flask_request.data
            self.local_data.pop(data_dict["resource_id"], None)
        return response


@dataclass
class ChordHttpNode:
    node: NetworkedChordNode = field(init=False)
    find_local: Callable[[], IPEndpointId] \
        = field(default=local_endpoint_factory)

    @property
    def node_id(self) -> IPEndpointId:
        return self.node.node_id

    def launch(self):
        local_endpoint = self.find_local()
        self.node = NetworkedChordNode(
            local_endpoint, self.send_chord_request)
        dht = DHTService(self.node)

        app = Flask(__name__)
        app.route("/chord", methods=["POST"])(self.receive_chord_request)
        app.route("/lookup", methods=["POST"])(dht.lookup)
        app.route("/insert", methods=["POST"])(dht.insert)
        app.route("/delete", methods=["POST"])(dht.delete)
        app.run(debug=True, port=int(local_endpoint.port))

        sleep(10)
        bootstrapper = NetworkBootstrapper(local_endpoint)
        boostrap_id = bootstrapper.find_bootstrap()
        bootstrap = ChordRemoteEndpoint(self.node_id, boostrap_id, self.node.network)
        sleep(10)
        self.node.join_network(bootstrap)
        sleep(10)
        dht.activate()

    def receive_chord_request(self):
        ser_request = flask_request.data
        req = deserialize_request(ser_request, self.node_id.keyspace)
        resp = self.node.process_message(req)
        resp_ser = serialize_response(resp)
        return resp_ser

    def send_chord_request(self, request: ChordRequest) -> ChordResponse:
        ser_request = serialize_request(request)
        ser_resp = send_request(f"http://{request.forward_id}/chord", data=ser_request)
        response = deserialize_response(ser_resp.raw, self.node_id.keyspace)
        return response


def main():
    node = ChordHttpNode()
    node.launch()


if __name__ == "__main__":
    main()
