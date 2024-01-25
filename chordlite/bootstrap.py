import socket
from json import loads, dumps
from typing import Set
from dataclasses import dataclass, field
from threading import Thread
from time import sleep
from chordlite.endpoint import IPEndpointId


@dataclass
class NetworkBootstrapper:
    endpoint_id: IPEndpointId
    broadcast_port: int
    repetitions: int = 10
    wait_time: int = 2
    all_node_ids: Set[IPEndpointId] = field(default_factory=set)

    def find_bootstrap(self) -> IPEndpointId:
        sender_thread = Thread(target=self.send_nodeid)
        receiver_thread = Thread(target=self.receive_nodeid)
        sender_thread.start()
        receiver_thread.start()
        sender_thread.join()
        receiver_thread.join(5)
        return min(self.all_node_ids, key=lambda n: n.key)

    def send_nodeid(self):
        msg = dumps({ "node_id": str(self.endpoint_id) }).encode("utf-8")
        for i in range(self.repetitions):
            ip = self.endpoint_id.ip_address
            print(f'sending on {ip}')
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
            sock.bind((ip, 0))
            sock.sendto(msg, ("255.255.255.255", self.broadcast_port))
            sock.close()
            sleep(self.wait_time)

    def receive_nodeid(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.bind(('', self.broadcast_port))
        while True:
            msg = sock.recvfrom(1024)[0].decode("utf-8")
            raw_node_id: str = loads(msg)["node_id"]
            ip_addr, port = raw_node_id.split(":")[0], raw_node_id.split(":")[1]
            node_id = IPEndpointId(ip_addr, port)
            self.all_node_ids.add(node_id)
