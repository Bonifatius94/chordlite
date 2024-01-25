from json import loads, dumps
from dataclasses import dataclass
from typing import Callable

from chordlite.endpoint import IPEndpointId
from chordlite.node import ChordStatus
from chordlite.transport import \
    ChordRequest, ChordResponse, ChordRequestType, NetworkedChordNode


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


def serialize_request(request: ChordRequest) -> bytes:
    data = {
        "request_type": int(request.request_type),
        "forward_id": str(request.forward_id),
        "receiver_id": str(request.receiver_id),
        "requester_id": str(request.requester_id),
        "requested_resource_id": str(request.requested_resource_id),
        "new_successor_id": str(request.new_successor_id),
        "new_predecessor_id": str(request.new_predecessor_id)
    }
    return dumps(data).encode("utf-8")


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
        "responder_id": str(response.responder_id),
        "successor_id": str(response.successor_id),
        "predecessor_id": str(response.predecessor_id),
        "status": int(response.status)
    }
    return dumps(data).encode("utf-8")


@dataclass
class ChordHttpEndpoint:
    node: NetworkedChordNode
    send_request: Callable[[str, bytes], bytes]

    def receive_chord_request(self, ser_request: bytes):
        req = deserialize_request(ser_request, self.node.node_id.keyspace)
        resp = self.node.process_message(req)
        resp_ser = serialize_response(resp)
        return resp_ser

    def send_chord_request(self, request: ChordRequest) -> ChordResponse:
        ser_request = serialize_request(request)
        ser_resp = self.send_request(f"http://{request.forward_id}/chord", ser_request)
        response = deserialize_response(ser_resp, self.node.node_id.keyspace)
        return response
