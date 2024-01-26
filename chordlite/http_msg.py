from json import loads, dumps
from typing import Callable, Optional

from chordlite.key import ResourceKey
from chordlite.endpoint import IPEndpointId
from chordlite.node import ChordStatus
from chordlite.transport import \
    ChordRequest, ChordResponse, ChordRequestType


def serialize_endpoint(endpoint: Optional[IPEndpointId]) -> Optional[str]:
    return str(endpoint) if endpoint else None


def deserialize_endpoint(data: str, keyspace: int) -> Optional[IPEndpointId]:
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
        ResourceKey(int(data_dict["requested_resource_id"])),
        new_endpoint(data_dict["new_successor_id"]),
        new_endpoint(data_dict["new_predecessor_id"])
    )


def serialize_request(request: ChordRequest) -> bytes:
    data = {
        "request_type": int(request.request_type),
        "forward_id": serialize_endpoint(request.forward_id),
        "receiver_id": serialize_endpoint(request.receiver_id),
        "requester_id": serialize_endpoint(request.requester_id),
        "requested_resource_id": request.requested_resource_id.value,
        "new_successor_id": serialize_endpoint(request.new_successor_id),
        "new_predecessor_id": serialize_endpoint(request.new_predecessor_id)
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
        "responder_id": serialize_endpoint(response.responder_id),
        "successor_id": serialize_endpoint(response.successor_id),
        "predecessor_id": serialize_endpoint(response.predecessor_id),
        "status": int(response.status)
    }
    return dumps(data).encode("utf-8")


def receive_chord_request(
        process_message: Callable[[ChordRequest], ChordResponse],
        keyspace: int, ser_request: bytes):
    req = deserialize_request(ser_request, keyspace)
    resp = process_message(req)
    resp_ser = serialize_response(resp)
    return resp_ser


def send_chord_request(
        send_request: Callable[[str, bytes], bytes],
        keyspace: int, request: ChordRequest) -> ChordResponse:
    ser_request = serialize_request(request)
    ser_resp = send_request(f"http://{request.forward_id}/chord", ser_request)
    response = deserialize_response(ser_resp, keyspace)
    return response
