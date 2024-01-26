from chordlite import \
    IPEndpointId, ChordRequest, ChordResponse, \
    ChordRequestType, ChordStatus, ResourceKey
from chordlite.http_msg import \
    serialize_request, deserialize_request, \
    serialize_response, deserialize_response


def test_can_transmit_request():
    endpoint = IPEndpointId("10.0.0.1", "5555")
    orig_request = ChordRequest(
        ChordRequestType.FIND_SUCC,
        IPEndpointId("10.0.0.2", "5555"),
        IPEndpointId("10.0.0.3", "5555"),
        IPEndpointId("10.0.0.1", "5555"),
        ResourceKey(1)
    )

    ser_request = serialize_request(orig_request)
    deser_request = deserialize_request(ser_request, endpoint.keyspace)

    assert orig_request == deser_request


def test_can_transmit_response():
    endpoint = IPEndpointId("10.0.0.1", "5555")
    orig_response = ChordResponse(
        IPEndpointId("10.0.0.2", "5555"),
        IPEndpointId("10.0.0.3", "5555"),
        status=ChordStatus.TIMEOUT
    )

    ser_response = serialize_response(orig_response)
    deser_response = deserialize_response(ser_response, endpoint.keyspace)

    assert orig_response == deser_response
