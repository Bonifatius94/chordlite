from chordlite import IPEndpointId, ChordKey


def test_can_create_endpoint():
    key = IPEndpointId("10.0.0.1", "5555")
    assert key is not None


def test_can_use_endpoint_for_key_arithmetics():
    key = IPEndpointId("10.0.0.1", "5555")
    result = key + 10
    assert type(result) == ChordKey
