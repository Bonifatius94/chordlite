from chordlite import IPEndpointId


def test_can_create_endpoint():
    key = IPEndpointId("10.0.0.1", "5555")
    assert key is not None


def test_can_use_endpoint_for_key_arithmetics():
    key = IPEndpointId("10.0.0.1", "5555", 64)
    result = key + 24
    assert result.value == 1
