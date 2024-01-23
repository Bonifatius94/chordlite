from chordlite import ResourceKey


def test_can_init_overflow_key():
    assert ResourceKey(10, 9).value == 1


def test_can_init_negative_key():
    assert ResourceKey(-10, 9).value == 8


def test_can_add_with_overflow():
    assert (ResourceKey(10, 128) + ResourceKey(120, 128)).value == 2


def test_can_sub_with_underflow():
    assert (ResourceKey(10, 128) - ResourceKey(11, 128)).value == 127


def test_can_mul_with_overflow():
    assert (ResourceKey(10, 128) * ResourceKey(13, 128)).value == 2


def test_can_compare_eq_neq():
    assert ResourceKey(10, 128) != ResourceKey(13, 128)
    assert ResourceKey(10, 128) != ResourceKey(141, 128)
    assert ResourceKey(10, 128) == ResourceKey(10, 128)
    assert ResourceKey(10, 128) == ResourceKey(138, 128)


def test_can_compare_lt_gt():
    assert ResourceKey(10, 128)  < ResourceKey(13, 128)
    assert ResourceKey(10, 128)  > ResourceKey(128, 128)
    assert ResourceKey(10, 128) <= ResourceKey(10, 128)
    assert ResourceKey(10, 128) <= ResourceKey(11, 128)
    assert ResourceKey(10, 128) >= ResourceKey(10, 128)
    assert ResourceKey(11, 128) >= ResourceKey(10, 128)


def test_can_compare_within_range():
    assert ResourceKey(10, 128) < ResourceKey(13, 128) < ResourceKey(14, 128)
