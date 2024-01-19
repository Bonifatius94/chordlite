from chordlite.key import ChordKey


def test_can_init_overflow_key():
    assert ChordKey(10, 9).value == 1


def test_can_init_negative_key():
    assert ChordKey(-10, 9).value == 8


def test_can_add_with_overflow():
    assert (ChordKey(10, 128) + ChordKey(120, 128)).value == 2


def test_can_sub_with_underflow():
    assert (ChordKey(10, 128) - ChordKey(11, 128)).value == 127


def test_can_mul_with_overflow():
    assert (ChordKey(10, 128) * ChordKey(13, 128)).value == 2


def test_can_compare_eq_neq():
    assert ChordKey(10, 128) != ChordKey(13, 128)
    assert ChordKey(10, 128) != ChordKey(141, 128)
    assert ChordKey(10, 128) == ChordKey(10, 128)
    assert ChordKey(10, 128) == ChordKey(138, 128)


def test_can_compare_lt_gt():
    assert ChordKey(10, 128) < ChordKey(13, 128)
    assert ChordKey(10, 128) > ChordKey(128, 128)
    assert ChordKey(10, 128) <= ChordKey(10, 128)
    assert ChordKey(10, 128) <= ChordKey(11, 128)
    assert ChordKey(10, 128) >= ChordKey(10, 128)
    assert ChordKey(11, 128) >= ChordKey(10, 128)


def test_can_compare_within_range():
    assert ChordKey(10, 128) < ChordKey(13, 128) < ChordKey(14, 128)
