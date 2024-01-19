from chordlite import ChordKey, ChordNode


def test_can_init_two_node_network():
    n1 = ChordNode(ChordKey(0, 1024))
    n2 = ChordNode(ChordKey(100, 1024))

    bootstrap = n1
    n1.initiate_join(bootstrap)
    n2.initiate_join(bootstrap)

    assert n1.successor == n2 and n1.predecessor == n2
    assert n2.successor == n1 and n2.predecessor == n1
    assert all([f == n2 for f in n1.fingers])
    assert all([f == n1 for f in n2.fingers])


def test_can_init_three_node_network():
    n1 = ChordNode(ChordKey(0, 1024))
    n2 = ChordNode(ChordKey(100, 1024))
    n3 = ChordNode(ChordKey(200, 1024))

    bootstrap = n1
    n1.initiate_join(bootstrap)
    n2.initiate_join(bootstrap)
    n3.initiate_join(bootstrap)

    assert n1.successor == n2
    assert n2.successor == n3
    assert n3.successor == n1
    assert n1.predecessor == n3
    assert n2.predecessor == n1
    assert n3.predecessor == n2


def test_can_init_arbitrary_large_network():
    nodes = [ChordNode(ChordKey(k, 1024)) for k in range(0, 1024, 8)]

    bootstrap = min(nodes, key=lambda n: n.node_id)
    for node in nodes:
        node.initiate_join(bootstrap)

    exp_succs = nodes[1:] + [nodes[0]]
    exp_preds = [nodes[-1]] + nodes[:-1]
    assert [n.successor.node_id for n in nodes] == [n.node_id for n in exp_succs]
    assert [n.predecessor.node_id for n in nodes] == [n.node_id for n in exp_preds]
