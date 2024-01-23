from typing import List
from threading import Thread
from chordlite import \
    IPEndpointId, ChordNode, VirtualNetwork, \
    NetworkedChordNode, ChordRemoteEndpoint


def test_can_init_network_over_sync_virtual_network():
    network = VirtualNetwork()
    nodes = [NetworkedChordNode(IPEndpointId(f"10.0.0.{key}", "5555", 1 << 14), network)
             for key in range(128)]
    nodes = sorted(nodes, key=lambda n: n.node_id)
    assert len(set([n.node_id for n in nodes])) == 128

    def exp_fingers_of_node(node: ChordNode) -> List[ChordNode]:
        return [min(nodes, key=lambda f: f.node_id - s).node for s in node.finger_starts]

    for node in nodes:
        network.register_node(node)

    bootstrap_id = min([n.node_id for n in nodes])
    bootstrap_endpoints = [ChordRemoteEndpoint(n.node_id, bootstrap_id, network) for n in nodes]
    for node, bootstrap in zip(nodes, bootstrap_endpoints):
        node.node.initiate_join(bootstrap)
    for node in nodes:
        node.node.update_finger_table()

    exp_succs = nodes[1:] + [nodes[0]]
    exp_preds = [nodes[-1]] + nodes[:-1]
    exp_fingers = [f.node_id for n in nodes for f in exp_fingers_of_node(n.node)]
    assert [n.node.successor.node_id for n in nodes] == [n.node_id for n in exp_succs]
    assert [n.node.predecessor.node_id for n in nodes] == [n.node_id for n in exp_preds]
    assert [f.node_id for n in nodes for f in n.node.fingers] == exp_fingers


def test_can_init_network_over_parallel_virtual_network():
    network = VirtualNetwork()
    nodes = [NetworkedChordNode(IPEndpointId(f"10.0.0.{key}", "5555", 1 << 14), network)
             for key in range(128)]
    nodes = sorted(nodes, key=lambda n: n.node_id)
    assert len(set([n.node_id for n in nodes])) == 128

    def exp_fingers_of_node(node: ChordNode) -> List[ChordNode]:
        return [min(nodes, key=lambda f: f.node_id - s).node for s in node.finger_starts]

    for node in nodes:
        network.register_node(node)

    bootstrap_id = min([n.node_id for n in nodes])
    bootstrap_endpoints = [ChordRemoteEndpoint(n.node_id, bootstrap_id, network) for n in nodes]
    threads = [Thread(target=lambda: n.join_network(b))
               for n, b in zip(nodes, bootstrap_endpoints)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    exp_succs = nodes[1:] + [nodes[0]]
    exp_preds = [nodes[-1]] + nodes[:-1]
    exp_fingers = [f.node_id for n in nodes for f in exp_fingers_of_node(n.node)]
    assert [n.node.successor.node_id for n in nodes] == [n.node_id for n in exp_succs]
    assert [n.node.predecessor.node_id for n in nodes] == [n.node_id for n in exp_preds]
    assert [f.node_id for n in nodes for f in n.node.fingers] == exp_fingers
