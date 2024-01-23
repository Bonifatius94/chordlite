from typing import List
from time import sleep
from threading import Thread
from chordlite import ResourceKey, ChordNode


def test_can_init_network_in_parallel():
    nodes = [ChordNode(ResourceKey(k, 1024)) for k in range(0, 1024, 8)]

    def exp_fingers_of_node(node: ChordNode) -> List[ChordNode]:
        return [min(nodes, key=lambda f: f.node_id - s) for s in node.finger_starts]

    def join_worker(node: ChordNode, bootstrap: ChordNode):
        node.initiate_join(bootstrap)
        sleep(5)
        node.update_finger_table()

    bootstrap = min(nodes, key=lambda n: n.node_id)
    threads = [Thread(target=join_worker, args=(n, bootstrap)) for n in nodes]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    exp_succs = nodes[1:] + [nodes[0]]
    exp_preds = [nodes[-1]] + nodes[:-1]
    exp_fingers = [f.node_id for n in nodes for f in exp_fingers_of_node(n)]
    assert [n.successor.node_id for n in nodes] == [n.node_id for n in exp_succs]
    assert [n.predecessor.node_id for n in nodes] == [n.node_id for n in exp_preds]
    assert [f.node_id for n in nodes for f in n.fingers] == exp_fingers
