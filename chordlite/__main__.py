from chordlite.key import ChordKey
from chordlite.node import ChordNode


def main():
    keyspace = 1024
    keys_per_node = 8
    nodes = [ChordNode(ChordKey(node_id, keyspace))
             for node_id in range(0, keyspace, keys_per_node)]

    bootstrap = nodes[0]
    for node in nodes:
        node.initiate_join(bootstrap)
        print([f.node_id for f in node.fingers])

    for i in range(10):
        for node in nodes:
            node.update_others()
        for node in nodes:
            print([f.node_id for f in node.fingers])


if __name__ == "__main__":
    main()
