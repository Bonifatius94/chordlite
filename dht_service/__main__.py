import os
from time import sleep
from threading import Thread

from requests import post as send_request
from flask import Flask, request as flask_request, Response as HttpResponse

from chordlite import \
    NetworkedChordNode, local_endpoint, NetworkBootstrapper, \
    send_chord_request, receive_chord_request
from dht_service.dht import DHTService


chord_port = os.environ["CHORD_PORT"]
broadcast_port = os.environ["BROADCAST_PORT"]


endpoint = local_endpoint(chord_port)
post_http = lambda url, data: send_request(url, data).raw
make_response = lambda body, status: HttpResponse(body, status)
node = NetworkedChordNode(endpoint, lambda r: send_chord_request(post_http, endpoint.keyspace, r))
dht = DHTService(node, post_http, make_response, dht_port=int(chord_port))
bootstrapper = NetworkBootstrapper(endpoint, broadcast_port=int(broadcast_port))

sleep(10)
boostrap_id = bootstrapper.find_bootstrap()
print("bootstrap is", str(boostrap_id))
sleep(10)

app = Flask(f"{__name__}/chord")

@app.route(rule="/chord", methods=["POST"])
def chord_msg():
    return receive_chord_request(
        node.process_message, node.node_id.keyspace, flask_request.data)

@app.route(rule="/lookup", methods=["POST"])
def lookup():
    return dht.lookup(flask_request.data)

@app.route(rule="/insert", methods=["POST"])
def insert():
    return dht.insert(flask_request.data)

@app.route(rule="/delete", methods=["POST"])
def delete():
    return dht.delete(flask_request.data)


def init_chord():
    sleep(10)
    node.join_network(boostrap_id)
    sleep(10)
    dht.activate()


init_task = Thread(target=init_chord)
init_task.start()
app.run(debug=True, port=int(chord_port))
