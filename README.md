
# ChordLite

## About
This repository outlines an implementation of the virtual peer-to-peer
Chord overlay network for distributed resource management in cloud workloads.

The chord protocol provides distributed resource lookups within O(log n) time
for a network with n nodes which allows for great scalability. By its peer-to-peer
nature, all nodes equally serve routing and payload tasks such that no master node
is required, eliminating the common issue of a single-point-of-failure in cloud
workloads. Additional replication techniques and self-healing procedures allow
for robust services with high uptime.

Popular cloud databases such as Apache Cassandra follow similar approaches which
makes Chord an interesting approach to high uptime cloud workloads.
In the particular example of this project, a distributed hash table (DHT)
is served via Docker Swarm simulating a cloud workload.

## Project Setup

```sh
git clone https://github.com/Bonifatius94/chordlite
cd chordlite
```

```sh
python -m pip install -r requirements.txt
```

## Run Unit Tests + Linter

```sh
python -m pytest tests
```

```sh
python -m pylint chordlite
```

## Launch P2P Cluster Serving a DHT

```sh
sudo apt-get update && sudo apt-get install -y docker.io docker-compose
sudo usermod -aG docker $USER && reboot
```

```sh
docker-compose -f dht-compose.yml up --build
```
