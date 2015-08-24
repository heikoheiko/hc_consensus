# Copyright (c) 2015 Heiko Hees
import simpy
from base import Message, EnvironmentBase, mk_genesis, LockSet
from hc_consensus import ConsensusProtocol

class Network(object):

    def deliver(self, sender, receiver, message):
        bw = min(sender.ul_bandwidth, receiver.dl_bandwidth)
        delay = sender.base_latency + receiver.base_latency
        delay += message.size / float(bw)
        # schedule later
        receiver.receive(sender, message)


class SimNetwork(Network):

    def __init__(self, env):
        self.env = env

    def deliver(self, sender, receiver, message):
        bw = min(sender.ul_bandwidth, receiver.dl_bandwidth)
        delay = sender.base_latency + receiver.base_latency
        delay += message.size / bw

        def transfer():
            yield self.env.timeout(delay)
            receiver.receive(sender, message)

        self.env.process(transfer())

class Transport(object):

    ul_bandwidth = 1 * 10**6  # bytes/s net bandwidth
    dl_bandwidth = 1 * 10**6  # bytes/s net bandwidth
    base_latency = 0.05  # secs

    def __init__(self, network):
        self.network = network
        self.receive_listeners = []
        self.peers = []

        # filter duplicates, should be a lru set
        self.egress_filter = set()
        self.ingress_filter = set()

    def broadcast(self, m):
        assert isinstance(m, Message)
        if m.hash not in self.egress_filter:
            self.egress_filter.add(m.hash)
            self.ingress_filter.add(m.hash)
            for p in self.peers:
                self.send(p, m)
        assert m.hash in self.egress_filter and m.hash in self.ingress_filter

    def send(self, peer, m):
        self.network.deliver(self, peer, m)

    def sendany(self, m):
        if self.peers:
            self.peers[0].receive(self, m)

    def receive(self, peer, m):
        assert isinstance(peer, Transport)
        assert isinstance(m, Message)
        # implement delay here
        if m.hash in self.ingress_filter:
            return
        self.ingress_filter.add(m)
        for l in self.receive_listeners:
            l(peer, m)


class NodeEnv(Transport, EnvironmentBase):
    # recover timeout also needed in case we had a 50:50 network split or on bootstrap
    def __init__(self, network):
        Transport.__init__(self, network)
        self.timeout = None

    def start_timeout(self, duration, cb):
        pass

    def cancel_timeout(self):
        pass


class Node(object):
    def __init__(self, network, validators, address, genesis):
        self.env = NodeEnv(network)
        self.consensus_protocol = ConsensusProtocol(coinbase=address,
                                                    env=self.env,
                                                    validators=validators,
                                                    head=genesis)

    def add_peer(self, peer):
        assert isinstance(peer, Node)
        self.env.peers.append(peer.env)

    def start(self):
        self.consensus_protocol.start()








def main():

    # use simpy?
    use_simpy = True
    sim_duration = 10  # secs

    # validators
    num_validators = 10
    LockSet.eligible_votes = num_validators

    validators = range(num_validators)
    genesis = mk_genesis(validators)
    assert genesis.lockset.is_valid

    if use_simpy:
        env = simpy.Environment()
        network = SimNetwork(env)
    else:
        network = Network()

    nodes = []
    for a in validators:
        n = Node(network, validators, a, genesis)
        nodes.append(n)
    for n in nodes:  # connect all nodes for now
        for nn in nodes:
            n.add_peer(nn)

    for n in nodes:
        n.start()

    if use_simpy:
        env.run(until=sim_duration)


if __name__ == '__main__':
    main()
