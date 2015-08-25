# Copyright (c) 2015 Heiko Hees
import simpy
import random
from base import Message, EnvironmentBase, mk_genesis, LockSet
from utils import DEBUG
from hc_consensus import ConsensusProtocol

random.seed(42)

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

    def broadcast(self, sender, m):
        assert isinstance(m, Message)
        if self.is_faulty:
            print 'broadcast failed'
        if m.hash not in self.egress_filter:
            self.egress_filter.add(m.hash)
            self.ingress_filter.add(m.hash)
            for p in self.peers:
                self.send(sender, p, m)
        assert m.hash in self.egress_filter and m.hash in self.ingress_filter

    def sendany(self, sender, m):
        if self.peers:
            self.send(sender, self.peers[0], m)

    def _send(self, sender, peer, m):
        assert isinstance(sender, ConsensusProtocol), type(sender)
        self.network.deliver(self, peer, m)

    send = _send

    def receive(self, peer, m):
        assert isinstance(peer, Transport)
        assert isinstance(m, Message)
        # implement delay here
        if m.hash in self.ingress_filter:
            return
        self.ingress_filter.add(m)
        for l in self.receive_listeners:
            l(peer, m)

    # faultyness

    def wont_send(self, sender, peer, m):
        pass

    def send_on_timeout_window(self, sender, peer, m):
        assert isinstance(sender, ConsensusProtocol), type(sender)
        assert isinstance(sender.timeout, (int, float)), sender.timeout

        def d():
            yield self.network.env.timeout(sender.timeout)
            self._send(sender, peer, m)
            print 'sending late', sender, m

        self.network.env.process(d())

    @property
    def is_faulty(self):
        return self.send != self._send


class SimTimeout(object):

    def __init__(self, env, delay, cb):
        self.env = env
        self.cb = cb
        self.delay = delay
        self.env.process(self.start())

    def start(self):
        yield self.env.timeout(self.delay)
        if self.cb:
            self.cb()

    def cancel(self):
        self.cb = None


class NodeEnv(Transport, EnvironmentBase):
    # recover timeout also needed in case we had a 50:50 network split or on bootstrap
    def __init__(self, network):
        Transport.__init__(self, network)
        self.timeout = None
        self.network = network

    def start_timeout(self, duration, cb):
        if isinstance(self.network, SimNetwork):
            self.timeout = SimTimeout(self.network.env, duration, cb)

    def cancel_timeout(self):
        if self.timeout:
            self.timeout.cancel()


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


def normvariate_base_latencies(nodes):
    min_latency = 0.005
    for n in nodes:
        t = n.env
        t.base_latency = max(min_latency, random.normalvariate(t.base_latency, t.base_latency/2))
        assert t.base_latency > 0

def add_faulty_nodes(nodes):
    num_faulty = int(len(nodes) * 1/3.)
    for n in nodes[:num_faulty]:
        n.env.send = n.env.wont_send
        assert n.env.is_faulty

def add_slow_nodes(nodes):
    num_faulty = int(len(nodes) * 2/3.)
    for n in nodes[-num_faulty:]:
        n.env.send = n.env.send_on_timeout_window
        assert n.env.is_faulty


def check_consistency(nodes):
    print 'checking consistency'
    cs = [n.consensus_protocol for n in nodes]

    # check they are all on the same block or the previous one
    s = list(set(c.head.height for c in cs))
    if len(s) > 1:
        print 'nodes on different heights', s
        print 'but note: byzantine nodes might have no chance to sync'
    height = max(s)

    # check they are all using the same block
    while height > 0:
        bs = list(set(c.get_block(height) for c in cs))
        assert len(bs) == 1 or (len(bs) == 2 and None in bs), bs
        height -= 1

    # highest round seen (i.e. number of failed proposers)
    max_rounds = 0
    for c in cs:
        blk = c.head
        while blk.height > 0:
            max_rounds = max(max_rounds, blk.lockset.round)
            blk = c.get_block(blk.height-1)

    # messages
    bytes_transfered = 0
    num_messages = 0
    for c in cs:
        num_messages += len(c.messages)
        bytes_transfered += sum(m.size for m in c.messages)

    print num_messages, 'messages'
    print bytes_transfered, 'bytes transfered'


    print 'max rounds', max_rounds



def main():

    # use simpy?
    use_simpy = True
    sim_duration = 100  # secs

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

    normvariate_base_latencies(nodes)
#    add_faulty_nodes(nodes)
    add_slow_nodes(nodes)

    for n in nodes:
        n.start()

    if use_simpy:
        env.run(until=sim_duration)

    check_consistency(nodes)

    return nodes

if __name__ == '__main__':
    nodes = main()
