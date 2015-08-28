# Copyright (c) 2015 Heiko Hees
import simpy
import random
from collections import Counter
from base import Message, EnvironmentBase, mk_genesis, LockSet
from utils import DEBUG
from hc_consensus2 import ConsensusManager, ChainManager, RoundManager

random.seed(42)


class Network(object):

    def deliver(self, sender, receiver, message):
        bw = min(sender.ul_bandwidth, receiver.dl_bandwidth)
        delay = sender.base_latency + receiver.base_latency
        delay += message.size / float(bw)
        # schedule later
        receiver.receive(sender, message)


class SimNetwork(Network):

    last_delivery = 0

    def __init__(self, env):
        self.env = env

    def deliver(self, sender, receiver, message):
        bw = min(sender.ul_bandwidth, receiver.dl_bandwidth)
        delay = sender.base_latency + receiver.base_latency
        delay += message.size / bw

        def transfer():
            yield self.env.timeout(delay)
            receiver.receive(sender, message)
            self.last_delivery = self.env.now

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

    def broadcast(self, sender, m, force=False):
        assert isinstance(m, Message)
        if self.is_faulty:
            print 'broadcast failed'
        if m.hash not in self.egress_filter or force:
            self.egress_filter.add(m.hash)
            self.ingress_filter.add(m.hash)
            for p in self.peers:
                self.send(sender, p, m)
        assert m.hash in self.egress_filter and m.hash in self.ingress_filter

    def sendany(self, sender, m):
        if self.peers:
            self.send(sender, self.peers[0], m)

    def _send(self, sender, peer, m):
        assert isinstance(sender, ConsensusManager), type(sender)
        self.network.deliver(self, peer, m)

    send = _send

    def receive(self, peer, m):
        assert isinstance(peer, Transport)
        assert isinstance(m, Message)
        if m.hash in self.ingress_filter:
            return
        self.ingress_filter.add(m.hash)
        for l in self.receive_listeners:
            l(peer, m)

    # faultyness

    def wont_send(self, sender, peer, m):
        pass

    def send_on_timeout_window(self, sender, peer, m):
        to = sender.active_round.timeout
        assert isinstance(sender, ConsensusManager), type(sender)
        assert isinstance(to, (int, float)), to

        def d():
            yield self.network.env.timeout(to)
            self._send(sender, peer, m)
            # print 'sending late', sender, m

        self.network.env.process(d())

    @property
    def is_faulty(self):
        return self.send != self._send


class SimTimeout(object):

    def __init__(self, env, delay, cb, *args):
        self.env = env
        self.delay = delay
        self.cb = cb
        self.args = args
        self.env.process(self.start())

    def start(self):
        yield self.env.timeout(self.delay)
        if self.cb:
            self.cb(*self.args)

    def cancel(self):
        self.cb = None


class NodeEnv(Transport, EnvironmentBase):
    # recover timeout also needed in case we had a 50:50 network split or on bootstrap

    def __init__(self, network):
        Transport.__init__(self, network)
        self.timeout = None
        self.network = network

    @property
    def now(self):
        return self.network.env.now

    def start_timeout(self, delay, cb, *args):
        if isinstance(self.network, SimNetwork):
            self.timeout = SimTimeout(self.network.env, delay, cb, *args)

    def cancel_timeout(self):
        if self.timeout:
            self.timeout.cancel()


class Node(object):

    def __init__(self, network, validators, address, genesis):
        self.env = NodeEnv(network)
        chainmanager = ChainManager(genesis)
        self.consensus_protocol = ConsensusManager(env=self.env,
                                                   chainmanager=chainmanager,
                                                   coinbase=address,
                                                   validators=validators)

    def add_peer(self, peer):
        assert isinstance(peer, Node)
        self.env.peers.append(peer.env)

    def start(self):
        self.consensus_protocol.start()


def normvariate_base_latencies(nodes, sigma_factor=0.5):
    min_latency = 0.001
    for n in nodes:
        t = n.env
        sigma = t.base_latency * sigma_factor
        t.base_latency = max(min_latency, random.normalvariate(t.base_latency, sigma))
        assert t.base_latency > 0


def add_faulty_nodes(nodes, num_faulty):
    for n in nodes[:num_faulty]:
        n.env.send = n.env.wont_send
        assert n.env.is_faulty


def add_slow_nodes(nodes, num_slow):
    # nodes sending at the edge of the timeout window
    for n in list(reversed(nodes))[:num_slow]:
        n.env.send = n.env.send_on_timeout_window
        assert n.env.is_faulty


def check_consistency(nodes):
    print 'checking consistency'
    cs = [n.consensus_protocol for n in nodes]
    # check they are all on the same block or the previous one
    s = Counter(c.head.height for c in cs)
    if len(s) > 1:
        print 'nodes on different heights (H:num_nodes)', s
        print 'but note: byzantine nodes might have no chance to sync'
    else:
        print 'all nodes on same height', s
    max_height = height = max(s)

    # check they are all using the same block
    while height > 0:
        bs = list(set(c.chainmanager.get_block_by_height(height) for c in cs))
        assert len(bs) == 1 or (len(bs) == 2 and None in bs), bs
        height -= 1

    # highest round seen (i.e. number of failed proposers)
    max_rounds = 0
    for c in cs:
        blk = c.head
        assert blk
        while blk.height > 0:
            max_rounds = max(max_rounds, blk.lockset.round)
            blk = c.chainmanager.get_block_by_height(blk.height - 1)

    # messages
    ingress_bytes_transfered = 0
    ingress_num_messages = 0
    egress_bytes_transfered = 0
    egress_num_messages = 0

    for c in cs:
        ingress_num_messages += len(c.messages_received)
        ingress_bytes_transfered += sum(m.size for m in c.messages_received)
        egress_num_messages += len(c.messages_sent) * len(nodes)
        egress_bytes_transfered += sum(m.size for m in c.messages_sent) * len(nodes)

    print ingress_num_messages, 'ingress messages'
    print ingress_bytes_transfered, 'bytes received (note this is filtered)'
    print ingress_bytes_transfered / max_height / len(nodes), 'bytes per height and node'

    print egress_num_messages, 'egress messages'
    print egress_bytes_transfered, 'bytes sent'
    print egress_bytes_transfered / max_height / len(nodes), 'bytes per height and node'
    print 'max height', max_height, 'max rounds', max_rounds + 1
    print
    elapsed = nodes[0].env.network.last_delivery
    print 'elapsed', elapsed
    print 'avg/block time', elapsed / max_height


def main(num_nodes=10, sim_duration=10, timeout=0.5,
         base_latency=0.05, latency_sigma_factor=0.5,
         num_faulty_nodes=3, num_slow_nodes=0):

    sim_duration = sim_duration  # secs
    # initial timeout
    RoundManager.timeout = timeout  # secs
    Transport.base_latency = base_latency  # 0.05 is half the globe one way

    # validators
    num_validators = num_nodes
    LockSet.eligible_votes = num_validators

    validators = range(num_validators)
    genesis = mk_genesis(validators)
    assert genesis.lockset.is_valid

    env = simpy.Environment()
    network = SimNetwork(env)

    nodes = []
    for a in validators:
        n = Node(network, validators, a, genesis)
        nodes.append(n)
    for n in nodes:  # connect all nodes for now
        for nn in nodes:
            n.add_peer(nn)

    normvariate_base_latencies(nodes, sigma_factor=latency_sigma_factor)
    assert num_faulty_nodes + num_slow_nodes <= num_nodes
    add_faulty_nodes(nodes, num_faulty_nodes)
    add_slow_nodes(nodes, num_slow_nodes)

    for n in nodes:
        n.start()

    env.run(until=sim_duration)

    if False:
        print '\n' * 3
        print int(env.now), 'resetting one node'

        # reset one node and have it sync up
        n = nodes[0]
        n.consensus_protocol.reset(to_genesis=True)
        n.env.send = n.env._send
        env.run(until=sim_duration * 4)

    if False:  # run w/ stopped nodes to check sync
        print '\n' * 3
        print int(env.now), 'stopping proposals and timeouts'

        for n in nodes:
            n.consensus_protocol.stopped = True

        #  fix faulty(hope they sync up)
        for n in nodes:
            if n.env.send == n.env.wont_send:
                n.env.send = n.env._send

        env.run(until=sim_duration * 10)

    check_consistency(nodes)

    return nodes

if __name__ == '__main__':
    num_nodes = 10
    faulty_fraction = 1 / 3.  # nodes not sending anything
    slow_fraction = 1 / 3.    # nodes sending votes and proposals at the edge of the timeout window

    nodes = main(num_nodes=num_nodes,
                 sim_duration=50,
                 timeout=0.5,
                 base_latency=0.05,
                 latency_sigma_factor=0.5,
                 num_faulty_nodes=int(num_nodes * faulty_fraction),
                 num_slow_nodes=int(num_nodes * slow_fraction)
                 )
