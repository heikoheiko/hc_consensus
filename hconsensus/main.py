from hc_consensus import ConsensusProtocol, Message, EnvironmentBase, mk_genesis

class Network(object):

    def deliver(self, sender, receiver, message):
        bw = min(sender.ul_bandwidth, receiver.dl_bandwidth)
        delay = sender.base_latency + receiver.base_latency
        delay += message.size / bw
        # schedule later
        receiver.receive(sender, message)


class Transport(object):

    ul_bandwidth = 1 * 10**6  # bytes/s net bandwidth
    dl_bandwidth = 1 * 10**6  # bytes/s net bandwidth
    base_latency = 0.05  # secs

    def __init__(self, network):
        self.network = network
        self.receive_listeners = []
        self.peers = []
        self.broadcast_filter = set()  # filter duplicates, should be a lru set

    def broadcast(self, m):
        assert isinstance(m, Message)
        if m.hash not in self.broadcast_filter:
            self.broadcast_filter.add(m.hash)
            for p in self.peers:
                self.send(p, m)

    def send(self, peer, m):
        peer.receive(self, m)

    def sendany(self, m):
        if self.peers:
            self.peers[0].receive(self, m)

    def receive(self, peer, m):
        assert isinstance(peer, Transport)
        assert isinstance(m, Message)
        # implement delay here
        for l in self.receive_listeners:
            l(peer, m)


class NodeEnv(EnvironmentBase, Transport):
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
    validators = range(10)
    genesis = mk_genesis(validators)
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


if __name__ == '__main__':
    main()
