# Copyright (c) 2015 Heiko Hees
from base import LockSet, LockSetManager, Locked, NotLocked, Message, Vote
from base import EnvironmentBase, Signature, BlockProposal, VotingInstruction
from base import BlockRequest, BlockReply, Block, Proposal, isaddress
from utils import cstr, DEBUG, phx


##################################################################################


class ManagerDict(object):

    def __init__(self, dklass, parent):
        self.d = dict()
        self.dklass = dklass
        self.parent = parent

    def __getitem__(self, k):
        if k not in self.d:
            self.d[k] = self.dklass(self.parent, k)
        return self.d[k]

    def __iter__(self):
        return iter(self.d)

    def pop(self, k):
        self.d.pop(k)


class MissingParent(Exception):
    pass


class ChainManager(object):

    "chain manager mock"

    def __init__(self, genesis=None):
        self.chain = []
        if genesis:
            self.chain.append(genesis)

    @property
    def head(self):
        return self.chain[-1]

    def add_block(self, blk):
        assert blk.prevhash == self.head.hash
        assert self.validate_block(blk)
        self.chain.append(blk)

    def validate_block(self, blk):
        if blk.prevhash not in self:
            raise MissingParent('can not validate block w/o commited parent')
        return blk.validate()

    def __contains__(self, bh):
        return bh in [b.hash for b in self.chain]

    def get(self, bh, default=None):
        for b in reversed(self.chain):
            if b.hash == bh:
                return b
        return default

    def get_block_by_height(self, height):
        for b in reversed(self.chain):
            if b.height == height:
                return b


class Synchronizer(object):

    def __init__(self, consensusmanager):
        self.cm = consensusmanager
        self.requested = set()

    def process(self):
        "check which blocks are missing, request and keep track of them"
        missing = set()
        for blk in self.cm.block_candidates.values():
            if not self.cm.get_block(blk.prevhash):
                missing.add(blk.prevhash)
            elif blk.prevhash in self.requested:
                self.requested.remove(blk.prevhash)  # cleanup
        p = self.cm.active_round.proposal
        if isinstance(p, VotingInstruction) and not self.cm.get(p.blockhash):
            missing.add(p.blockhash)

        for blockhash in missing - self.requested:
            self.requested.add(blockhash)
            self.cm.broadcast(BlockRequest(blockhash))


class ConsensusManager(object):

    def __init__(self, env, chainmanager, coinbase, validators):
        self.env = env
        self.env.receive_listeners.append(self.receive_message)
        self.chainmanager = chainmanager
        self.coinbase = coinbase
        self.validators = validators
        self.synchronizer = Synchronizer(self)
        self.heights = ManagerDict(HeightManager, self)
        self.block_candidates = dict()

        assert len(validators) == LockSet.eligible_votes  # fixme
        assert self.coinbase in self.validators

        # add initial lockset
        for v in self.head.voteset:
            self.add_vote(v)

        # debug
        self.messages_received = []
        self.messages_sent = []
        self.stopped = False

    def __repr__(self):
        return '<CP A:%d H:%d R:%d L:%r>' % (self.coinbase, self.height, self.round, self.lock)

    def log(self, tag, **kargs):
        # if self.coinbase !=0: return
        t = int(self.env.now)
        c = lambda x: cstr(self.coinbase, x)
        msg = ' '.join([str(t), c(repr(self)),  tag, (' %r' % kargs if kargs else '')])
        if self.stopped:
            msg = 'X' + msg
        print msg

    @property
    def head(self):
        return self.chainmanager.head

    @property
    def height(self):
        return self.head.height + 1

    @property
    def round(self):
        return self.heights[self.height].round

    def proposer(self, height, round):
        v = abs(hash(repr((height, round))))
        return self.validators[v % len(self.validators)]

    # message handling

    def broadcast(self, m):
        self.messages_sent.append(m)
        self.env.broadcast(self, m)

    def receive_message(self, peer, m):
        self.log('receive', msg=m)
        self.messages_received.append(m)
        assert isinstance(m, Message)
        if isinstance(m, Vote):
            self.add_vote(m)
        elif isinstance(m, Proposal):
            self.add_proposal(m)
        elif isinstance(m, BlockRequest):
            self.receive_block_request(peer, m)
        elif isinstance(m, BlockReply):
            self.add_block(m.block)
        else:
            raise Exception('unhandled message')
        self.process()

    def get_block(self, blockhash):
        return self.block_candidates.get(blockhash) or self.chainmanager.get(blockhash)

    def receive_block_request(self, peer, blockrequest):
        blockhash = blockrequest.blockhash
        b = self.get_block(blockhash)
        self.log('receive block request', bh=phx(blockhash), found=b)
        if b:
            self.env.send(self, peer, BlockReply(b, id(blockrequest)))  # filter id

    def add_vote(self, v):
        assert isinstance(v, Vote)
        assert v.signature.address in self.validators
        self.heights[v.height].add_vote(v)

    def add_proposal(self, p):
        assert p.signature.address in self.validators
        assert p.lockset.is_valid
        assert p.lockset.height == p.height or p.round == 0
        assert p.height == p.signature.height
        assert p.lockset.round == p.round == p.signature.round
        for v in p.lockset:
            self.add_vote(v)  # implicitly checks their validity
        if isinstance(p, BlockProposal):
            blk = p.block
            assert blk.height == p.height
            assert p.lockset.has_noquorum or p.round == 0
            self.add_block(blk)  # implicitly checks the votes validity
        else:
            assert isinstance(p, VotingInstruction)
            assert p.lockset.has_quorum_possible
        self.heights[p.height].add_proposal(p)

    def add_block(self, blk):
        assert blk.voteset.has_quorum  # on previous block
        assert blk.voteset.height == blk.height - 1
        for v in blk.voteset:
            self.add_vote(v)
        self.block_candidates[blk.hash] = blk

    def last_committing_lockset(self):
        return self.heights[self.height - 1].last_valid_lockset()

    def last_valid_lockset(self):
        return self.heights[self.height].last_valid_lockset() or self.last_committing_lockset()

    @property
    def lock(self):
        self.heights[self.height].last_lock()

    @property
    def active_round(self):
        hm = self.heights[self.height]
        return hm.rounds[hm.round]

    def setup_timeout(self):
        ar = self.active_round
        delay = ar.setup_timeout()
        if delay is not None:
            self.env.start_timeout(delay, self.on_timeout, ar)

    def on_timeout(self, ar):
        if self.active_round == ar:
            self.process()

    def process(self):
        self.commit()
        self.cleanup()
        self.synchronizer.process()
        self.heights[self.height].process()
        self.setup_timeout()

    start = process

    def commit(self):
        for blk in [c for c in self.block_candidates.values() if c.prevhash == self.head.hash]:
            if self.heights[blk.height].has_quorum == blk.hash:
                success = self.chainmanager.add_block(blk)
                self.log('commited', blk=blk)
                if success:
                    assert self.head == blk
                    self.commit()
                    return

    def cleanup(self):
        for blk in self.block_candidates.values():
            if self.head.height >= blk.height:
                self.block_candidates.pop(blk.hash)
        for h in list(self.heights):
            if self.heights[h].height < self.head.height:
                self.heights.pop(h)


class HeightManager(object):

    def __init__(self, consensusmanager, height=0):
        self.cm = consensusmanager
        self.log = self.cm.log
        self.height = height
        self.rounds = ManagerDict(RoundManager, self)
        print('Created HeightManager H:%d' % self.height)

    @property
    def round(self):
        "highest valid lockset on height"
        for r in reversed(sorted(self.rounds)):
            if self.rounds[r].lockset.is_valid:
                return r + 1
        return 0

    def last_lock(self):
        "highest lock on height"
        for r in reversed(sorted(self.rounds)):
            if self.rounds[r].lock:
                return self.rounds[r].lock

    def last_valid_lockset(self):
        for r in reversed(sorted(self.rounds)):
            ls = self.rounds[r].lockset
            if ls.is_valid:
                return ls
        return None

    @property
    def has_quorum(self):
        found = None
        for r in sorted(self.rounds):
            ls = self.rounds[r].lockset
            if ls.is_valid and ls.has_quorum:
                assert found is None  # consistency check, only one quorum allowed
                found = ls.has_quorum
        return found

    def add_vote(self, v):
        self.rounds[v.round].add_vote(v)

    def add_proposal(self, p):
        assert p.height == self.height
        assert p.lockset.is_valid
        if p.round > self.round:
            self.round = p.round
        self.rounds[p.round].add_proposal(p)

    def process(self):
        self.rounds[self.round].process()


class RoundManager(object):

    timeout = 1  # secs
    timeout_round_factor = 1.2

    def __init__(self, heightmanager, round=0):
        self.hm = heightmanager
        self.cm = heightmanager.cm
        self.log = self.hm.log
        assert isinstance(round, int)
        self.round = round
        self.height = heightmanager.height
        self.lockset = LockSet()
        self.proposal = None
        self.lock = None
        self.timeout_time = None
        print('Created RoundManager R:%d' % self.round)

    def setup_timeout(self):
        "setup a timeout for waiting for a proposal"
        if self.timeout_time is not None:
            return
        now = self.cm.env.now
        delay = self.timeout * self.timeout_round_factor ** self.round
        self.timeout_time = now + delay
        return delay

    def add_vote(self, v):
        self.lockset.add(v)

    def add_proposal(self, p):
        assert not self.proposal
        self.proposal = p

    def process(self):
        assert self.cm.round == self.round
        assert self.cm.height == self.hm.height == self.height

        if self.cm.stopped:
            self.log('stopped not creating proposal')
            return
        p = self.propose()
        if p:
            self.cm.broadcast(p)
        v = self.vote()
        if v:
            self.cm.broadcast(v)

    def propose(self):
        if self.cm.proposer(self.height, self.round) != self.cm.coinbase:
            return
        if self.proposal:
            assert self.lock and self.lock.blockhash == self.proposal.blockhash
            assert self.proposal.signature.address == self.cm.coinbase
            return

        lockset = self.cm.last_valid_lockset()
        self.log('in creating proposal', lockset=lockset)

        if self.round == 0 or lockset.has_noquorum:
            cl = self.cm.last_committing_lockset()  # quorum which signs prev block
            block = Block(self.cm.coinbase, self.hm.height, self.round, self.cm.head.hash, cl)
            proposal = BlockProposal(self.signature(), lockset, block)
        elif lockset.has_quorum_possible:
            bh = lockset.has_quorum_possible
            proposal = VotingInstruction(self.signature(), lockset, bh)
        else:
            raise Exception('invalid lockset')
        self.log('created proposal', p=proposal)
        self.proposal = proposal
        return proposal

    def vote(self):
        if self.lock:
            return  # voted in this round
        self.log('in vote', proposal=self.proposal)

        # get last lock on height
        last_lock = self.hm.last_lock()

        if self.proposal:
            if isinstance(self.proposal, VotingInstruction):
                assert self.proposal.lockset.has_quorum_possible
                v = Locked(self.signature(), self.proposal.blockhash)
            elif not isinstance(last_lock, Locked):
                assert isinstance(self.proposal, BlockProposal)
                assert self.proposal.lockset.has_noquorum or self.round == 0
                assert self.proposal.block.prevhash == self.cm.head.hash
                assert self.cm.chainmanager.validate_block(self.proposal.block)  # fixme
                v = Locked(self.signature(), self.proposal.block.hash)
            else:  # repeat vote
                v = Locked(self.signature(), last_lock.blockhash)
        elif self.timeout_time is not None and self.timeout_time < self.cm.env.now:
            self.log('timeout')
            if isinstance(last_lock, Locked):  # repeat vote
                v = Locked(self.signature(), last_lock.blockhash)
            else:
                v = NotLocked(self.signature())
        else:
            return
        self.lock = v
        self.lockset.add(v)
        return v

    def signature(self):
        return Signature(self.cm.coinbase, self.cm.height, self.cm.round)
