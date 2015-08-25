# Copyright (c) 2015 Heiko Hees
import time
from utils import sha3, phx
from collections import Counter

def now():
    return int(time.time()*1000)

def ishash(h):
    return isinstance(h, str) and len(h) == 32

def isaddress(a):
    return isinstance(a, (int, long)) and a >= 0

class Hashable(object):

    def __hash__(self):
        return hash(repr(self))

    @property
    def hash(self):
        return sha3(str(hash(self)))

    def __eq__(self, other):
        return hash(self) == hash(other)


class Signature(Hashable):
    "note: this is a dummy signature for quick simulations"
    def __init__(self, address, height, round):
        assert isaddress(address)
        self.address = address
        self.height = height
        self.round = round

    def __repr__(self):
        return '<Sig A:%d H:%d R:%d>' % (self.address, self.height, self.round)


class Block(Hashable):

    def __init__(self, coinbase, height, round, prevhash, lockset):
        assert isaddress(coinbase)
        assert ishash(prevhash)
        self.coinbase = coinbase  # note: needs a signature
        self.height = height
        self.round = round  # only added to differentiate blocks at H
        self.prevhash = prevhash
        assert not len(lockset) or lockset.has_quorum   # genesis
        self.lockset = lockset

    def __repr__(self):
        return '<Block CB:%s H:%s R:%s prev=%s %r>' % \
             (self.coinbase, self.height, self.round, phx(self.prevhash), self.lockset)

    def validate(self):
        return True


class Message(Hashable):
    size = 100  # bytes
    pass


class SignedMessage(Message):
    size = 165
    def __init__(self, signature):
        self.signature = signature

    def __repr__(self):
        return "<%s %r>" % (self.__class__.__name__, self.signature)

    @property
    def height(self):
        return self.signature.height

    @property
    def round(self):
        return self.signature.round

# syncing
class BlockRequest(Message):
    size = 100
    def __init__(self, blockhash):  # no sig necessary
        super(BlockRequest, self).__init__()
        self.blockhash = blockhash

    def __repr__(self):
        return "<%s %r>" % (self.__class__.__name__, phx(self.blockhash))


class BlockReply(Message):
    size = 1000
    def __init__(self, block):  # no sig necessary
        super(BlockReply, self).__init__()
        self.block = block

    def __repr__(self):
        return "<%s %r>" % (self.__class__.__name__, self.block)


# proposals
class Proposal(SignedMessage):
    def __init__(self, signature, lockset):
        super(Proposal, self).__init__(signature)
        self.lockset = lockset

class BlockProposal(Proposal):
    size = 1000 + 1000
    def __init__(self, signature, lockset, block):
        super(BlockProposal, self).__init__(signature, lockset)
        self.block = block

    def __repr__(self):
        return "<%s %r B:%s>" % (self.__class__.__name__, self.signature, phx(self.block.hash))


class VotingInstruction(Proposal):
    size = 100 + 1000
    def __init__(self, signature, lockset, blockhash):
        super(VotingInstruction, self).__init__(signature, lockset)
        self.blockhash = blockhash

    def __repr__(self):
        return "<%s %r B:%s>" % (self.__class__.__name__, self.signature, phx(self.blockhash))


# votes
class Vote(SignedMessage):
    size = 100

    def __init__(self, signature):
        super(Vote, self).__init__(signature)

class NotLocked(Vote):
    "promise to not vote on any block until unlocked"
    pass

class Locked(Vote):
    "promise to not vote on a different block until unlocked"
    def __init__(self, signature, blockhash):
        super(Locked, self).__init__(signature)
        assert ishash(blockhash)
        self.blockhash = blockhash

    def __repr__(self):
        return "<%s %r B:%r>" % (self.__class__.__name__, self.signature, phx(self.blockhash))


class LockSet(Hashable):  # careful, is mutable!

    eligible_votes = 10
    processed = False

    def __init__(self):
        self.votes = set()

    def __repr__(self):
        return '<LockSet(H:%d R:%d V:%d)>' % (self.height, self.round, len(self))

    def add(self, vote):
        assert isinstance(vote, Vote)
        if vote not in self.votes:
            assert vote.signature not in [v.signature for v in self.votes]
            assert not len(self) or self.height_round == (v.signature.height, v.signature.round)
            self.votes.add(vote)
            return True

    def __len__(self):
        return len(self.votes)

    def blockhashes(self):
        assert self.is_valid
        c = Counter(v.blockhash for v in self.votes if isinstance(v, Locked))
        # deterministc sort necessary
        return sorted(c.most_common(), cmp=lambda a, b: cmp((b[1], b[0]), (a[1], a[0])))


    @property
    def height_round(self):
        assert len(self), 'no votes, can not determin height'
        h = set([(v.signature.height, v.signature.round) for v in self.votes])
        assert len(h) == 1, len(h)
        return list(h)[0]

    height = property(lambda self: self.height_round[0])
    round = property(lambda self: self.height_round[1])

    @property
    def is_valid(self):
        return len(self) > 2/3. * self.eligible_votes and self.height_round

    @property
    def has_quorum(self):
        """
        we've seen +2/3 of all eligible votes voting for one block.
        there is a quorum.
        """
        assert self.is_valid
        bhs = self.blockhashes()
        if bhs and bhs[0][1] > 2/3. * self.eligible_votes:
            assert self.has_quorum_possible
            return bhs[0][0]
        assert self.has_noquorum or self.has_quorum_possible


    @property
    def has_noquorum(self):
        assert self.is_valid
        bhs = self.blockhashes()
        if not bhs or bhs[0][1] < 1/3. * self.eligible_votes:
            assert not self.has_quorum_possible
            return True


    @property
    def has_quorum_possible(self):
        """
        we've seen +1/3 of all eligible votes voting for one block.
        at least one vote was from a honest node.
        we can assume that this block is agreeable.
        """
        assert self.is_valid  # we could tell that earlier
        bhs = self.blockhashes()
        if bhs and bhs[0][1] > 1/3. * self.eligible_votes:
            return bhs[0][0]


class LockSetManager(object):

    def __init__(self):
        self.locksets = dict()

    def update(self, lockset):
        for vote in lockset.votes:
            self.get(vote.signature.height, vote.signature.round).add(vote)

    def cleanup(self, height):
        # cleanup older locksets
        for (H, R) in self.locksets.keys():
            if H < height - 1:  # keep lockset of last height
                del self.locksets[(H, R)]

    def get(self, height, round):
        return self.locksets.setdefault((height, round), LockSet())




def mk_genesis(validators):
    ls = LockSet()
    bh = '\0'*32
    for a in validators:
        ls.add(Locked(Signature(a, height=0, round=0), bh))
    assert ls.is_valid
    assert bh == ls.has_quorum
    genesis = Block(2**256-1, 0, 0, bh, ls)
    return genesis


class EnvironmentBase(object):

    def __init__(self):
        self.receive_listeners = []

    def broadcast(self, m):
        pass

    def sendany(self, m):
        pass

    def start_timeout(self, duration, cb):
        pass

    def cancel_timeout(self):
        pass

