import time
from ethereum.utils import sha3
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
        return '<Signature %r>' % [self.address, self.height, self.round]


class Block(Hashable):

    def __init__(self, coinbase, height, round, prevhash, lockset):
        assert isaddress(coinbase)
        self.coinbase = coinbase  # note: needs a signature
        self.height = height
        self.round = round  # only added to differentiate blocks at H
        self.prevhash = prevhash
        assert not len(lockset) or lockset.has_quorum   # genesis
        self.lockset = lockset

    def __repr__(self):
        return '<Block cb=%s h=%s r=%sprevhash=%s %r>' % \
             (self.coinbase, self.height, self.round, self.prevhash[8].encode('hex'), self.lockset)

    def validate(self):
        return True


class Message(Hashable):
    pass


class SignedMessage(Hashable):
    def __init__(self, signature):
        self.signature = signature

    def __repr__(self):
        return "<%s %r>" % (self.__class__.__name__, self.signature)

# syncing
class BlockRequest(Message):
    def __init__(self, blockhash):  # no sig necessary
        super(BlockRequest, self).__init__()
        self.blockhash = blockhash

    def __repr__(self):
        return "<%s %r>" % (self.__class__.__name__, self.block)


class BlockReply(Message):
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
    def __init__(self, signature, block, lockset):
        super(BlockProposal, self).__init__(signature, lockset)
        self.block = block

class VotingInstruction(Proposal):
    def __init__(self, signature, blockhash, lockset):
        super(VotingInstruction, self).__init__(signature, lockset)
        self.blockhash = blockhash

# votes
class Vote(SignedMessage):
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


class LockSet(Hashable):  # careful, is mutable!

    eligible_votes = 10

    def __init__(self):
        self.votes = set()

    def __repr__(self):
        return '<LockSet(%r)>' % list(self.votes)

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
        h = set([(v.signature.height, v.signature.round) for v in self.votes])
        assert len(h) == 1
        return list(h)[0]

    @property
    def is_valid(self):
        return len(self) > 2/3. * self.eligible_votes

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
        assert self.has_quorum or self.has_quorum_possible


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

    def get_quorum_lockset(self, height):
        pass

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
        ls.add(Locked(Signature(a, 0, 0), bh))
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



class ConsensusProtocol(object):

    timeout = 2.

    def __init__(self, coinbase, env, validators, head):
        assert isaddress(coinbase)
        self.coinbase = coinbase
        assert isinstance(env, EnvironmentBase)
        self.env = env
        self.env.receive_listeners.append(self.receive_message)

        self.validators = [v for v in validators if isaddress(v)]
        assert len(validators) == LockSet.eligible_votes  # fixme
        assert self.coinbase in self.validators
        self.locksets = LockSetManager()
        assert head.height == 0 or head.prevhash in self.blocks
        self.head = head
        self.blocks = {head.hash: head}  # hash -> block

        # the state
        self.lock = None
        self.round = 0
        self.height  # is a property, see below

        self.new_height()

    def start(self):
        self.round = 0
        l = self.head.lockset
        self.locksets.locksets[(l.height, l.round)] = l
        # checks
        self.create_proposal()


    @property
    def state(self):
        return dict(lock=self.lock, height=self.height, round=self.round)

    @property
    def height(self):
        return self.head.height + 1

    def new_height(self):
        self.locked = None  # None, NotLocked or Locked
        # self.locksets.cleanup()
        self.round = -1  # will be incremented in new_round
        self.new_round()

    def new_round(self):
        self.round += 1
        self.env.start_timeout(self.timeout, self.on_timeout)  # implicitly cancels old timeout
        # we might need to propose
        self.create_proposal()


    def signature(self):
        return Signature(self.coinbase, self.height, self.round)

    def on_timeout(self):
        # round timed out. i.e. we should not have received a proposal for this round
        if self.lock and isinstance(self.lock, Locked):
            assert self.lock.round == 0 or self.lock.round <= self.round
        lockset = self.lockset.get(self.height, self.round)
        assert lockset and lockset.is_valid

        # increment
        self.new_round()

        if self.lock and isinstance(self.lock, Locked):
            self.lock = Locked(self.signature(), self.lock.blockhash)
        else:
            self.lock = NotLocked(self.signature())
        self.env.broadcast(self.lock)


    def commit(self, bh=None, block=None):
        if block:
            bh = block.hash
            self.blocks[bh] = block
        if bh not in self.blocks:
            self.sync(bh)
            return
        blk = self.blocks[bh]
        assert self.height == blk.height - 1
        assert blk.prevhash in self.blocks
        assert blk.prevhash == self.head.hash
        self.head = blk
        self.new_height()

    def sync(self, blockhash):
        "request block for blockhash"
        self.env.sendany(BlockRequest(self.signature(), blockhash))

    def receive_message(self, peer, m):
        assert isinstance(m, Message)
        if isinstance(m, Vote):
            self.receive_vote(m)
        elif isinstance(m, Proposal):
            self.receive_proposal(m)
        elif isinstance(m, BlockRequest):
            self.receive_block_request(peer, m.blockhash)
        elif isinstance(m, BlockReply):
            self.receive_block(m.block)
        else:
            raise Exception('unhandled message')

    def receive_block_request(self, peer, blockhash):
        b = self.block.get(blockhash)
        if b:
            self.network.send(peer, BlockReply(b))

    def receive_block(self, blk):
        assert blk.lockset.has_quorum_possible
        if blk.height <= self.height:
            return
        if blk.prevhash not in self.blocks:
            self.sync(blk.prevhash)
        else:
            assert blk.prevhash == self.head.hash
            self.commit(block=blk)

    def receive_vote(self, v):
        # ignore votes on older heights and rounds
        H, R = v.signature.height, v.signature.round
        if H < self.height:
            return
        if H == self.height and R < self.round:
            return

        # broadcast
        self.env.broadcast(v)

        # add to lockset
        lockset = self.locksets.get(H, R)
        was_valid = lockset.is_valid
        lockset.add(v)

        # did we just get a valid lockset
        if not was_valid and lockset.is_valid:
            self.on_valid_lockset(lockset)


    def on_valid_lockset(self, lockset):
        # we go to a new round

        # commit if possible
        assert lockset.height == self.height
        bh = lockset.has_quorum
        if bh:
            self.commit(bh)
            assert self.round == 0
        else:
            self.new_round(lockset.round + 1)


    def unlock(self, lockset):
        if self.lock is None:
            return
        assert lockset.is_valid
        if lockset.height == self.lock.signature.height:
            assert lockset.round > self.lock.signature.round
        self.lock = None

    def lock(self, height, round, bh=None):
        assert not self.lock
        assert self.height == height and self.round == round
        if bh:
            lock = Locked(self.signature(), bh)
        else:
            lock = NotLocked(self.signature())
        self.lock = lock
        return lock

    def receive_proposal(self, p):
        if not p.lockset.is_valid:
            return
        H, R = p.signature.height, p.signature.round
        if H < self.height:
            return
        if R <= self.round:
            return
        if H > self.height + 1:
            raise Exception('need to sync')
        if H == self.height + 1:  # proposal for new heigth
            bh = p.lockset.has_quorum
            if not bh:  # new height only valid if there is quorum
                return
            self.commit(H, bh)
        elif isinstance(p, BlockProposal):
            if not p.lockset.has_noquorum:
                return  # need no quorum to accept new porposal
            # FIXME validate block
            self.vote(p)
        else:
            assert isinstance(p, VotingInstruction)
            if not p.lockset.has_quorum_possible:
                return  # need no quorum possible to accept VotingInstruction
            self.vote(p)

    def proposer(self, height, round):
        v = abs(hash(repr((height, round))))
        return self.validators[v % len(self.validators)]

    def create_proposal(self):
        """
        proposals are always for a new round
        and need to include a valid LockSet of the last round
        """
        lockset = self.locksets.get((self.height, self.round), LockSet())

        if self.height > 1 or self.round > 0:  # genesis exception
            assert lockset.is_valid
        elif len(lockset):
            # increase round if not increased yet by commit
            H, R = lockset.height_round
            if self.round == R:
                assert self.height == H
                self.new_round()

        if self.coinbase != self.proposer(self.height, self.round):
            return
        if self.round == 0 or lockset.has_noquorum:
            # get quorum which signs prev block
            ls = self.locksets.get_quorum_lockset(self.height-1)
            assert ls or self.height == 1  # FIXME genesis
            block = Block(self.coinbase, self.height, self.round, self.head.hash, lockset)
            proposal = BlockProposal(block, self.signature(), lockset)

        elif lockset.has_quorum_possible:
            bh = lockset.has_quorum_possible
            proposal = VotingInstruction(self.signature(), bh, lockset)
        else:
            raise Exception('invalid lockset')
        self.vote(proposal)
        self.env.broadcast(proposal)

    def vote(self, proposal):
        assert isinstance(proposal, Proposal)
        lockset = proposal.lockset
        assert lockset.is_valid
        if isinstance(proposal, BlockProposal):
            assert lockset.has_noquorum or self.lockset.height < self.heigh
            assert proposal.block.validate()
            self.blocks[proposal.block.hash] = proposal.block
            vote = self.lock(proposal.block.hash)
        else:
            assert isinstance(proposal, VotingInstruction)
            assert lockset.has_quorum_possible
            self.unlock()
            if isinstance(self.lock, Locked) and self.lock.blockhash == proposal.blockhash:
                # already locked on this block
                pass
            if proposal.blockhash not in self.blocks:
                # don't know this block
                vote = self.lock()
            else:
                vote = self.lock(proposal.blockhash)
        self.env.broadcast(vote)



