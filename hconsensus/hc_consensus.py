# Copyright (c) 2015 Heiko Hees
from base import LockSet, LockSetManager, Locked, NotLocked, Message, Vote
from base import EnvironmentBase, Signature, BlockProposal, VotingInstruction
from base import BlockRequest, BlockReply, Block, Proposal
from utils import cstr, DEBUG, isaddress


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
        self.last_valid_lockset = head.lockset

    # utils

    def __repr__(self):
        return '<CP H:%d R:%d L:%r>' % (self.height, self.round, self.lock)

    def log(self, tag, **kargs):
        c = lambda x: cstr(self.coinbase, x)
        msg = ' '.join([c(self.coinbase), c(repr(self)),  tag, (' %r' % kargs if kargs else '')])
        print msg

    @property
    def current_lockset(self):
        return self.locksets.get(self.height, self.round)
    lockset = current_lockset

    @property
    def height(self):
        return self.head.height + 1

    def signature(self):
        return Signature(self.coinbase, self.height, self.round)

    def proposer(self, height, round):
        v = abs(hash(repr((height, round))))
        return self.validators[v % len(self.validators)]

    # locks
    def unlock(self, lockset):
        self.log('unlocking', lock=self.lock)
        if self.lock is None:
            return
        assert lockset.is_valid
        if lockset.height == self.lock.signature.height:
            assert lockset.round > self.lock.signature.round
        self.lock = None

    def relock(self, bh=None):
        self.log('locking', bh=bh.encode('hex')[:8])
        assert not self.lock
        if bh:
            lock = Locked(self.signature(), bh)
        else:
            lock = NotLocked(self.signature())
        self.lock = lock
        return lock


    # communication

    def sync(self, blockhash):
        "request block for blockhash"
        self.log('syncing', bh=blockhash)
        self.env.sendany(BlockRequest(self.signature(), blockhash))

    def receive_message(self, peer, m):
        self.log('receive', msg=m)
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
            assert isinstance(blk, Block)
            self.commit(block=blk)

    # events

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

    def on_valid_lockset(self, lockset):
        assert lockset.is_valid
        self.log('valid lockset', lockset=lockset)

        if lockset.height != self.height:
            assert lockset.height < self.height
            return
        self.last_valid_lockset = lockset

        # commit if possible
        bh = lockset.has_quorum
        if bh:
            self.commit(bh)
            assert self.round == 0
        else:
            self.new_round(lockset.round + 1)

    # steps

    def start(self):
        self.log('starting')
        self.create_proposal()

    def new_height(self):
        self.lock = None  # None, NotLocked or Locked
        # self.locksets.cleanup()
        self.round = -1  # will be incremented in new_round
        self.new_round()

    def new_round(self):
        self.round += 1
        self.log('new round')
        self.env.start_timeout(self.timeout, self.on_timeout)  # implicitly cancels old timeout
        # we might need to propose
        self.create_proposal()

    def commit(self, bh=None, block=None):
        if block:
            assert isinstance(block, Block)
            bh = block.hash
            self.blocks[bh] = block
        self.log('in commit', bh=bh.encode('hex')[:8])
        if bh not in self.blocks:
            self.sync(bh)
            return
        blk = self.blocks[bh]
        self.log('know block', block=blk)
        assert self.height == blk.height
        assert blk.prevhash in self.blocks
        assert blk.prevhash == self.head.hash
        assert blk.validate()
        self.head = blk
        self.new_height()

    def receive_vote(self, v):
        self.log('receive vote', v=v)
        # ignore votes on older heights and rounds
        H, R = v.signature.height, v.signature.round
        if H < self.height:
            return
        if H == self.height and R < self.round:
            return

        # broadcast
        self.log('broadcasting vote')
        self.env.broadcast(v)

        # add to lockset
        lockset = self.locksets.get(H, R)
        lockset.add(v)
        if lockset.is_valid:
            self.on_valid_lockset(lockset)

    def receive_proposal(self, p):
        self.log('receive proposal', p=p)
        if not p.lockset.is_valid:
            self.log('invalid lockset')
            return

        # learn votes, eventually commits
        self.locksets.update(p.lockset)
        self.on_valid_lockset(self.locksets.get(p.lockset.height, p.lockset.round))

        # check if the proposal is on our hight
        H, R = p.signature.height, p.signature.round
        if H < self.height:
            self.log('wrong height')
            return
        if R < self.round:
            self.log('wrong round')
            return
        if H > self.height + 1:
            self.log('not in sync')
            raise Exception('need to sync')

        self.log('proposal is valid')
        if isinstance(p, BlockProposal):
            if self.round > 0 and not p.lockset.has_noquorum:
                self.log('invalid proposal')
                return  # need no quorum to accept new proposal
            assert p.block.validate()
            self.vote(p)
        else:
            assert isinstance(p, VotingInstruction)
            if not p.lockset.has_quorum_possible:
                return  # need no quorum possible to accept VotingInstruction
            self.vote(p)
        self.log('broadcasting proposal')
        self.env.broadcast(p)

    def create_proposal(self):
        """
        proposals are always for a new round
        and need to include a valid LockSet of the last round
        """
        lockset = self.last_valid_lockset
        self.log('in create proposal', lockset=lockset)
        assert lockset.is_valid, "can't create proposal w/o valid lockset"
        assert self.height - 1 == lockset.height and self.round == 0 or \
               self.round - 1 == lockset.round and self.height == lockset.height

        if self.coinbase != self.proposer(self.height, self.round):
            return
        self.log('is proposer', H=self.height, R=self.round)
        if self.round == 0 or lockset.has_noquorum:
            # get quorum which signs prev block
            block = Block(self.coinbase, self.height, self.round, self.head.hash, lockset)
            proposal = BlockProposal(self.signature(), lockset, block)
        elif lockset.has_quorum_possible:
            bh = lockset.has_quorum_possible
            proposal = VotingInstruction(self.signature(), lockset, bh)
        else:
            raise Exception('invalid lockset')

        self.receive_proposal(proposal)


    def vote(self, proposal):
        assert isinstance(proposal, Proposal)
        lockset = proposal.lockset
        self.log('in vote', proposal=proposal)
        assert lockset.is_valid # +2/3 of all votes
        if proposal.signature.height != self.height or proposal.signature.round != self.round:
            self.log('unexpected', proposal=proposal)

        assert self.proposer(self.height, self.round) == proposal.signature.address

        if isinstance(proposal, BlockProposal):
            assert lockset.has_noquorum or lockset.height == self.height - 1
            assert proposal.block.validate()
            self.unlock(lockset)
            self.blocks[proposal.block.hash] = proposal.block
            vote = self.relock(proposal.block.hash)
        else:
            assert isinstance(proposal, VotingInstruction)
            assert lockset.has_quorum_possible
            self.unlock(lockset)
            if isinstance(self.lock, Locked) and self.lock.blockhash == proposal.blockhash:
                # already locked on this block
                pass
            self.unlock(lockset)
            if proposal.blockhash not in self.blocks:
                # don't know this block
                vote = self.relock()
            else:
                vote = self.relock(proposal.blockhash)

        self.receive_vote(vote)  # use same mechanism
