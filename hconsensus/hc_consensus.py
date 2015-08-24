# Copyright (c) 2015 Heiko Hees
from base import LockSet, LockSetManager, Locked, NotLocked, Message, Vote
from base import EnvironmentBase, Signature, BlockProposal, VotingInstruction
from base import BlockRequest, BlockReply, Block, Proposal, isaddress
from utils import cstr, DEBUG


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
        self._lock = None
        self.round = 0
        self.height  # is a property, see below


        # locksets
        self.last_valid_lockset = head.lockset # lockset, that ended the last round
        # self.lockset          # the lockset for the current round
        self.last_committing_lockset = head.lockset # lockset, that agreed on a block

        # debug
        self.messages = []



    # utils

    def __repr__(self):
        return '<CP H:%d R:%d L:%r>' % (self.height, self.round, self._lock)

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

    def get_block(self, height):
        if height > self.head.height:
            return
        blk = self.head
        while blk.height > height:
            blk = self.blocks[blk.prevhash]
        assert blk.height == height
        return blk

    # locks
    def relock(self, lockset=None, blockhash=None):
        if lockset:
            # assert the lockset is from the last round
            assert isinstance(lockset, LockSet)
            assert lockset.is_valid
            assert lockset.height == self.height - 1 or lockset.round == self.round - 1
        else:
            assert not self._lock

        # unlock
        if self._lock:
            # we must not have voted in this round
            assert self._lock.height < self.height \
                or lockset.round < self.round
            self.log('unlocking', lock=self._lock)
            self._lock = None

        # lock
        assert not self._lock
        if blockhash:
            lock = Locked(self.signature(), blockhash)
        else:
            lock = NotLocked(self.signature())
        self._lock = lock
        self.log('locked')
        self.env.cancel_timeout()
        return lock

    # communication

    def sync(self, blockhash):
        "request block for blockhash"
        self.log('syncing', bh=blockhash)
        self.env.sendany(BlockRequest(self.signature(), blockhash))

    def receive_message(self, peer, m):
        self.log('receive', msg=m)
        self.messages.append(m)
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
        assert False # broken, needs lockset
        assert blk.lockset.has_quorum_possible
        if blk.height <= self.height:
            return
        if blk.prevhash not in self.blocks:
            self.sync(blk.prevhash)
        else:
            assert blk.prevhash == self.head.hash
            assert isinstance(blk, Block)
            self.commit(lockset, block=blk)

    # events

    def on_timeout(self):
        """
        round timed out.
        i.e. we have not received a proposal for this round
        we must not have voted for this round
        """

        self.log('timeout', expected_from=self.proposer(self.height, self.round))
        if self._lock:
            assert self._lock.round < self.round, self._lock

        lockset = self.last_valid_lockset
        if self._lock and isinstance(self._lock, Locked):
            self.relock(lockset, self._lock.blockhash)
        else:
            self.relock(lockset)
        self.receive_vote(self._lock)  # implicit broadcast

    def on_valid_lockset(self, lockset):
        assert lockset.is_valid
        if lockset.processed:
            return
        lockset.processed = True
        self.log('valid lockset', lockset=lockset)

        if lockset.height != self.height:
            assert lockset.height < self.height
            return
        self.last_valid_lockset = lockset

        # commit if possible
        bh = lockset.has_quorum
        if bh:
            self.commit(lockset, bh)
            assert self.round == 0
        else:
            self.new_round(lockset.round + 1)

    # steps

    def start(self):
        self.log('starting')
        self.create_proposal()

    def new_height(self):
        self._lock = None  # None, NotLocked or Locked
        # self.locksets.cleanup()
        self.round = -1  # will be incremented in new_round
        self.new_round()

    def new_round(self, round=None):
        if round is not None:
            self.round = round
        else:
            self.round += 1
        self.log('new round')
        self.env.cancel_timeout()
        self.env.start_timeout(self.timeout, self.on_timeout)  # implicitly cancels old timeout
        # we might need to propose
        self.create_proposal()

    def commit(self, lockset, bh=None, block=None):
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
        self.last_committing_lockset = lockset
        self.new_height()

    def receive_vote(self, v):
        self.log('receive vote', v=v)
        # ignore votes on older heights and rounds
        H, R = v.height, v.round
        if H < self.height:
            return
        if H == self.height and R < self.round:
            return

        # broadcast
        self.log('forwarding vote')
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
        H, R = p.height, p.round
        if H < self.height:
            self.log('wrong height')
            return
        if R < self.round:
            self.log('wrong round')
            return
        if H > self.height + 1:
            self.log('not in sync', lockset=p.lockset, proposal=p, pheight=p.height)
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
        self.log('forwarding proposal')
        self.env.broadcast(p)

    def create_proposal(self):
        """
        proposals are always for a new round
        and need to include a valid LockSet of the last round
        """
        lockset = self.last_valid_lockset
        self.log('in create proposal', lockset=lockset,
                 is_proposer=self.proposer(self.height, self.round))
        assert lockset.is_valid, "can't create proposal w/o valid lockset"
        assert self.height - 1 == lockset.height and self.round == 0 or \
               self.round - 1 == lockset.round and self.height == lockset.height

        if self.coinbase != self.proposer(self.height, self.round):
            return
        self.log('is proposer', H=self.height, R=self.round)
        if self.round == 0 or lockset.has_noquorum:
            # get quorum which signs prev block
            cl = self.last_committing_lockset
            block = Block(self.coinbase, self.height, self.round, self.head.hash, cl)
            proposal = BlockProposal(self.signature(), lockset, block)
        elif lockset.has_quorum_possible:
            bh = lockset.has_quorum_possible
            proposal = VotingInstruction(self.signature(), lockset, bh)
        else:
            raise Exception('invalid lockset')

        self.receive_proposal(proposal)  # implicit broadcast


    def vote(self, proposal):
        assert isinstance(proposal, Proposal)
        lockset = proposal.lockset
        self.log('in vote', proposal=proposal)
        assert lockset.is_valid # +2/3 of all votes
        if proposal.height != self.height or proposal.round != self.round:
            self.log('unexpected', proposal=proposal)

        assert self.proposer(self.height, self.round) == proposal.signature.address

        if isinstance(proposal, BlockProposal):
            assert lockset.has_noquorum or lockset.height == self.height - 1
            assert proposal.block.validate()
            self.blocks[proposal.block.hash] = proposal.block
            vote = self.relock(lockset, blockhash=proposal.block.hash)
        else:
            assert isinstance(proposal, VotingInstruction)
            assert lockset.has_quorum_possible
            if isinstance(self._lock, Locked) and self._lock.blockhash == proposal.blockhash:
                # already locked on this block
                pass
            if proposal.blockhash not in self.blocks:
                # don't know this block
                vote = self.relock(lockset)
            else:
                vote = self.relock(lockset, blockhash=proposal.blockhash)

        self.receive_vote(vote)  # implicit broadcast
