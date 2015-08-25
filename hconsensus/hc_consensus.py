# Copyright (c) 2015 Heiko Hees
from base import LockSet, LockSetManager, Locked, NotLocked, Message, Vote
from base import EnvironmentBase, Signature, BlockProposal, VotingInstruction
from base import BlockRequest, BlockReply, Block, Proposal, isaddress
from utils import cstr, DEBUG, phx


class ConsensusProtocol(object):

    timeout = timeout_base = 1  # secs
    timeout_inc_factor = 1.5


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
        self.lockset          # the lockset for the current round which eventually commits on H
        self.head.lockset     # the lockset which commits H-2 in Block H-1
        self.last_valid_lockset = head.lockset  # lockset, that ended the last round (fake here)
        self.last_committing_lockset = head.lockset  # lockset, that agreed on a block (fake here)

        # debug
        self.messages = []



    # utils

    def __repr__(self):
        return '<CP A:%d H:%d R:%d L:%r>' % (self.coinbase, self.height, self.round, self._lock)

    def log(self, tag, **kargs):
        c = lambda x: cstr(self.coinbase, x)
        msg = ' '.join([c(repr(self)),  tag, (' %r' % kargs if kargs else '')])
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

    def validate_block(self, blk):
        if blk.prevhash not in self.blocks:
            self.log('can not validate block w/o parent')
            return
        if blk.height - 1 > self.head.height:
            self.log('can not validate block, not in sync')
            return
        return blk.validate()



    # communication

    def sync(self, blockhash):
        "request block for blockhash"
        self.log('syncing', bh=phx(blockhash))
        self.env.broadcast(self, BlockRequest(blockhash))

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
        b = self.blocks.get(blockhash)
        self.log('receive block request', bh=phx(blockhash), found=b)
        if b:
            self.env.send(self, peer, BlockReply(b))

    def receive_block(self, blk):
        assert isinstance(blk, Block)
        self.log('receive block', block=blk, known=bool(blk.hash in self.blocks))
        if blk.hash in self.blocks:
            return  # we received the block in the meantime
        self.blocks[blk.hash] = blk
        lockset = None
        # get parent block for lockset, fix inefficiency
        for block in self.blocks.values():
            if block.prevhash == blk.hash:
                lockset = block.lockset
        if not lockset:
            self.log('failed to find lockset (from parent block) for requested block')
            return
        assert lockset.has_quorum
        assert blk.lockset.has_quorum
        assert blk.height > self.height

        if blk.prevhash not in self.blocks:
            self.sync(blk.prevhash)
        else:
            assert blk.prevhash == self.head.hash
            self.commit(lockset, block=blk)

    # events

    def on_timeout(self):
        """
        round timed out.
        i.e. we have not received a proposal for this round
        we must not have voted for this round
        """

        self.log('timeout', expected_from=self.proposer(self.height, self.round))

        lockset = self.last_valid_lockset
        if self.height < lockset.height:
            # as long as not in sync abtain from voting, so there is chance to catch up
            return

        # must not be locked in this round (because then timout had been cancled)
        assert not self._lock or self._lock.round < self.round, self._lock

        self.relock(lockset)


    def on_valid_lockset(self, lockset):
        assert lockset.is_valid
        if lockset.processed:
            return
        lockset.processed = True
        self.log('on valid lockset', lockset=lockset)

        lv = self.last_valid_lockset
        if lockset.height > lv.height or lockset.height == lv.height and lockset.round > lv.round:
            self.last_valid_lockset = lockset

        # commit if possible
        bh = lockset.has_quorum
        if bh:
            success = self.commit(lockset, bh)
        elif lockset.round >= self.round:
            self.new_round(lockset.round + 1)

    # steps

    def commit(self, lockset, bh=None, block=None):
        if lockset.height == 0:
            return  # don't commit genesis
        if block:
            assert isinstance(block, Block)
            bh = block.hash
            self.blocks[bh] = block
        self.log('in commit', bh=phx(bh))
        if bh not in self.blocks:
            self.sync(bh)
            return
        blk = self.blocks[bh]
        self.log('know block', block=blk)
        if blk.height > self.height:
            self.sync(blk.prevhash)
            return
        if blk.height < self.height:
            self.log('already committed', block=blk)
            return
        assert blk.height == self.height
        assert blk.prevhash in self.blocks
        assert blk.prevhash == self.head.hash
        assert self.validate_block(blk)
        self.head = blk
        self.last_committing_lockset = lockset
        self.new_height()
        return True


    def start(self):
        self.log('starting')
        self.create_proposal()

    def new_height(self):
        self._lock = None  # None, NotLocked or Locked
        self.timeout = self.timeout_base  # reset timeout
        # self.locksets.cleanup()
        self.round = -1  # will be incremented in new_round
        self.new_round()

    def new_round(self, round=None):
        if round is not None:
            assert round >= self.round or round == 0, round
            self.round = round
        else:
            self.round += 1
        self.log('new round')

        # prepare timeout
        self.env.cancel_timeout()
        self.env.start_timeout(self.timeout, self.on_timeout)
        # set longer timeout for next round, reset on new height
        self.timeout *= self.timeout_inc_factor

        # we might need to propose
        self.create_proposal()

    # locks
    def relock(self, lockset, proposal=None):
        """
        if proposal:
            if locked on same round:
                return
            if voting instruction:
                unlock / lock (even if we don't know the block yet, cause +1/3)
            if blockproposal
                relock last vote
        else:
            if locked:
                relock last vote
            else:
                lock NotLocked
        """
        assert isinstance(lockset, LockSet)
        assert lockset.is_valid

        # from receive proposal
        if isinstance(proposal, Proposal):
            # exit if locked on same round or propsal from the past
            if self._lock:
                if self._lock.height > proposal.height:
                    return
                if self._lock.height == proposal.height and self._lock.round >= proposal.round:
                    return

            # assert the lockset is from the round before proposal
            assert lockset
            assert lockset.height == proposal.height - 1 or lockset.round == proposal.round - 1

            if isinstance(proposal, BlockProposal):
                assert lockset.has_noquorum or lockset.height == self.height - 1
                assert proposal.block.prevhash == self.head.hash
                assert self.validate_block(proposal.block)
                self.blocks[proposal.block.hash] = proposal.block
                if self._lock and isinstance(self._lock, Locked):
                    # repeat vote if we already voted on a block
                    lock = Locked(self.signature(), self._lock.blockhash)
                else:
                    lock = Locked(self.signature(), proposal.block.hash)
            else:  # voting instructions require compliance
                assert isinstance(proposal, VotingInstruction)
                assert lockset.has_quorum_possible
                lock = Locked(self.signature(), proposal.blockhash)
        else:
            assert not proposal
            if self._lock:
                # we must not have voted in this round
                assert self._lock.height < self.height or lockset.round < self.round

                # lockset must be from previous round
                assert lockset.height == self.height - 1 and self.round == 0 or \
                       lockset.round == self.round - 1, lockset

                # repeat last vote
                if isinstance(self._lock, Locked):
                    lock = Locked(self.signature(), self._lock.blockhash)
                else:
                    lock = NotLocked(self.signature())
            else:
                lock = NotLocked(self.signature())

        assert isinstance(lock, (Locked, NotLocked))
        self._lock = lock
        self.log('locked')
        self.env.cancel_timeout() # we send vote for this round, don't expect a timeout
        self.receive_vote(lock)  # implicit broadcast



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
        self.env.broadcast(self, v)

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
        if not self.proposer(p.height, p.round) == p.signature.address:
            self.log('wrong proposer')
            assert False
            return

        # learn votes, eventually commits
        self.locksets.update(p.lockset)
        _ls = self.locksets.get(p.lockset.height, p.lockset.round)
        assert _ls.is_valid
        self.log('updated logset', processed=_ls.processed)
        self.on_valid_lockset(_ls)

        # check if the proposal is up to date
        H, R = p.height, p.round
        if H < self.height or (H == self.height and R < self.round):
            self.log('proposal from the past')
            return  # is not propagating sensible?

        # blindly follow voting instructions (fixme if not fixed set of validators)
        if isinstance(p, VotingInstruction):
            self.log('following voting instruction')
            assert p.lockset.has_quorum_possible
        else:
            assert isinstance(p, BlockProposal)
            if H > self.height + 1:
                self.log('not in sync', lockset=p.lockset, proposal=p, pheight=p.height)
                self.sync(p.block.prevhash)  # can not validate
                return
            if self.round > 0 and not p.lockset.has_noquorum:
                self.log('invalid proposal')
                return  # need no quorum to accept new proposal
            if not self.validate_block(p.block):
                self.log('can not validate block')
                return  # can't vote if unable to validate
        self.vote(p)
        self.log('forwarding proposal')
        self.env.broadcast(self, p)

    def create_proposal(self):
        """
        proposals are always for a new round
        and need to include a valid LockSet of the last round
        """
        lockset = self.last_valid_lockset
        self.log('in create proposal', lockset=lockset,
                 is_proposer=self.proposer(self.height, self.round))
        assert lockset.is_valid, "can't create proposal w/o valid lockset"
        if self.coinbase != self.proposer(self.height, self.round):
            return

        assert self.height - 1 == lockset.height and self.round == 0 or \
               self.round - 1 == lockset.round and self.height == lockset.height, self

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
        self.log('created proposal', p=proposal)
        self.receive_proposal(proposal)  # implicit broadcast


    def vote(self, proposal):
        assert isinstance(proposal, Proposal)
        lockset = proposal.lockset
        self.log('in vote', proposal=proposal)
        assert lockset.is_valid  # +2/3 of all votes
        if proposal.height != self.height or proposal.round != self.round:
            self.log('unexpected', proposal=proposal)

        self.relock(lockset, proposal)

