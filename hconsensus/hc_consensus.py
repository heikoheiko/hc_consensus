# Copyright (c) 2015 Heiko Hees
from base import LockSet, LockSetManager, Locked, NotLocked, Message, Vote
from base import EnvironmentBase, Signature, BlockProposal, VotingInstruction
from base import BlockRequest, BlockReply, Block, Proposal, isaddress
from utils import cstr, DEBUG, phx


class ConsensusProtocol(object):

    timeout = 1  # secs
    timeout_round_factor = 1.2

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
        self.messages_received = []
        self.messages_sent = []
        self.votes = []

        self.stopped = False


    # utils

    def __repr__(self):
        return '<CP A:%d H:%d R:%d L:%r>' % (self.coinbase, self.height, self.round, self._lock)

    def log(self, tag, **kargs):
        # if self.coinbase !=0: return
        t = int(self.env.now)
        c = lambda x: cstr(self.coinbase, x)
        msg = ' '.join([str(t), c(repr(self)),  tag, (' %r' % kargs if kargs else '')])
        if self.stopped:
            msg ='X' + msg
        print msg

    @property
    def current_lockset(self):
        return self.locksets.get(self.height, self.round)
    lockset = current_lockset

    @property
    def height(self):
        return self.head.height + 1

    @property
    def hr(self):
        return self.height, self.round

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

    @property
    def is_insync(self):
        # last valid lockset height must not be greater than self.height
        return self.height - self.last_valid_lockset.height <= 1 \
               or not self.last_valid_lockset.height

    # communication

    def broadcast(self, m, force=False):
        self.messages_sent.append(m)
        self.env.broadcast(self, m, force)

    def sync(self, blockhash):
        "request block for blockhash"
        self.log('syncing', bh=phx(blockhash))
        self.broadcast(BlockRequest(blockhash), force=True)

    def receive_message(self, peer, m):
        self.log('receive', msg=m)
        self.messages_received.append(m)
        assert isinstance(m, Message)
        if isinstance(m, Vote):
            self.receive_vote(m)
        elif isinstance(m, Proposal):
            self.receive_proposal(m)
        elif isinstance(m, BlockRequest):
            self.receive_block_request(peer, m)
        elif isinstance(m, BlockReply):
            self.receive_block(m.block)
        else:
            raise Exception('unhandled message')

    def receive_block_request(self, peer, blockrequest):
        blockhash = blockrequest.blockhash
        b = self.blocks.get(blockhash)
        self.log('receive block request', bh=phx(blockhash), found=b)
        if b:
            self.env.send(self, peer, BlockReply(b, id(blockrequest)))  # filter id

    def receive_block(self, blk):
        assert isinstance(blk, Block)
        self.log('receive block', block=blk, known=bool(blk.hash in self.blocks))
        if blk.hash in self.blocks:
            return  # we received the block in the meantime
        assert blk.lockset.has_quorum
        self.blocks[blk.hash] = blk
        self.add_synced_block(blk)

    def sync_highest_known(self):
        # try to sync highest known block with quorum
        # find highest valid lockset
        self.log('in sync highest')
        best = self.head
        for ls in self.locksets:
            if ls.is_valid and ls.has_quorum and ls.height > best.height:
                best = ls
        if isinstance(best, LockSet):
            bh = best.has_quorum
            self.log('highest lockset with quorum', ls=best, bh=phx(bh))
            assert bh != self.head.hash
            if bh not in self.blocks:
                self.sync(bh)

    def add_synced_block(self, blk):
        self.log('adding synced block', block=blk)
        if blk.prevhash not in self.blocks:
            self.sync(blk.prevhash)
            return  # keep syncing

        if blk.height > self.head.height + 1:
            self.add_synced_block(self.blocks[blk.prevhash])  # continue with child block
            return

        # get parent block with lockset
        lockset = None
        for parent in self.blocks.values():  # fix inefficiency
            if parent.prevhash == blk.hash:
                lockset = parent.lockset
                break
        if not lockset:
            self.log('failed to find lockset (from parent block) for requested block')
            # print sorted(b.height for b in self.blocks.values())
            self.sync_highest_known()
            return

        assert lockset.has_quorum
        assert blk.lockset.has_quorum
        assert blk.height == self.head.height + 1
        assert blk.prevhash == self.head.hash,  self.head
        self.commit(lockset, block=blk)

        # try to commit parent
        self.add_synced_block(parent)


    # events

    def on_timeout(self):
        """
        round timed out.
        i.e. we have not received a proposal for this round
        we must not have voted for this round
        """
        lockset = self.last_valid_lockset
        self.log('timeout', expected_from=self.proposer(self.height, self.round),
                 last_valid=lockset)

        if self.height < lockset.height:
            # as long as not in sync abtain from voting, so there is chance to catch up
            return

        # must not be locked in this round (because then timout had been cancled)
        assert not self._lock or self._lock.round < self.round \
               or self._lock <= lockset.hr, self._lock

        #  Q: what is the H,R of nodes which are not in sync?
        #  H: should be based head.height
        #  Vorting instructions should be blindly followed? no!
            # could be that  1/3 is byzantine and the rest is not in sync? so yes!
        # but if syncing: actually voting should be deferred until we are in sync
        # consider to rebroadcast votes
        # but actually Timeouts should only be started after we a new lockset

        self.relock(lockset)


    def on_valid_lockset(self, lockset):
        assert lockset.is_valid
        if lockset.processed:
            return
        lockset.processed = True
        self.log('on valid lockset', lockset=lockset, last_valid=self.last_valid_lockset)

        lv = self.last_valid_lockset
        if lockset.height > lv.height or lockset.height == lv.height and lockset.round > lv.round:
            assert lockset.height >= self.last_valid_lockset.height
            assert lockset.round > self.last_valid_lockset.round or \
                   lockset.height > self.last_valid_lockset.height
            self.last_valid_lockset = lockset

        if lockset.hr >= self.hr:
            # receiving valid locksets synchronizes the nodes
            # prepare timeout
            self.log('resetting timeout')
            assert self.is_insync
            self.env.cancel_timeout()
            # set longer timeout for higher rounds
            if not self.stopped:
                timeout = self.timeout * self.timeout_round_factor ** lv.round
                self.env.start_timeout(timeout, self.on_timeout)
            else:
                self.log('stopped, not setting new timeouts')

        # commit if possible
        bh = lockset.has_quorum
        if bh:
            success = self.commit(lockset, bh)
            if success:
                assert self.hr >= lockset.hr
        elif lockset.height == self.height and lockset.round >= self.round:
            assert not lockset.has_quorum
            self.new_round(lockset.round + 1)
        elif self.last_valid_lockset.height > self.height:
            self.sync_highest_known()

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
        self.round = 0
        self.new_height()
        return True


    def start(self):
        self.log('starting')
        self.create_proposal()

    def new_height(self):
        self.log('in new height')
        self._lock = None  # None, NotLocked or Locked
        # self.locksets.cleanup()
        if self.last_valid_lockset.height == self.height:
            # sync to round
            self.new_round(self.last_valid_lockset.round + 1)
        else:
            self.new_round(0)

    def new_round(self, round=None):
        if round is not None:
            assert round >= self.round or round == 0, round
            self.round = round
        else:
            self.round += 1
        self.log('new round')


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
        self.log('in relock', lockset=lockset, proposal=proposal)
        self.log('', last_valid=self.last_valid_lockset, in_sync=self.is_insync)
        assert isinstance(lockset, LockSet)
        assert lockset.is_valid

        if proposal and proposal.height > self.height:
            self.log('proposal from future height, skipping')
            return

        if not self._lock:  # check we never double voted
            for v in self.votes:
                assert not (self.height, self.round) == (v.height, v.round)

        # we must not have voted in this round
        if self._lock and self._lock.height == self.height and lockset.round == self.round:
            self.log('already voted in this round')
            return


        # from receive proposal
        if isinstance(proposal, Proposal):
            # exit if locked on same round or propsal from the past
            if self._lock:
                if self._lock.height > proposal.height:
                    return
                if self._lock.height == proposal.height and self._lock.round >= proposal.round:
                    return
            else:
                pass

            # assert the lockset is from the round before proposal
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
            assert lock.height == proposal.height and lock.round == proposal.round, lock
        else:
            assert not proposal
            if self._lock:
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
            self.log('plan to lock on', lock=lock)
            assert lock.height == lockset.height and lock.round == lockset.round + 1 or \
                   lock.height == lockset.height + 1 and lock.round == 0

        assert isinstance(lock, (Locked, NotLocked))
        self.votes.append(lock)
        if self.coinbase == 5:
            print self.votes

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
        self.broadcast(v)

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

        if p.round == 0:
            assert p.lockset.has_quorum, p.lockset  # need quorum on last round

        # learn votes, eventually commits
        self.locksets.update(p.lockset)
        _ls = self.locksets.get(p.lockset.height, p.lockset.round)
        assert _ls.is_valid
        self.log('updated logset', processed=_ls.processed, ls=_ls)
        self.on_valid_lockset(_ls)

        # check if the proposal is up to date
        H, R = p.height, p.round
        if H < self.height or (H == self.height and R < self.round):
            self.log('proposal from the past')
            return  # is not propagating sensible?

        # blindly follow voting instructions (fixme if not fixed set of validators)
        if isinstance(p, VotingInstruction):
            self.log('following voting instruction')
            assert p.blockhash == p.lockset.has_quorum_possible
        else:
            assert isinstance(p, BlockProposal)
            if H > self.height + 1:
                self.log('not in sync', lockset=p.lockset, proposal=p, pheight=p.height)
                self.sync(p.block.prevhash)  # can not validate
                return
            if not self.validate_block(p.block):
                self.log('can not validate block')
                return  # can't vote if unable to validate
            if self.round > 0 and not p.lockset.has_noquorum:
                self.log('invalid proposal', plockset=p.lockset, hasquorum=bool(p.lockset.has_quorum), pround=p.round)
                return  # need no_quorum to accept new proposal on round > 0

        self.vote(p)
        self.log('forwarding proposal')
        self.broadcast(p)

    def create_proposal(self):
        """
        proposals are always for a new round
        and need to include a valid LockSet of the last round
        """
        if self.stopped:
            self.log('stopped not creating proposal')
            return
        lockset = self.last_valid_lockset
        self.log('in create proposal', lockset=lockset,
                 is_proposer=self.proposer(self.height, self.round))
        assert lockset.is_valid, "can't create proposal w/o valid lockset"
        if self.coinbase != self.proposer(self.height, self.round):
            return

        if self.height < lockset.height:
            self.log('not in sync')
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

