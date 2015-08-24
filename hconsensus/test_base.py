import pytest
from base import Signature, Block, mk_genesis, sha3, ishash, BlockProposal
from base import LockSet, Locked, NotLocked, SignedMessage, Vote

eligible_votes = 10
LockSet.eligible_votes = eligible_votes


def test_Signature():
    s1 = Signature(1, 2, 3)
    s2 = Signature(1, 2, 3)
    s3 = Signature(1, 3, 3)
    assert s1 == s2
    assert s1.hash == s2.hash
    assert s1 != s3

    assert s1.address == 1
    assert s1.height == 2
    assert s2.round == 3


def test_Block():
    genesis = mk_genesis(range(10))
    b1 = Block(1, 2, 3, genesis.hash, genesis.lockset)
    b2 = Block(1, 2, 3, genesis.hash, genesis.lockset)
    assert b1 == b2


def test_Message():
    m = SignedMessage(Signature(1, 2, 3))
    m2 = SignedMessage(Signature(1, 2, 3))
    assert m == m2

def test_Vote():
    v = Vote(Signature(1, 2, 3))
    v2 = Vote(Signature(1, 2, 3))
    assert v == v2

def test_LockSet():
    ls = LockSet()
    assert not ls
    assert len(ls) == 0
    v1 = Locked(Signature(1, 2, 3), '0'*32)
    v2 = Locked(Signature(1, 2, 3), '0'*32)
    ls.add(v1)
    assert ls
    assert len(ls) == 1
    lsh = ls.hash
    ls.add(v1)
    assert lsh == ls.hash
    assert len(ls) == 1
    ls.add(v2)
    assert lsh == ls.hash
    assert len(ls) == 1



def test_LockSet_isvalid():
    ls = LockSet()
    votes = [Locked(Signature(i, 2, 3), '0'*32) for i in range(ls.eligible_votes)]
    for i, v in enumerate(votes):
        ls.add(v)
        assert len(ls) == i + 1
        if len(ls) < ls.eligible_votes * 2/3.:
            assert not ls.is_valid
        else:
            assert ls.is_valid
            assert ls.has_quorum  # same blockhash


def test_LockSet_quorums():
    combinations = dict(has_quorum=[
                                [1]*7,
                                [1]*7 + [2]*3,
                                [1]*7 + [None]*3,
                                ],
                        has_noquorum=[
                                [1]*3 + [2]*3 + [None],
                                [None] * 7,
                                [None] * 10,
                                range(10),
                                range(7)
                                ],
                        has_quorum_possible=[
                                [1] * 4 + [None]*3,
                                [1] * 4 + [2]*4,
                                [1] * 4 + [2]*3 + [3]*3,
                                [1] * 6 + [2]
                            ])

    r, h = 1, 2
    hash_ = lambda v: sha3(str(v))

    for method, permutations in combinations.items():
        for set_ in permutations:
            assert len(set_) >= 7
            ls = LockSet()
            for address, p in enumerate(set_):
                if p is not None:
                    ls.add(Locked(Signature(address, h, r), hash_(p)))
                else:
                    ls.add(NotLocked(Signature(address, h, r)))
            assert len(ls) >= 7
            assert getattr(ls, method)

            # check stable sort
            bhs = ls.blockhashes()
            if len(bhs) > 1:
                assert ishash(bhs[0][0])
                assert isinstance(bhs[0][1], int)
                if bhs[0][1] == bhs[1][1]:
                    assert bhs[0][0] > bhs[1][0]
                else:
                    assert bhs[0][1] > bhs[1][1]

def test_BlockProposal():
    ls = LockSet()
    for i in range(10):
        s = Signature(i, 2, 3)
        ls.add(Locked(s, '0'*32))
    assert len(ls) == 10
    assert ls.has_quorum
    block = Block(1, 2, 3, '0'*32, ls)
    p = BlockProposal(Signature(1, 2, 3), ls, block)
    p2 = BlockProposal(Signature(1, 2, 3), ls, block)
    assert p == p2


