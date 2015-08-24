import pytest
from hc_consensus import Signature, Block, genesis, sha3, ishash
from hc_consensus import LockSet, Locked, NotLocked, Message, Vote

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
    b1 = Block(1, 2, 3, genesis.hash, LockSet())
    b2 = Block(1, 2, 3, genesis.hash, LockSet())
    assert b1 == b2


def test_Message():
    m = Message(Signature(1, 2, 3))

def test_Vote():
    v = Vote(Signature(1, 2, 3))


def test_LockSet():
    ls = LockSet()
    assert not ls
    assert len(ls) == 0
    lsh = ls.hash
    v1 = Locked(Signature(1, 2, 3), lsh)
    v2 = Locked(Signature(1, 2, 3), ls.hash)
    ls.add(v1)
    assert ls
    assert len(ls) == 1
    assert lsh != ls.hash
    lsh = ls.hash
    ls.add(v1)
    assert lsh == ls.hash
    assert len(ls) == 1


def test_LockSet_isvalid():
    ls = LockSet()
    bh = ls.hash
    votes = [Locked(Signature(i, 2, 3), bh) for i in range(ls.eligible_votes)]
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




