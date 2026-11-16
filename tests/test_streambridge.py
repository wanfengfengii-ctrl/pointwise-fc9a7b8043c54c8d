import hashlib
import json
import unittest

from archive import NewArchive
from source_sdk import SourceSDK
from streambridge import Pump


def digest_of(bodies):
    return hashlib.sha256(json.dumps(bodies, ensure_ascii=False,
                                     separators=(',', ':')).encode()).hexdigest()


def rows(first, bodies):
    return [dict(offset=first + i, body=b) for i, b in enumerate(bodies)]


class PumpTests(unittest.TestCase):
    def make(self, partitions=('a', 'b'), auto_accept=False):
        self.s = SourceSDK({p: 'owner-' + p for p in partitions},
                           auto_accept=auto_accept)
        self.a = NewArchive()
        self.p = Pump(self.s, self.a)
        return self.p

    def owned(self, partition):
        return {item['partition']: item for item in self.s.owned()}[partition]

    def deliver(self, partition, batch, bodies, first=0, parts=1, order=None):
        request = self.owned(partition)['fetch_request']
        self.s.deliver_batch(request, batch, rows(first, bodies), parts=parts,
                             order=order)
        self.s.finish_fetch(request)
        return request

    def drain(self, limit=50):
        for _ in range(limit):
            if not self.p.step():
                return
        self.fail('pump did not become idle')

    # ------------------------------------------------------------------
    def test_fragmented_batches_commit_only_contiguous_frontier(self):
        self.make()
        self.assertTrue(self.p.step())            # fetch both partitions
        self.assertIsNotNone(self.owned('a')['fetch_request'])
        self.assertIsNotNone(self.owned('b')['fetch_request'])

        # Deliver the later batch first, then the first one, with duplicated
        # and reordered fragments.  The gap must not be acknowledged.
        request = self.owned('a')['fetch_request']
        self.s.deliver_batch(request, 'later', rows(2, ['c', 'd']), parts=2,
                             order=[1, 0, 1])
        self.p.step()
        self.assertEqual(self.a.read('a'),
                         [dict(offset=2, body='c'), dict(offset=3, body='d')])
        self.assertEqual(self.a.progress(), [dict(partition='a', owner='owner-a',
                                                 offset=-1)])
        self.assertEqual(self.owned('a')['commit_request'], None)
        self.s.deliver_batch(request, 'first', rows(0, ['a', 'b']))
        self.s.finish_fetch(request)
        self.p.step()

        # One commit for the contiguous frontier, and no more.
        commit = self.owned('a')['commit_request']
        self.assertIsNotNone(commit)
        self.assertEqual(self.s.check(commit),
                         dict(state='pending', partition='a', owner='owner-a',
                              offset=3))

    def test_archive_frontier_stops_at_a_gap_and_never_reports_max(self):
        a = NewArchive()
        for first, bodies in ((0, ['a', 'b']), (4, ['e', 'f'])):
            a.stage('p', 'o', 'b%d' % first, first, 2, 0, 1,
                    [dict(offset=first + i, body=b) for i, b in enumerate(bodies)],
                    digest_of(bodies))
            self.assertTrue(a.complete('p', 'o', 'b%d' % first))
        self.assertEqual(a.progress(), [dict(partition='p', owner='o', offset=1)])
        self.assertEqual(a.read('p'), [dict(offset=0, body='a'),
                                       dict(offset=1, body='b'),
                                       dict(offset=4, body='e'),
                                       dict(offset=5, body='f')])

    def test_commit_reply_lost_is_confirmed_and_raw_rows_released(self):
        self.make(auto_accept=False)
        self.p.step()
        self.deliver('a', 'batch', ['温度', 'flow'])
        self.p.step()
        commit = self.owned('a')['commit_request']
        self.s.settle(commit, reply=False)        # accepted, reply lost
        self.assertTrue(self.p.step())
        self.assertEqual(self.owned('a')['committed'], 1)
        self.assertEqual(self.s.check(commit)['state'], 'accepted')
        self.assertEqual(self.s.retained_offsets('a', 'owner-a'), [])
        self.assertEqual(self.a.read('a'),
                         [dict(offset=0, body='温度'), dict(offset=1, body='flow')])
        # Same request; nothing new needs confirming.
        self.assertEqual(self.owned('a')['commit_request'], commit)
        self.p.step()
        self.assertEqual(self.owned('a')['commit_request'], commit)

    def test_archive_before_commit_strict_order(self):
        """No commit request while the batch is still incomplete."""
        self.make()
        self.p.step()
        request = self.owned('a')['fetch_request']
        self.s.deliver_batch(request, 'b', rows(0, ['x', 'y']), parts=2)
        # Simulate a fragment that never reaches the consumer: only part 0 is
        # replayed to the source event stream, then the fetch completes.
        events = self.s.poll_events()
        partial = [e for e in events if e['kind'] == 'fragment' and e['part'] == 0]
        done = [e for e in events if e['kind'] == 'fetch_done']
        self.assertEqual(len(partial), 1)
        self.s.replay_events(partial + done)
        self.p.step()
        self.assertEqual(self.a.pending(), [dict(partition='a', owner='owner-a',
                                                 batch='b', first=0, count=2)])
        self.assertEqual(self.a.read('a'), [])
        self.assertEqual(self.owned('a')['commit_request'], None)
        self.assertEqual(self.owned('a')['committed'], -1)

    def test_revoke_completes_inflight_then_releases_without_old_owner(self):
        self.make()
        self.p.step()
        request = self.owned('a')['fetch_request']
        self.s.deliver_batch(request, 'batch', rows(0, ['one', 'two']))
        self.s.begin_revoke('a', 'owner-a')
        self.p.step()
        # The outstanding fetch is still finished, archived and confirmed.
        self.s.finish_fetch(request)
        self.p.step()
        commit = self.owned('a')['commit_request']
        self.assertIsNotNone(commit)
        self.s.settle(commit)
        self.p.step()
        release = self.owned('a')['release_request']
        self.assertIsNotNone(release)
        self.s.settle(release, reply=False)       # accepted, reply lost
        self.p.step()
        self.assertNotIn('a', [x['partition'] for x in self.s.owned()])
        self.assertEqual({x['partition']: x['owner'] for x in self.s.owned()},
                         {'b': 'owner-b'})
        self.assertEqual(self.a.progress(), [])
        self.assertEqual(self.a.pending(), [])
        self.assertEqual(self.a.read('a'), [dict(offset=0, body='one'),
                                             dict(offset=1, body='two')])

        # A new owner starts clean; the old generation's events are ignored.
        self.s.assign('a', 'owner-a2')
        self.s.replay_events([dict(kind='fragment', request='request-old',
                                   partition='a', owner='owner-a', batch='late',
                                   first=0, count=1, part=0, parts=1,
                                   rows=[dict(offset=0, body='stale')],
                                   digest=digest_of(['stale'])),
                              dict(kind='begin_revoke', partition='a',
                                   owner='owner-a')])
        before = {x['partition']: x for x in self.s.owned()}
        self.p.step()
        owned_now = {x['partition']: x for x in self.s.owned()}
        self.assertEqual(owned_now['b'], before['b'])
        self.assertEqual(owned_now['a']['owner'], 'owner-a2')
        self.assertEqual(owned_now['a']['committed'], -1)
        self.assertEqual(self.a.read('a'), [dict(offset=0, body='one'),
                                            dict(offset=1, body='two')])
        self.assertIsNotNone(self.owned('a')['fetch_request'])

    def test_restart_resumes_partial_stage_without_refetch(self):
        self.make()
        self.p.step()
        request = self.owned('a')['fetch_request']
        # A previous Pump life staged half the batch before it was dropped.
        bodies = ['r0', 'r1', 'r2', 'r3']
        self.a.stage('a', 'owner-a', 'batch', 0, 4, 0, 2,
                     [dict(offset=0, body='r0'), dict(offset=2, body='r2')],
                     digest_of(bodies))
        self.assertEqual(self.a.read('a'), [])

        del self.p
        self.p = Pump(self.s, self.a)
        # Nothing observable is available: the fetch is still outstanding.
        self.assertFalse(self.p.step())
        self.assertEqual(self.owned('a')['fetch_request'], request)

        self.s.deliver_batch(request, 'batch',
                             rows(0, bodies), parts=2, order=[1, 0, 1])
        self.s.finish_fetch(request)
        self.p.step()
        self.assertEqual(self.a.read('a'),
                         [dict(offset=i, body=b) for i, b in enumerate(bodies)])
        self.assertEqual(self.a.pending(), [])
        self.assertIsNotNone(self.owned('a')['commit_request'])

    def test_restart_after_lost_commit_reply_does_not_replay_side_effects(self):
        self.make()
        self.p.step()
        self.deliver('a', 'batch', ['x', 'y'])
        self.p.step()
        commit = self.owned('a')['commit_request']
        self.s.settle(commit, reply=False)
        # The reply is lost and the old Pump disappears before observing it.
        del self.p
        self.p = Pump(self.s, self.a)
        self.p.step()
        self.assertEqual(self.s.check(commit)['state'], 'accepted')
        self.assertEqual(self.owned('a')['committed'], 1)
        self.assertEqual(self.owned('a')['commit_request'], commit)
        self.assertEqual(self.s.retained_offsets('a', 'owner-a'), [])
        self.assertEqual([x['offset'] for x in self.a.read('a')], [0, 1])
        # A fresh fetch is fine; a fresh commit is not needed.
        self.assertIsNotNone(self.owned('a')['fetch_request'])
        self.p.step()
        self.assertEqual(self.owned('a')['commit_request'], commit)

    def test_restart_with_revoke_and_lost_release_reply(self):
        self.make()
        self.p.step()
        self.deliver('a', 'batch', ['x'])
        self.p.step()
        self.s.settle(self.owned('a')['commit_request'])
        self.p.step()
        # An empty follow-up fetch keeps the partition busy; it must be
        # finished before release is eligible.
        follow_up = self.owned('a')['fetch_request']
        self.s.finish_fetch(follow_up)
        self.s.begin_revoke('a', 'owner-a')
        self.p.step()
        release = self.owned('a')['release_request']
        self.assertIsNotNone(release)
        self.s.settle(release, reply=False)   # accepted, reply lost
        del self.p
        self.p = Pump(self.s, self.a)
        self.p.step()
        self.assertEqual({x['partition']: x['owner'] for x in self.s.owned()},
                         {'b': 'owner-b'})
        self.assertEqual(self.a.read('a'), [dict(offset=0, body='x')])
        self.assertEqual(self.a.progress(), [])

    def test_lost_commit_and_release_replies_then_new_owner_generation(self):
        self.make()
        self.p.step()
        self.deliver('a', 'batch', ['x', 'y'])
        self.p.step()
        # Commit is accepted but both reply and the following release reply
        # are lost; the process dies before noticing either.
        self.s.settle(self.owned('a')['commit_request'], reply=False)
        self.p.step()
        self.assertEqual(self.owned('a')['committed'], 1)
        self.s.begin_revoke('a', 'owner-a')
        self.p.step()
        self.s.finish_fetch(self.owned('a')['fetch_request'])
        self.p.step()
        self.s.settle(self.owned('a')['release_request'], reply=False)
        del self.p

        # The operator hands the partition to a new generation.
        self.s.assign('a', 'owner-a2')
        self.p = Pump(self.s, self.a)
        self.p.step()
        self.assertIsNone(self.owned('a')['commit_request'])
        self.assertEqual(self.owned('a')['committed'], -1)
        self.assertEqual(self.owned('a')['owner'], 'owner-a2')
        self.assertEqual(self.a.progress(), [])
        self.assertEqual(self.a.read('a'),
                         [dict(offset=0, body='x'), dict(offset=1, body='y')])
        # The new generation confirms only its own newly archived data.
        request = self.owned('a')['fetch_request']
        self.s.deliver_batch(request, 'a2-0', rows(0, ['z']))
        self.s.finish_fetch(request)
        self.p.step()
        commit = self.owned('a')['commit_request']
        self.assertIsNotNone(commit)
        check = self.s.check(commit)
        self.assertEqual((check['owner'], check['offset']), ('owner-a2', 0))
        self.s.settle(commit)
        self.p.step()
        self.assertEqual(self.owned('a')['committed'], 0)

    def test_pending_release_does_not_block_other_partitions(self):
        self.make(partitions=('a', 'b'))
        self.p.step()
        self.deliver('a', 'a-0', ['a'])
        self.deliver('b', 'b-0', ['b'])
        self.p.step()
        self.s.settle(self.owned('a')['commit_request'])
        self.s.settle(self.owned('b')['commit_request'])
        self.p.step()
        for partition in ('a', 'b'):
            self.s.finish_fetch(self.owned(partition)['fetch_request'])
        self.s.begin_revoke('a', 'owner-a')
        self.p.step()
        self.assertIsNotNone(self.owned('a')['release_request'])
        # a's release stays pending while b keeps archiving new data.
        request = self.owned('b')['fetch_request']
        if request is None:
            self.p.step()
            request = self.owned('b')['fetch_request']
        self.s.deliver_batch(request, 'b-1', rows(1, ['b2']))
        self.s.finish_fetch(request)
        self.p.step()
        self.s.settle(self.owned('b')['commit_request'])
        self.p.step()
        self.assertEqual(self.owned('b')['committed'], 1)
        self.assertIsNotNone(self.owned('a')['release_request'])
        self.s.settle(self.owned('a')['release_request'], reply=False)
        self.p.step()
        self.assertEqual({x['partition']: x['owner'] for x in self.s.owned()},
                         {'b': 'owner-b'})
        self.assertEqual(self.a.progress(),
                         [dict(partition='b', owner='owner-b', offset=1)])

    def test_interleaved_transfer_restart_and_lost_replies(self):
        """End-to-end: two partitions, revoke mid-fetch, lost replies, restart."""
        self.make(partitions=('a', 'b', 'c'))
        self.p.step()
        # a receives two batches in one fetch, fragments interleaved.
        request_a = self.owned('a')['fetch_request']
        self.s.deliver_batch(request_a, 'a-0', rows(0, ['a0', 'a1']), parts=2,
                             order=[0, 1])
        self.s.deliver_batch(request_a, 'a-1', rows(2, ['a2']), parts=1)
        self.s.begin_revoke('a', 'owner-a')
        # b stays slow: its fetch is never completed for now.
        self.deliver('c', 'c-0', ['c0'])
        self.p.step()
        self.assertEqual(self.owned('a')['fetch_request'], request_a)
        self.s.finish_fetch(request_a)
        self.p.step()
        self.assertEqual(self.a.read('a'),
                         [dict(offset=i, body='a%d' % i) for i in range(3)])
        commit_a = self.owned('a')['commit_request']
        self.assertIsNotNone(commit_a)
        self.s.settle(commit_a, reply=False)       # lost reply
        self.p.step()
        self.assertEqual(self.owned('a')['committed'], 2)
        self.assertIsNotNone(self.owned('c')['commit_request'])
        self.s.settle(self.owned('c')['commit_request'])
        self.p.step()
        self.assertEqual(self.owned('c')['committed'], 0)

        # a can now release; the reply is lost and the process dies.
        self.assertIsNotNone(self.owned('a')['release_request'])
        self.s.settle(self.owned('a')['release_request'], reply=False)
        del self.p
        self.p = Pump(self.s, self.a)
        self.p.step()
        self.assertNotIn('a', [x['partition'] for x in self.s.owned()])
        self.assertEqual(self.a.read('a'),
                         [dict(offset=i, body='a%d' % i) for i in range(3)])
        self.assertEqual(self.a.read('c'), [dict(offset=0, body='c0')])

        # b was never fetched to completion; it is still owned and pending.
        self.assertIsNotNone(self.owned('b')['fetch_request'])
        request_b = self.owned('b')['fetch_request']
        self.s.deliver_batch(request_b, 'b-0', rows(0, ['b0']))
        self.s.finish_fetch(request_b)
        self.p.step()
        self.assertEqual(self.a.read('b'), [dict(offset=0, body='b0')])
        self.assertIsNotNone(self.owned('b')['commit_request'])

    def test_source_committed_ahead_of_archive_is_a_protocol_error(self):
        class Ahead:
            def __init__(self):
                self.state = dict(partition='a', owner='owner-a', committed=4,
                                  revoking=False, fetch_request=None,
                                  commit_request=None, release_request=None)
            def owned(self):
                return [dict(self.state)]
            def poll_events(self):
                return []
            def fetch(self, partition, owner):
                raise AssertionError('nothing to fetch')
            def commit(self, partition, owner, offset):
                raise AssertionError('must not confirm')
            def release(self, partition, owner):
                raise AssertionError('must not release')
            def check(self, request):
                raise AssertionError('no requests exist')

        with self.assertRaises(RuntimeError):
            Pump(Ahead(), NewArchive()).step()

    def test_slow_and_pending_partitions_do_not_block_progress(self):
        self.make(partitions=('a', 'b', 'c'), auto_accept=True)
        self.p.step()
        # b keeps its fetch pending forever; c is still fetching.
        self.deliver('a', 'a1', ['a'])
        self.deliver('c', 'c1', ['c'])
        for _ in range(3):
            self.p.step()
        self.assertEqual(self.owned('a')['committed'], 0)
        self.assertEqual(self.owned('c')['committed'], 0)
        self.assertEqual(self.owned('b')['committed'], -1)
        self.assertEqual(self.a.read('a'), [dict(offset=0, body='a')])

        # Transferring b does not stop a or c either.
        self.s.begin_revoke('b', 'owner-b')
        self.deliver('a', 'a2', ['a2'], first=1)
        self.deliver('c', 'c2', ['c2'], first=1)
        for _ in range(3):
            self.p.step()
        self.assertEqual(self.owned('a')['committed'], 1)
        self.assertEqual(self.owned('c')['committed'], 1)
        self.assertIn('b', [x['partition'] for x in self.s.owned()])

    def test_idle_pump_returns_false_and_close_does_not_commit(self):
        self.make()
        self.p.step()
        self.deliver('a', 'batch', ['x'])
        self.p.step()
        # Only pending commit/fetch remain: no busy loop.
        before = {x['partition']: x for x in self.s.owned()}
        self.assertFalse(self.p.step())
        self.assertFalse(self.p.step())
        self.assertEqual(self.s.owned(), list(before.values()))
        commit = self.owned('a')['commit_request']
        self.assertIsNotNone(commit)
        self.p.close()
        self.assertEqual(self.s.check(commit)['state'], 'pending')

    def test_fragment_digest_and_shape_are_enforced_by_archive(self):
        a = NewArchive()
        good = digest_of(['x', 'y'])
        a.stage('p', 'o', 'b', 0, 2, 0, 2, [dict(offset=0, body='x')], good)
        self.assertFalse(a.complete('p', 'o', 'b'))
        with self.assertRaises(ValueError):     # body disagrees with the digest
            a.stage('p', 'o', 'b', 0, 2, 1, 2, [dict(offset=1, body='wrong')],
                    good)
            a.complete('p', 'o', 'b')
        self.assertEqual(a.read('p'), [])
        a2 = NewArchive()
        a2.stage('p', 'o', 'b', 0, 2, 0, 2, [dict(offset=0, body='x')], good)
        with self.assertRaises(ValueError):     # conflicting metadata
            a2.stage('p', 'o', 'b', 0, 2, 1, 3, [dict(offset=1, body='y')], good)
        with self.assertRaises(ValueError):     # offset outside the batch
            a2.stage('p', 'o', 'b', 0, 2, 1, 2, [dict(offset=7, body='y')], good)
        a2.stage('p', 'o', 'b', 0, 2, 1, 2, [dict(offset=1, body='y')], good)
        self.assertTrue(a2.complete('p', 'o', 'b'))
        self.assertTrue(a2.complete('p', 'o', 'b'))   # re-delivery is a no-op
        self.assertEqual(a2.read('p'), [dict(offset=0, body='x'),
                                        dict(offset=1, body='y')])
        self.assertEqual(a2.pending(), [])
        self.assertEqual(a2.progress(), [dict(partition='p', owner='o', offset=1)])

    def test_corrupt_fragment_digest_is_rejected(self):
        a = NewArchive()
        a.stage('p', 'o', 'b', 0, 1, 0, 1, [dict(offset=0, body='x')],
                digest_of(['y']))
        with self.assertRaises(ValueError):
            a.complete('p', 'o', 'b')
        self.assertEqual(a.read('p'), [])
        self.assertEqual(a.progress(), [dict(partition='p', owner='o', offset=-1)])


if __name__ == '__main__':
    unittest.main()
