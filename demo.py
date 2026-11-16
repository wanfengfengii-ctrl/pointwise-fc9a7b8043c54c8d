import json

from archive import Archive, NewArchive
from legacy import capture
from source import FakeSource
from source_sdk import SourceSDK
from streambridge import Pump

# 1. Legacy synchronous capture is untouched.
s = FakeSource([dict(partition='a', offset=0, body='sample')])
a = Archive()
assert capture(s, a) == 1 and s.acks == [('a', 0)]
print(json.dumps(a.read('a')))

# 2. New asynchronous shard capture: fragments, transfer and lost replies.
source = SourceSDK({'a': 'owner-a', 'b': 'owner-b'})
archive = NewArchive()
pump = Pump(source, archive)


def owned(partition):
    return {item['partition']: item for item in source.owned()}[partition]


def step_until_idle(limit=20):
    for _ in range(limit):
        if not pump.step():
            return
    raise AssertionError('pump kept producing work')


pump.step()
assert owned('a')['fetch_request'] and owned('b')['fetch_request']

# a: three fragments, reordered and one duplicated.
request_a = owned('a')['fetch_request']
source.deliver_batch(request_a, 'a-0',
                     [dict(offset=0, body='温度'), dict(offset=1, body='flow')],
                     parts=2, order=[1, 0, 1])
source.finish_fetch(request_a)
# b: fragments arriving in reverse order.
request_b = owned('b')['fetch_request']
source.deliver_batch(request_b, 'b-0',
                     [dict(offset=0, body='one'), dict(offset=1, body='two')],
                     parts=2, order=[1, 0])
source.finish_fetch(request_b)

step_until_idle()

# Confirm both archived batches, losing a's reply on purpose.
commit_a = owned('a')['commit_request']
commit_b = owned('b')['commit_request']
assert commit_a and commit_b
source.settle(commit_a, reply=False)   # accepted, reply dropped
source.settle(commit_b)                # accepted with reply
step_until_idle()
assert source.check(commit_a)['state'] == 'accepted'
assert owned('a')['committed'] == 1 and owned('b')['committed'] == 1
assert source.retained_offsets('a', 'owner-a') == []   # raw rows are gone

# Hand partition a over.  The empty follow-up fetch must finish first.
source.begin_revoke('a', 'owner-a')
pump.step()
source.finish_fetch(owned('a')['fetch_request'])
step_until_idle()
release = owned('a')['release_request']
assert release is not None
source.settle(release, reply=False)    # accepted, reply dropped

# Simulated process exit: throw the Pump away, keep the two port objects.
del pump
pump = Pump(source, archive)
step_until_idle()
assert {item['partition']: item['owner'] for item in source.owned()} == {
    'b': 'owner-b'}
assert archive.read('a') == [dict(offset=0, body='温度'),
                            dict(offset=1, body='flow')]
assert all(e['owner'] != 'owner-a' for e in archive.progress())  # old owner retired

# A new owner takes over the partition; stale events of the old generation
# must not touch the new ownership or the archive.
source.assign('a', 'owner-a2')
source.replay_events([
    dict(kind='fragment', request='request-stale', partition='a',
         owner='owner-a', batch='a-stale', first=0, count=1, part=0, parts=1,
         rows=[dict(offset=0, body='stale')], digest='0' * 64),
    dict(kind='begin_revoke', partition='a', owner='owner-a'),
])
step_until_idle()
assert owned('a')['owner'] == 'owner-a2'
assert archive.read('a') == [dict(offset=0, body='温度'),
                            dict(offset=1, body='flow')]
assert archive.pending() == []
assert owned('a')['fetch_request'] is not None

print(json.dumps({
    'owned': [{k: item[k] for k in ('partition', 'owner', 'committed')}
              for item in source.owned()],
    'archive': {'a': archive.read('a'), 'b': archive.read('b')},
    'progress': archive.progress(),
    'pending': archive.pending(),
}, ensure_ascii=False, indent=2))
