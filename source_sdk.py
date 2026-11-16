"""Deterministic source-side fixture. It contains no Pump or archive logic.

Only owned/fetch/commit/release/check/poll_events are consumer ports. The
remaining methods belong to the test operator, who controls server events.
"""
import copy
import hashlib
import json


class SourceSDK:
    def __init__(self, assignments, auto_accept=False):
        self._owners = {}
        self._requests = {}
        self._events = []
        self._serial = 0
        self._delivered = {}
        self._raw = {}
        self.auto_accept = auto_accept
        for partition, owner in assignments.items():
            self.assign(partition, owner)

    def assign(self, partition, owner):
        if partition in self._owners:
            raise ValueError('partition already owned')
        if (partition, owner) in self._delivered:
            raise ValueError('ownership token reused')
        self._owners[partition] = dict(partition=partition, owner=owner, committed=-1,
            revoking=False, fetch_request=None, commit_request=None, release_request=None)
        self._delivered[partition, owner] = {}
        self._raw[partition, owner] = {}

    def owned(self):
        return copy.deepcopy([self._owners[p] for p in sorted(self._owners)])

    def _owned(self, partition, owner):
        state = self._owners.get(partition)
        if not state or state['owner'] != owner:
            raise ValueError('ownership lost')
        return state

    def _new(self, kind, partition, owner, offset=None):
        state = self._owned(partition, owner)
        prior = state[kind + '_request']
        if prior and self._requests[prior]['state'] == 'pending':
            raise ValueError('request still pending')
        self._serial += 1
        request = 'request-%d' % self._serial
        op = dict(kind=kind, partition=partition, owner=owner, state='pending')
        if offset is not None:
            op['offset'] = offset
        self._requests[request] = op
        state[kind + '_request'] = request
        return request

    def fetch(self, partition, owner):
        if self._owned(partition, owner)['revoking']:
            raise ValueError('revoking owner cannot fetch')
        return self._new('fetch', partition, owner)

    def commit(self, partition, owner, offset):
        if type(offset) is not int or offset < -1:
            raise ValueError('invalid offset')
        return self._new('commit', partition, owner, offset)

    def release(self, partition, owner):
        return self._new('release', partition, owner)

    def check(self, request):
        op = self._requests[request]
        return copy.deepcopy({k: v for k, v in op.items() if k != 'kind'})

    def deliver_batch(self, request, batch, rows, parts=1, order=None):
        op = self._requests[request]
        if op['kind'] != 'fetch' or op['state'] != 'pending':
            raise ValueError('not an outstanding fetch')
        state = self._owned(op['partition'], op['owner'])
        rows = sorted(copy.deepcopy(rows), key=lambda row: row['offset'])
        if not rows or type(parts) is not int or parts < 1:
            raise ValueError('empty or invalid batch')
        first = rows[0]['offset']
        if [r['offset'] for r in rows] != list(range(first, first + len(rows))):
            raise ValueError('noncontiguous batch')
        if any(type(r['body']) is not str for r in rows):
            raise ValueError('body must be UTF-8 text')
        key = (op['partition'], op['owner'])
        if first <= state['committed']:
            raise ValueError('source has already released these rows')
        digest = hashlib.sha256(json.dumps([r['body'] for r in rows], ensure_ascii=False,
            separators=(',', ':')).encode()).hexdigest()
        signature = dict(first=first, count=len(rows), digest=digest)
        known = self._delivered[key].get(batch)
        if known is not None and known != signature:
            raise ValueError('batch identity conflict')
        for other, values in self._delivered[key].items():
            if other != batch and set(r['offset'] for r in rows) & set(range(values['first'], values['first'] + values['count'])):
                raise ValueError('overlapping batches')
        order = list(range(parts)) if order is None else list(order)
        if sorted(set(order)) != list(range(parts)):
            raise ValueError('all fragment parts must be delivered')
        digest = hashlib.sha256(json.dumps([r['body'] for r in rows], ensure_ascii=False,
            separators=(',', ':')).encode()).hexdigest()
        self._delivered[key][batch] = signature
        self._raw[key].update({r['offset']: r['body'] for r in rows})
        for part in order:
            self._events.append(dict(kind='fragment', request=request, partition=op['partition'],
                owner=op['owner'], batch=batch, first=first, count=len(rows), part=part, parts=parts,
                rows=copy.deepcopy(rows[part::parts]), digest=digest))

    def finish_fetch(self, request):
        op = self._requests[request]
        if op['kind'] != 'fetch' or op['state'] != 'pending':
            raise ValueError('not an outstanding fetch')
        state = self._owned(op['partition'], op['owner'])
        op['state'] = 'accepted'
        state['fetch_request'] = None
        self._events.append(dict(kind='fetch_done', request=request,
            partition=op['partition'], owner=op['owner']))

    def begin_revoke(self, partition, owner):
        self._owned(partition, owner)['revoking'] = True
        self._events.append(dict(kind='begin_revoke', partition=partition, owner=owner))

    def settle(self, request, accepted=True, reply=True):
        op = self._requests[request]
        if op['kind'] == 'fetch':
            raise ValueError('use finish_fetch for fetch')
        if op['state'] != 'pending':
            return
        state = self._owned(op['partition'], op['owner'])
        key = (op['partition'], op['owner'])
        if accepted:
            offsets = {offset for batch in self._delivered[key].values() for offset in range(batch['first'], batch['first'] + batch['count'])}
            frontier = -1
            while frontier + 1 in offsets:
                frontier += 1
            if op['kind'] == 'commit':
                if not state['committed'] <= op['offset'] <= frontier:
                    raise ValueError('commit exceeds delivered continuous frontier')
                state['committed'] = op['offset']
                self._raw[key] = {o: b for o, b in self._raw[key].items() if o > op['offset']}
            else:
                if not state['revoking'] or state['fetch_request'] or state['committed'] < max(offsets, default=-1):
                    raise ValueError('release not yet eligible')
                del self._owners[op['partition']]
            op['state'] = 'accepted'
        else:
            op['state'] = 'rejected'
        if reply:
            self._events.append(dict(kind='reply', request=request, state=op['state']))

    def replay_events(self, events):
        self._events.extend(copy.deepcopy(events))

    def poll_events(self):
        if self.auto_accept:
            pending = [r for r, op in self._requests.items() if op['state'] == 'pending' and op['kind'] != 'fetch']
            for request in pending:
                self.settle(request)
        events, self._events = self._events, []
        return copy.deepcopy(events)

    def retained_offsets(self, partition, owner):
        """Test-operator observation, not a permitted Pump port."""
        return sorted(self._raw[partition, owner])
