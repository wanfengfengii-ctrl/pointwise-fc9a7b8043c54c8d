import hashlib
import json


class Archive:
    def __init__(self):self.records={}
    def store(self,partition,offset,body):
        key=(partition,offset)
        if key in self.records and self.records[key]!=body:raise ValueError('conflict')
        self.records[key]=body
    def read(self,partition):
        return [dict(offset=o,body=b) for (p,o),b in sorted(self.records.items()) if p==partition]


class NewArchive:
    """Persistent fragment archive for the asynchronous source protocol.

    Fragments may be staged one at a time, but only a fully staged batch that
    passes metadata, coverage and digest validation is published.  Published
    records are queryable through read(); staged fragments are not.  All state
    lives in this object, so a restarted Pump can resume partial batches.
    """

    def __init__(self):
        self._batches = {}    # (partition, owner, batch) -> staging state
        self._owners = set()  # (partition, owner) registered by staging
        self._spans = {}      # (partition, owner) -> merged [start, end] spans
        self._published = {}  # partition -> {offset: body}

    # -- validation helpers ---------------------------------------------
    @staticmethod
    def _check_scalar(partition, owner, batch):
        if not isinstance(partition, str) or not isinstance(owner, str):
            raise ValueError('partition and owner must be strings')
        if batch is None or not isinstance(batch, (str, int)):
            raise ValueError('batch identity must be a string or integer')

    @staticmethod
    def _check_fragment(first, count, part, parts, digest):
        if type(first) is not int or type(count) is not int or count < 1:
            raise ValueError('invalid batch extent')
        if type(parts) is not int or type(part) is not int or parts < 1 or not 0 <= part < parts:
            raise ValueError('invalid fragment metadata')
        if not isinstance(digest, str):
            raise ValueError('invalid digest')

    @staticmethod
    def _rows(rows, first, count):
        if not isinstance(rows, list):
            raise ValueError('rows must be a list')
        result = {}
        for row in rows:
            if not isinstance(row, dict):
                raise ValueError('invalid row')
            offset = row.get('offset')
            body = row.get('body')
            if type(offset) is not int or not first <= offset < first + count:
                raise ValueError('row offset outside batch')
            if not isinstance(body, str):
                raise ValueError('body must be UTF-8 text')
            if offset in result:
                raise ValueError('duplicate row offset in fragment')
            result[offset] = body
        return result

    @staticmethod
    def _digest(bodies):
        blob = json.dumps(bodies, ensure_ascii=False, separators=(',', ':')).encode()
        return hashlib.sha256(blob).hexdigest()

    # -- archive port ----------------------------------------------------
    def stage(self, partition, owner, batch, first, count, part, parts, rows, digest):
        self._check_scalar(partition, owner, batch)
        self._check_fragment(first, count, part, parts, digest)
        incoming = self._rows(rows, first, count)

        key = (partition, owner, batch)
        state = self._batches.get(key)
        if state is None:
            state = dict(first=first, count=count, parts=parts, digest=digest,
                         rows={}, complete=False)
            self._batches[key] = state
        elif (state['first'], state['count'], state['parts'], state['digest']) != (
                first, count, parts, digest):
            raise ValueError('batch identity or metadata conflict')

        if state['complete']:
            # A replayed fragment of an already published batch must agree
            # with what was published; anything else is real corruption.
            published = self._published.get(partition, {})
            for offset, body in incoming.items():
                if published.get(offset) != body:
                    raise ValueError('fragment conflicts with published batch')
            return None

        part_rows = state['rows'].setdefault(part, {})
        for offset, body in incoming.items():
            if offset in part_rows and part_rows[offset] != body:
                raise ValueError('fragment conflicts with staged content')
        part_rows.update(incoming)
        self._owners.add((partition, owner))
        return None

    def complete(self, partition, owner, batch):
        key = (partition, owner, batch)
        state = self._batches.get(key)
        if state is None:
            return False
        if state['complete']:
            return True
        if set(state['rows']) != set(range(state['parts'])):
            return False

        offsets = {}
        for part in range(state['parts']):
            for offset, body in state['rows'][part].items():
                if offset in offsets:
                    raise ValueError('fragment offset covered more than once')
                offsets[offset] = body
        expected = set(range(state['first'], state['first'] + state['count']))
        if set(offsets) != expected:
            raise ValueError('fragments do not cover the complete batch')

        bodies = [offsets[offset] for offset in range(state['first'],
                                                      state['first'] + state['count'])]
        if self._digest(bodies) != state['digest']:
            raise ValueError('batch digest mismatch')

        span = (state['first'], state['first'] + state['count'] - 1)
        owner_key = (partition, owner)
        spans = self._spans.get(owner_key, [])
        for start, end in spans:
            if start <= span[1] and span[0] <= end:
                raise ValueError('completed batches overlap')
        merged = self._merge(spans + [span])

        records = self._published.setdefault(partition, {})
        # Everything is validated; mutate only now so publication is atomic.
        records.update(offsets)
        self._spans[owner_key] = merged
        state['complete'] = True
        state['rows'] = {}
        return True

    @staticmethod
    def _merge(spans):
        merged = []
        for start, end in sorted(spans):
            if merged and start <= merged[-1][1] + 1:
                if end > merged[-1][1]:
                    merged[-1] = (merged[-1][0], end)
            else:
                merged.append((start, end))
        return merged

    def progress(self):
        result = []
        for partition, owner in sorted(self._owners):
            spans = self._spans.get((partition, owner), [])
            frontier = -1
            for start, end in spans:
                if start <= frontier + 1:
                    frontier = max(frontier, end)
                else:
                    break
            result.append(dict(partition=partition, owner=owner, offset=frontier))
        return result

    def pending(self):
        return [dict(partition=partition, owner=owner, batch=batch,
                     first=state['first'], count=state['count'])
                for (partition, owner, batch), state in sorted(self._batches.items())
                if not state['complete']]

    def retire(self, partition, owner):
        owner_key = (partition, owner)
        for (p, o, _batch), state in self._batches.items():
            if (p, o) == owner_key and not state['complete']:
                raise ValueError('cannot retire owner with pending batches')
        for key in [k for k in self._batches if (k[0], k[1]) == owner_key]:
            del self._batches[key]
        self._spans.pop(owner_key, None)
        self._owners.discard(owner_key)
        return None

    def read(self, partition):
        return [dict(offset=offset, body=body)
                for offset, body in sorted(self._published.get(partition, {}).items())]
