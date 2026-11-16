"""Asynchronous shard capture bridging a source to an archive.

The Pump only uses the public ports documented in CONTRACT.md:

  source  owned / fetch / commit / release / check / poll_events
  archive stage / complete / progress / pending / retire / read

Every durable fact lives in those two port objects.  Dropping a Pump and
building a new one over the same ports is the supported restart path; the new
Pump reconciles from source.owned() and archive.progress()/pending() instead
of replaying side effects.
"""


class Pump:
    def __init__(self, source, archive):
        self._source = source
        self._archive = archive
        self._order = []        # tracked partitions, stable rotation order
        self._states = {}       # partition -> mutable view of its ownership
        self._owned_view = {}   # partition -> owned() entry from the last sync
        self._active = {}       # (partition, owner, kind) -> request record
        self._resolved = {}     # (partition, owner, kind) -> last finished request
        self._rr = 0
        self._cooldown = set()  # partitions not retried again this step
        self._sync_changed = False
        self._closed = False

    # ------------------------------------------------------------------
    # public surface
    def step(self):
        """Do one finite round of available work; True if anything happened."""
        if self._closed:
            raise RuntimeError('pump is closed')
        self._cooldown = set()
        self._sync_changed = False
        self._sync_owned()
        events = self._source.poll_events()
        did = False
        for event in events:
            if self._apply_event(event):
                did = True
        self._sync_owned()
        if self._sync_changed:
            did = True
        while self._advance_once():
            did = True
        # Ownership may have changed while advancing (a release was accepted).
        self._sync_owned()
        if self._sync_changed:
            did = True
        if self._acquire_fetches():
            did = True
        return did

    def close(self):
        """Release local memory only.  Never confirms or hands anything back."""
        self._closed = True
        self._order = []
        self._states = {}
        self._owned_view = {}
        self._active = {}
        self._resolved = {}
        self._cooldown = set()

    # ------------------------------------------------------------------
    # ownership bookkeeping
    def _sync_owned(self):
        view = {item['partition']: item for item in self._source.owned()}
        owned_keys = {(partition, item['owner']) for partition, item in view.items()}

        progress = self._archive.progress()
        pending = self._archive.pending()
        archive_keys = {(e['partition'], e['owner']) for e in progress}
        archive_keys |= {(e['partition'], e['owner']) for e in pending}

        stray = {(e['partition'], e['owner']) for e in pending} - owned_keys
        if stray:
            raise RuntimeError(
                'archive holds an incomplete batch for an owner the source no '
                'longer lists: %r' % (sorted(stray),))

        retired = set()
        for key in sorted(archive_keys):
            if key not in owned_keys:
                self._retire_key(*key)
                retired.add(key)

        for partition in list(self._order):
            state = self._states.get(partition)
            item = view.get(partition)
            if state is not None and (item is None or item['owner'] != state['owner']):
                key = (partition, state['owner'])
                if key in archive_keys and key not in retired:
                    self._retire_key(*key)
                self._drop_partition(partition)

        for partition, item in view.items():
            state = self._states.get(partition)
            if state is None:
                state = dict(partition=partition, owner=item['owner'],
                             committed=item['committed'],
                             revoking=bool(item.get('revoking')),
                             fetch=item.get('fetch_request'),
                             rejected_commit=None, rejected_release=None,
                             released=False)
                self._states[partition] = state
                if partition not in self._order:
                    self._order.append(partition)
            else:
                state['committed'] = item['committed']
                state['revoking'] = bool(item.get('revoking'))
                state['fetch'] = item.get('fetch_request')

            for kind, field in (('commit', 'commit_request'),
                                ('release', 'release_request')):
                request = item.get(field)
                key = (partition, state['owner'], kind)
                if request is None or key in self._active:
                    continue
                if self._resolved.get(key) == request:
                    continue  # already checked; owned() still lists it
                self._resolved.pop(key, None)  # a newer request supersedes it
                self._active[key] = dict(request=request, partition=partition,
                                         owner=state['owner'], kind=kind,
                                         offset=None, done=False, accepted=False)

        self._owned_view = view

    def _retire_key(self, partition, owner):
        try:
            self._archive.retire(partition, owner)
        except ValueError as exc:
            raise RuntimeError(
                'cannot retire %r/%r: %s' % (partition, owner, exc)) from exc
        self._sync_changed = True
        for kind in ('commit', 'release'):
            self._active.pop((partition, owner, kind), None)
            self._resolved.pop((partition, owner, kind), None)

    def _drop_partition(self, partition):
        state = self._states.pop(partition, None)
        if state is not None:
            self._sync_changed = True
        if partition in self._order:
            self._order.remove(partition)
        self._cooldown.discard(partition)
        if state is not None:
            for kind in ('commit', 'release'):
                key = (partition, state['owner'], kind)
                self._active.pop(key, None)
                self._resolved.pop(key, None)

    # ------------------------------------------------------------------
    # events
    def _apply_event(self, event):
        kind = event.get('kind')
        if kind == 'fragment':
            return self._on_fragment(event)
        if kind == 'fetch_done':
            return self._on_fetch_done(event)
        if kind == 'begin_revoke':
            return self._on_begin_revoke(event)
        if kind == 'reply':
            return self._on_reply(event)
        return False

    def _current(self, partition, owner):
        """The state only while that exact ownership token is still current."""
        item = self._owned_view.get(partition)
        if item is None or item['owner'] != owner:
            return None
        return self._states.get(partition)

    def _on_fragment(self, event):
        partition = event['partition']
        owner = event['owner']
        if self._current(partition, owner) is None:
            return False  # a late event from a relinquished generation
        self._archive.stage(partition, owner, event['batch'], event['first'],
                            event['count'], event['part'], event['parts'],
                            event['rows'], event['digest'])
        # Idempotent and returns False until every fragment has arrived,
        # including fragments staged by an earlier Pump life.
        self._archive.complete(partition, owner, event['batch'])
        return True

    def _on_fetch_done(self, event):
        state = self._current(event['partition'], event['owner'])
        if state is not None and state['fetch'] == event['request']:
            state['fetch'] = None
            return True
        return False

    def _on_begin_revoke(self, event):
        state = self._current(event['partition'], event['owner'])
        if state is not None and not state['revoking']:
            state['revoking'] = True
            return True
        return False

    def _on_reply(self, event):
        # Replies are hints only.  check()/owned() stay authoritative, so a
        # lost reply is harmless and a duplicate one changes nothing.
        if event.get('state') not in ('accepted', 'rejected'):
            raise ValueError('invalid reply state: %r' % (event.get('state'),))
        # The authoritative state is read through check()/owned() later.
        return False

    # ------------------------------------------------------------------
    # one bounded action
    def _advance_once(self):
        snapshot = self._snapshot()
        count = len(self._order)
        for offset in range(count):
            index = (self._rr + offset) % count
            partition = self._order[index]
            state = self._states.get(partition)
            if state is None:
                continue
            if self._drive(state, snapshot):
                self._rr = (index + 1) % max(1, len(self._order))
                self._sync_owned()
                return True
        return False

    def _snapshot(self):
        progress = {}
        for entry in self._archive.progress():
            progress[(entry['partition'], entry['owner'])] = entry['offset']
        pending = {(e['partition'], e['owner']) for e in self._archive.pending()}
        return dict(progress=progress, pending=pending)

    def _drive(self, state, snapshot):
        partition = state['partition']
        owner = state['owner']
        key = (partition, owner)
        archived = snapshot['progress'].get(key, -1)

        # The source can never have accepted records that are not durably
        # archived; that would mean data was lost, so refuse to guess.
        if archived < state['committed']:
            raise RuntimeError(
                'source committed offset %d leads archive frontier %d for %s/%s'
                % (state['committed'], archived, partition, owner))

        commit = self._active.get((partition, owner, 'commit'))
        if commit is not None:
            response = self._check(commit, state)
            if response['state'] == 'pending':
                return False
            self._finish(commit, response['state'])
            if commit['accepted']:
                offset = response.get('offset')
                if offset is not None:
                    commit['offset'] = offset
                    if offset > archived:
                        raise RuntimeError(
                            'source accepted offset %d beyond archive frontier %d'
                            % (offset, archived))
            else:
                attempted = commit['offset']
                if attempted is None:
                    attempted = response.get('offset', archived)
                state['rejected_commit'] = (attempted, state['fetch'])
                self._cooldown.add(partition)
            return True

        if (archived > state['committed']
                and state['rejected_commit'] != (archived, state['fetch'])
                and partition not in self._cooldown):
            # Confirm only the archive's contiguous, durably published
            # frontier; never ahead of what is readable.
            self._send_commit(state, archived)
            return True

        release = self._active.get((partition, owner, 'release'))
        if release is not None:
            response = self._check(release, state)
            if response['state'] == 'pending':
                return False
            self._finish(release, response['state'])
            if release['accepted']:
                state['released'] = True
            else:
                state['rejected_release'] = self._release_signature(state, snapshot)
                self._cooldown.add(partition)
            return True

        if state['revoking'] and not state['released'] and self._release_ready(state, snapshot):
            signature = self._release_signature(state, snapshot)
            if (state['rejected_release'] != signature
                    and partition not in self._cooldown):
                self._send_release(state)
                return True
        return False

    def _check(self, record, state):
        response = self._source.check(record['request'])
        if not isinstance(response, dict) or response.get('state') not in (
                'pending', 'accepted', 'rejected'):
            raise RuntimeError('invalid check response: %r' % (response,))
        if (response.get('partition') != state['partition']
                or response.get('owner') != state['owner']):
            raise RuntimeError('check response crossed an ownership generation')
        return response

    def _finish(self, record, outcome):
        record['done'] = True
        record['accepted'] = outcome == 'accepted'
        key = (record['partition'], record['owner'], record['kind'])
        self._active.pop(key, None)
        self._resolved[key] = record['request']

    def _send_commit(self, state, offset):
        request = self._source.commit(state['partition'], state['owner'], offset)
        self._active[(state['partition'], state['owner'], 'commit')] = dict(
            request=request, partition=state['partition'], owner=state['owner'],
            kind='commit', offset=offset, done=False, accepted=False)

    @staticmethod
    def _release_signature(state, snapshot):
        key = (state['partition'], state['owner'])
        return (snapshot['progress'].get(key, -1), state['committed'], state['fetch'])

    def _release_ready(self, state, snapshot):
        if state['fetch'] is not None:
            return False
        key = (state['partition'], state['owner'])
        if key in snapshot['pending']:
            return False
        return snapshot['progress'].get(key, -1) == state['committed']

    def _send_release(self, state):
        request = self._source.release(state['partition'], state['owner'])
        self._active[(state['partition'], state['owner'], 'release')] = dict(
            request=request, partition=state['partition'], owner=state['owner'],
            kind='release', offset=None, done=False, accepted=False)

    # ------------------------------------------------------------------
    # acquisition: poll every idle, non-revoking partition once per step
    def _acquire_fetches(self):
        started = False
        for partition in list(self._order):
            state = self._states.get(partition)
            if state is None or state['revoking'] or state['fetch'] is not None:
                continue
            state['fetch'] = self._source.fetch(partition, state['owner'])
            started = True
        return started
