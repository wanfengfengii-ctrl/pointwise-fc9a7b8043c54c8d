import hashlib
import json
import unittest
from source_sdk import SourceSDK


class SourceSDKTests(unittest.TestCase):
    def setUp(self):self.s=SourceSDK({'a':'owner-a','b':'owner-b'})
    def batch(self, first=0):return [dict(offset=first,body='温度'),dict(offset=first+1,body='flow')]
    def test_fragment_digest_reordering_and_duplicate_parts(self):
        request=self.s.fetch('a','owner-a');self.s.deliver_batch(request,'batch',self.batch(),parts=2,order=[1,0,1]);self.s.finish_fetch(request)
        events=self.s.poll_events();self.assertEqual([e['part'] for e in events[:-1]],[1,0,1]);self.assertEqual(events[-1]['kind'],'fetch_done')
        digest=hashlib.sha256(json.dumps(['温度','flow'],ensure_ascii=False,separators=(',',':')).encode()).hexdigest()
        self.assertEqual(events[0]['digest'],digest);self.assertEqual(self.s.poll_events(),[])
        events[0]['rows'][0]['body']='outside';self.assertEqual(self.s.retained_offsets('a','owner-a'),[0,1])
    def test_commit_lost_reply_keeps_server_fact_and_releases_raw(self):
        request=self.s.fetch('a','owner-a');self.s.deliver_batch(request,'batch',self.batch());self.s.finish_fetch(request);self.s.poll_events()
        commit=self.s.commit('a','owner-a',1);self.s.settle(commit,reply=False)
        self.assertEqual(self.s.poll_events(),[]);self.assertEqual(self.s.check(commit)['state'],'accepted')
        owned={x['partition']:x for x in self.s.owned()};self.assertEqual(owned['a']['committed'],1);self.assertEqual(owned['a']['commit_request'],commit)
        self.assertEqual(self.s.retained_offsets('a','owner-a'),[])
        with self.assertRaises(ValueError):self.s.deliver_batch(self.s.fetch('a','owner-a'),'again',self.batch())
    def test_revoke_keeps_outstanding_fetch_but_prevents_new_fetch(self):
        request=self.s.fetch('a','owner-a');self.s.begin_revoke('a','owner-a')
        with self.assertRaises(ValueError):self.s.fetch('a','owner-a')
        self.s.deliver_batch(request,'batch',self.batch());release=self.s.release('a','owner-a')
        with self.assertRaises(ValueError):self.s.settle(release)
        self.s.finish_fetch(request);commit=self.s.commit('a','owner-a',1);self.s.settle(commit)
        self.s.settle(release,reply=False);self.assertEqual([x['partition'] for x in self.s.owned()],['b'])
        self.assertEqual(self.s.check(release)['state'],'accepted')
        with self.assertRaises(ValueError):self.s.commit('a','owner-a',1)
        self.s.assign('a','new-owner');self.assertEqual(self.s.owned()[0]['owner'],'new-owner')
        with self.assertRaises(ValueError):self.s.commit('a','owner-a',1)
    def test_out_of_order_batches_do_not_grant_gap_commit(self):
        request=self.s.fetch('a','owner-a');self.s.deliver_batch(request,'later',self.batch(2))
        commit=self.s.commit('a','owner-a',3)
        with self.assertRaises(ValueError):self.s.settle(commit)
        self.s.deliver_batch(request,'first',self.batch());self.s.finish_fetch(request);self.s.settle(commit)
        self.assertEqual(self.s.check(commit)['state'],'accepted')
    def test_owned_is_defensive_and_pending_requests_survive_consumer_loss(self):
        request=self.s.fetch('a','owner-a');copy=self.s.owned();copy[0]['committed']=900
        self.assertEqual(self.s.owned()[0]['committed'],-1);self.assertEqual(self.s.owned()[0]['fetch_request'],request)
        with self.assertRaises(ValueError):self.s.fetch('a','owner-a')
        self.assertEqual(self.s.check(request)['state'],'pending')
        other=self.s.fetch('b','owner-b');self.s.finish_fetch(other);self.assertEqual(self.s.check(other)['state'],'accepted')
    def test_rejected_request_can_be_replaced_and_auto_accept_is_optional(self):
        request=self.s.commit('a','owner-a',-1);self.s.settle(request,accepted=False)
        self.assertEqual(self.s.check(request)['state'],'rejected')
        next_request=self.s.commit('a','owner-a',-1);self.s.auto_accept=True
        self.assertEqual(self.s.poll_events()[-1],dict(kind='reply',request=next_request,state='accepted'))

if __name__=='__main__':unittest.main()
