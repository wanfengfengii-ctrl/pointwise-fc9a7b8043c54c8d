import unittest
from archive import Archive
from source import FakeSource
from legacy import capture

class Tests(unittest.TestCase):
    def test_order(self):
        s=FakeSource([dict(partition='a',offset=0,body='x'),dict(partition='b',offset=0,body='y')]);a=Archive()
        self.assertEqual(capture(s,a),2)
        self.assertEqual(s.acks,[('a',0),('b',0)])
        self.assertEqual(a.read('a'),[dict(offset=0,body='x')])
    def test_no_ack_after_failed_archive(self):
        s=FakeSource([dict(partition='a',offset=0,body='x')])
        class Broken:
            def store(self,*args):raise OSError('disk')
        with self.assertRaises(OSError):capture(s,Broken())
        self.assertEqual(s.acks,[])
    def test_idempotent_store(self):
        a=Archive();a.store('p',0,'x');a.store('p',0,'x')
        self.assertEqual(a.read('p'),[dict(offset=0,body='x')])
        with self.assertRaises(ValueError):a.store('p',0,'y')
