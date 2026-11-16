import json
from archive import Archive
from source import FakeSource
from legacy import capture
s=FakeSource([dict(partition='a',offset=0,body='sample')]);a=Archive()
assert capture(s,a)==1 and s.acks==[('a',0)]
print(json.dumps(a.read('a')))
