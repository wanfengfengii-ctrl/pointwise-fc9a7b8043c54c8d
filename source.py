class FakeSource:
    def __init__(self,records):
        self.records=[dict(r) for r in records];self.index=0;self.acks=[]
    def take(self):
        if self.index==len(self.records):return None
        record=self.records[self.index];self.index+=1;return dict(record)
    def ack(self,partition,offset):self.acks.append((partition,offset))
