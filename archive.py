class Archive:
    def __init__(self):self.records={}
    def store(self,partition,offset,body):
        key=(partition,offset)
        if key in self.records and self.records[key]!=body:raise ValueError('conflict')
        self.records[key]=body
    def read(self,partition):
        return [dict(offset=o,body=b) for (p,o),b in sorted(self.records.items()) if p==partition]
