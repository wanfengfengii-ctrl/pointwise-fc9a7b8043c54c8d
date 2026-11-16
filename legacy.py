def capture(source,archive):
    count=0
    while True:
        record=source.take()
        if record is None:return count
        archive.store(record['partition'],record['offset'],record['body'])
        source.ack(record['partition'],record['offset']);count+=1
