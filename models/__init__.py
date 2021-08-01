from influxdb_client import InfluxDBClient

DB_URL = "http://localhost:8086"
TOKEN = 'gq5w4zE-Dx_NgEvYcVCi2eByA4Nl1IE-chuzo9WapO4E3mVW9N-TzLluphLVmiDfZe9A7TXEyUdKjyRvCv9xpA=='
DB_ORG = 'org'


def singleton(cls, *args, **kw):
    instances = {}

    def _singleton():
        if cls not in instances:
            instances[cls] = cls(*args, **kw)
        return instances[cls]
    return _singleton


@singleton
class DBClient(object):

    def __init__(self):
        self.client = InfluxDBClient(url=DB_URL, token=TOKEN, org=DB_ORG)
