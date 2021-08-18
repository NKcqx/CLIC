from thriftGen.notifyservice.NotifyService import Client
from loguru import logger
from thrift.transport.TSocket import TSocket
from thrift.protocol.TBinaryProtocol import TBinaryProtocol

"""
Time       : 2021/6/30 10:15 上午
Author     : zjchen
Description:
"""


class NotifyServiceClient(object):
    def __init__(self, stageId, jobName, host, port):
        if (host is None) and (port == 0):
            self.isDebug = True
        else:
            self.isDebug = False
            self.transport = TSocket(host=host, port=port)
            self.client = Client(TBinaryProtocol(self.transport))
            self.stageId = stageId
            self.logger = logger
            self.jobName = jobName

    def notify(self, snapshot):
        if self.isDebug:
            self.logger.info("debug info: {}", snapshot.message)
            return
        try:
            self.transport.open()
            self.client.postStatus(self.jobName, self.stageId, snapshot)
        except Exception as e:
            self.logger.error(e)
        finally:
            self.transport.close()


