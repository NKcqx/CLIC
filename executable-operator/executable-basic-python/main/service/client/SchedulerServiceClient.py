# import logging
# from executable.basic.utils.Logger import Logger
# from thrift.transport.TSocket import TSocket
# from thrift.protocol.TBinaryProtocol import TBinaryProtocol
# from thrift.protocol.TMultiplexedProtocol import TMultiplexedProtocol
# from thriftGen.master.SchedulerService import Client
#
#
# class SchedulerServiceClient(object):
#     """
#     Description:
#         实现thrift的Client类
#     Attributes:
#         1. stageId: 必须要在服务器的redis中注册过才能执行方法调用
#         2. masterHost: clic-master的地址
#         3. masterPort: clic-master的端口
#     """
#     def __init__(self, stageId, masterHost, masterPort):
#         if (stageId is None) and (masterPort is None) and (masterHost is None):
#             self.isDebug = True
#         else:
#             self.isDebug = False
#             self.transport = TSocket(host=masterHost, port=masterPort)
#             protocol = TBinaryProtocol(self.transport)
#             tMultiplexedProtocol = TMultiplexedProtocol(protocol, "SchedulerService")
#             self.client = Client(tMultiplexedProtocol)
#             self.stageId = stageId
#             self.logger = Logger('ServiceClientLogger', logging.DEBUG).logger
#
#     def checkDebug(self, info):
#         if self.isDebug:
#             self.logger.debug("debug mode: {}".format(info))
#         return not self.isDebug
#
#     def postStarted(self):
#         if self.checkDebug("postStarted"):
#             self.transport.open()
#             self.client.postStageStarted(self.stageId, {})
#             self.transport.close()
#
#     def postCompleted(self):
#         if self.checkDebug("postCompleted"):
#             self.transport.open()
#             self.client.postStageCompleted(self.stageId, {})
#             self.transport.close()
#
#     def postDataPrepared(self):
#         if self.checkDebug("postDataPrepared"):
#             self.transport.open()
#             self.client.postDataPrepared(self.stageId)
#             self.transport.close()
