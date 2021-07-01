# from service.client.SchedulerServiceClient import SchedulerServiceClient
# import unittest
#
# 单元测试需要clic-master和redis环境，这里注释掉防止无法打包
#
# class TestServiceClient(unittest.TestCase):
#     masterClient = SchedulerServiceClient("testStageName", "localhost", 7777)
#
#     def test_postStarted(self):
#         self.masterClient.postStarted()
#
#     def test_postCompleted(self):
#         self.masterClient.postCompleted()
#
#     def test_postDataPrepared(self):
#         self.masterClient.postDataPrepared()
