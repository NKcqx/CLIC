import logging
import sys
import time
from executable.DagHook import DagHook
from executable.DagArgs import DagArgs
from executable.basic.utils.Logger import Logger
from executable.basic.utils.ArgUtil import parse_args
from executable.basic.utils.TopoTraversal import TopoTraversal
from service.client.NotifyServiceClient import NotifyServiceClient
from thriftGen.notifyservice.NotifyService import StageSnapshot, StageStatus


class DagExecutor(object):
    """
    Description:
        python语言Dag的核心执行类，使用拓扑排序来执行解析好的Dag图(ArgUtil中实现)，并提供给operator远程调用的功能
    """
    def __init__(self, args, operatorFactory, dagHook=DagHook()):
        """
        Description:
            init类，会初始化DagArgs、HeadOperator等参数
        Args:
            1. args: parser.parse_known_args()方法的返回对象
            2. operatorFactory: 一个Factory对象
            3. dagHook: execute前后执行函数
        """
        self.logger = Logger('ExecutorLogger', logging.DEBUG).logger
        self.initArgs(args)
        self.initNotifyClient()
        self.initOperator(operatorFactory)

        self.dagHook = dagHook

    def initArgs(self, args):
        """
        Description:
            解析命令行参数，并新建一个DagArgs对象来存储
        Args:
            args: parser.parse_known_args()方法的返回对象
        """
        self.basicArgs = DagArgs(args)
        self.platformArgs = self.basicArgs.platformArgs

    def initOperator(self, factory):
        try:
            self.headOperators = parse_args(self.basicArgs.dagPath, factory)
        except Exception as e:
            self.logger.error(e)
            self.notifyServiceClient.notify(StageSnapshot(StageStatus.FAILURE, e, dict()))
            sys.exit(-1)

    def initNotifyClient(self):
        """创建一个masterClient的连接，每个operator都将使用这个client"""
        self.notifyServiceClient = NotifyServiceClient(self.basicArgs.stageId,
                                                       self.basicArgs.jobName,
                                                       self.basicArgs.notifyHost,
                                                       self.basicArgs.notifyPort)
    def execute(self):
        """
        Description:
            执行顺序: pre_handler -> postStarted -> execute -> postCompleted -> post_handler
        Args:
            所有参数都提前准备好了，这里只需要调用execute的方法即可
        """
        try:
            self.dagHook.pre_handler(self.platformArgs)
            self.logger.info("Stage(" + str(self.basicArgs.stageId) + ")" + " started!")
            self.notifyServiceClient.notify(StageSnapshot(StageStatus.RUNNING, "", dict()))
            self.executeDag()
            self.notifyServiceClient.notify(StageSnapshot(StageStatus.COMPLETED, "", dict()))
            self.logger.info("Stage(" + str(self.basicArgs.stageId) + ")" + " completed!")
            self.dagHook.post_handler(self.platformArgs)
        except Exception as e:
            self.notifyServiceClient.notify(StageSnapshot(StageStatus.FAILURE, e, dict()))
            self.logger.error(e)

    def executeDag(self):
        start = time.process_time()
        topoTraversal = TopoTraversal(self.headOperators)
        while topoTraversal.hasNextOpt():
            curOpt = topoTraversal.nextOpt()
            self.logger.info("Stage({}) ———— Current Operator is ".format(self.basicArgs.stageId) + curOpt.name)
            curOpt.execute()
            self.logger.info("Stage({}) ———— Current Result:\n {}".format(self.basicArgs.stageId, curOpt.getOutputData("result")))
            connections = curOpt.getOutputConnections()
            for connection in connections:
                targetOpt = connection.getTargetOpt()
                topoTraversal.updateInDegree(targetOpt, -1)
                keyPairs = connection.getKeys()
                for keyPair in keyPairs:
                    if keyPair[0] is not None and keyPair[1] is not None:
                        sourceResult = curOpt.getOutputData(keyPair[0])
                        targetOpt.setInputData(keyPair[1], sourceResult)
        end = time.process_time()
        self.logger.info("Stage({}) ———— Running hold time: ".format(self.basicArgs.stageId) + str(end - start) + "s")
        self.logger.info("Stage({}) ———— End The Current Stage".format(self.basicArgs.stageId))
