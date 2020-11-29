from utils.TopoTraversal import TopoTraversal
import logging
import time
import random
"""
@ProjectName: CLIC
@Time       : 2020/11/28 下午5:41
@Author     : NKCqx
@Description: 
"""
def execute(heads):
    print("Stage(Pytorch) ———— Start A New Pytorch Stage")
    start = time.process_time()
    # 开始拓扑排序
    topoTraversal = TopoTraversal(heads)
    while topoTraversal.hasNextOpt():
        curOpt = topoTraversal.nextOpt()
        print("="*100 + "Stage(Pytorch) ———— Current Pytorch Operator is " + curOpt.name)
        curOpt.execute()
        print("Stage(Pytorch) ———— Current Pytorch Result:\n", curOpt.getOutputData("result"))
        connections = curOpt.getOutputConnections()
        for connection in connections:
            targetOpt = connection.getTargetOpt()
            topoTraversal.updateInDegree(targetOpt, -1)
            keyPairs = connection.getKeys()
            for keyPair in keyPairs:
                if keyPair[0] is not None and keyPair[1] is not None:
                    sourceResult = curOpt.getOutputData(keyPair[0])
                    targetOpt.setInputData(keyPair[1], sourceResult)

    # 任务结束，输出信息
    end = time.process_time()
    print("Stage(Pytorch) ———— Running hold time:： " + str(end - start) + "s")
    print("Stage(Pytorch) ———— End The Current Pytorch Stage")
