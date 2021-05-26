import argparse
from executable.DagExecutor import DagExecutor
from pytorch.basic.PytorchOperatorFactory import PytorchOperatorFactory

"""
@ProjectName: CLIC
@Time       : 2020/11/28 下午5:41
@Author     : NKCqx, zjchen
@Description: 
"""


# def parseYAML(yaml_path):
#     return YamlParser.parse(yaml_path)
#
#
# def execute(heads):
#     print("Stage(Pytorch) ———— Start A New Pytorch Stage")
#     start = time.process_time()
#     # 开始拓扑排序
#     topoTraversal = TopoTraversal(heads)
#     while topoTraversal.hasNextOpt():
#         curOpt = topoTraversal.nextOpt()
#         print("="*100 + "Stage(Pytorch) ———— Current Pytorch Operator is " + curOpt.name)
#         curOpt.execute()
#         print("Stage(Pytorch) ———— Current Pytorch Result:\n", curOpt.getOutputData("result"))
#         connections = curOpt.getOutputConnections()
#         for connection in connections:
#             targetOpt = connection.getTargetOpt()
#             topoTraversal.updateInDegree(targetOpt, -1)
#             keyPairs = connection.getKeys()
#             for keyPair in keyPairs:
#                 if keyPair[0] is not None and keyPair[1] is not None:
#                     sourceResult = curOpt.getOutputData(keyPair[0])
#                     targetOpt.setInputData(keyPair[1], sourceResult)

    # 任务结束，输出信息
    # end = time.process_time()
    # print("Stage(Pytorch) ———— Running hold time:： " + str(end - start) + "s")
    # print("Stage(Pytorch) ———— End The Current Pytorch Stage")


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='analysis and execute the given yaml job')
    parser.add_argument("--dagPath", type=str, help='yaml file path declaring the job')
    parser.add_argument("--stageId", type=str, help='python udf file\'s root dir path that containg all the udf file in absolute path')
    parser.add_argument("--masterHost", type=str, default=None)
    parser.add_argument("--masterPort", type=str, default=None)
    args = parser.parse_known_args(["--dagPath=/Users/zjchen/Desktop/ClicYamlOutput/physical-dag-1710586742.yml", "--stageId=1710586742", "--masterHost=127.0.0.1", "--masterPort=7777"])

    executor = DagExecutor(args, PytorchOperatorFactory())

    executor.execute()
