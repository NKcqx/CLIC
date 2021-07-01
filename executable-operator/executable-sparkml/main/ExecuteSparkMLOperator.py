from executable.DagExecutor import DagExecutor
from sparkml.constants.SparkMLOperatorFactory import SparkMLOperatorFactory
from sparkml.constants.SparkMLDagHook import SparkMLDagHook
import argparse

"""
@ProjectName: CLIC
@Time       : 2020/12/8 10:44
@Author     : Jimmy, NKCqx
@Description: 
"""


# def parseYAML(yaml_path):
#     return YamlParser.parse(yaml_path)
#
#
# def execute(heads):
#     conf = SparkConf().setAppName("CLIC_demo").setMaster("local")
#     spark = SparkInitUtil(conf=conf)
#     start = time.process_time()
#     topoTraversal = TopoTraversal(heads)
#     while topoTraversal.hasNextOpt():
#         curOpt = topoTraversal.nextOpt()
#         print('*' * 100 + '\n' + 'Current operator is ' + curOpt.name)
#         curOpt.execute()
#         # try:
#         #     print('*' * 100 + '\n')
#         #     curOpt.getOutputData("result").show(truncate=False)
#         # except Exception as e:
#         #     print(e)
#         connections = curOpt.getOutputConnections()
#         for connection in connections:
#             targetOpt = connection.getTargetOpt()
#             topoTraversal.updateInDegree(targetOpt, -1)
#             keyPairs = connection.getKeys()
#             for keyPair in keyPairs:
#                 if keyPair[0] is not None and keyPair[1] is not None:
#                     sourceResult = curOpt.getOutputData(keyPair[0])
#                     targetOpt.setInputData(keyPair[1], sourceResult)
#
#     # 任务结束，输出信息
#     end = time.process_time()
#     print("Stage(SparkML) ———— Running hold time:： " + str(end - start) + "s")
#     print("Stage(SparkML) ———— End The Current SparkML Stage")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='analysis and execute the given yaml job')
    parser.add_argument("--dagPath", type=str, help='yaml file path declaring the job')
    # parser.add_argument("--udfPath", type=str,
    #                     help='python udf file\'s root dir path that containg all the udf file in absolute path', )
    parser.add_argument("--stageId", type=str)
    parser.add_argument("--masterHost", type=str, default=None)
    parser.add_argument("--masterPort", type=str, default=None)
    args = parser.parse_known_args(["--dagPath=E:/clic_output/SparkML_PCA/physical-dag-1296291597.yml", "--stageId=1296291597", "--masterHost=127.0.0.1", "--masterPort=7777"])

    # args = parser.parse_args()

    executor = DagExecutor(args, SparkMLOperatorFactory(), SparkMLDagHook())

    executor.execute()
