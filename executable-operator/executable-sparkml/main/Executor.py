from utils.TopoTraversal import TopoTraversal

"""
@ProjectName: CLIC
@Time       : 2020/12/8 10:44
@Author     : jimmy
@Description: 
"""


def execute(heads):
    topoTraversal = TopoTraversal(heads)
    while topoTraversal.hasNextOpt():
        curOpt = topoTraversal.nextOpt()
        print('*' * 100 + '\n' + 'Current operator is ' + curOpt.name)
        curOpt.execute()
        try:
            print('*' * 100 + '\n' + 'Current result: ' + curOpt.getOutputData("result").show(truncate=False))
        except Exception as e:
            print(e)
        connections = curOpt.getOutputConnections()
        for connection in connections:
            targetOpt = connection.getTargetOpt()
            topoTraversal.updateInDegree(targetOpt, -1)
            keyPairs = connection.getKeys()
            for keyPair in keyPairs:
                result = curOpt.getOutputData(keyPair[0])
                targetOpt.setInputData(keyPair[1], result)
