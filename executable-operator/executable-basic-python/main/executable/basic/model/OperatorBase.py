from executable.basic.model.Connection import Connection
from abc import ABCMeta, abstractmethod


class OperatorBase(metaclass=ABCMeta):
    """
    Description:
        所有operator的抽象基类，要求子类必须实现execute
    Attributes:
        1. ID(str)                : opt的ID，全局唯一
        2. inDegree(int)          : opt在Dag图中的入度
        3. inputConnections(list) : 传入数据到这个opt的connection列表
        4. inputData(dict)        : 输入数据字典
        5. masterClient           : 该opt的Client，用来实现master的远程调用
        6. name(str)              : opt的名字
        7. outputConnections(list): 当前opt传出数据的connection列表
        8. outputData(dict)       : 输出数据字典
        9. params(dict)           : opt计算参数字典，与inputData不同(inputData只包括数据不包括参数)
    """
    def __init__(self, name, ID, inputKeys, outputKeys, Params):
        self.name = name
        self.ID = ID
        self.inputData = {keys: None for keys in inputKeys}
        self.outputData = {keys: None for keys in outputKeys}
        self.params = Params
        self.outputConnections = []
        self.inputConnections = []
        self.inDegree = 0
        self.masterClient = None

    def setMasterClient(self, masterClient):
        self.masterClient = masterClient

    def getMasterClient(self):
        return self.masterClient

    def setParams(self, key, value):
        self.params[key] = value

    def setInputData(self, key, data):
        self.inputData[key] = data

    def getInputData(self, key):
        return self.inputData[key]

    def setOutputData(self, key, data):
        self.outputData[key] = data

    def getOutputData(self, key):
        return self.outputData[key]

    def updateInDegree(self, delta):
        self.inDegree += delta
        return self.inDegree

    def getInDegree(self):
        return self.inDegree

    def getOutputConnections(self):
        return self.outputConnections

    def getInputConnections(self):
        return self.inputConnections

    def connectTo(self, sourceKey, targetOpt, targetKey):
        for connection in self.outputConnections:
            if connection.getTargetOpt() == targetOpt:
                connection.addKey(sourceKey, targetKey)
                return
        self.outputConnections.append(Connection(self, sourceKey, targetOpt, targetKey))

    def connectToFromConnection(self, connection):
        if connection.getSourceOpt() == self or connection.getTargetOpt() == self:
            self.outputConnections.append(connection)
        else:
            raise Exception("Connection 的两端未包含当前Opt, Connection："
                            + connection.getSourceOpt().toString()
                            + " -> "
                            + connection.getTargetOpt().toString()
                            + "。当前Operator"
                            + self.name)

    def disconnectTo(self, connection):
        if connection in self.outputConnections:
            self.outputConnections.remove(connection)

    def connectFrom(self, targetKey, sourceOpt, sourceKey):
        for connection in self.inputConnections:
            if connection.getSourceOpt() == sourceOpt:
                connection.addKey(sourceKey, targetKey)
                return
        self.updateInDegree(1)
        self.inputConnections.append(Connection(sourceOpt, sourceKey, self, targetKey))

    def connectFromConnection(self, connection):
        if connection.getSourceOpt() == self or connection.getTargetOpt() == self:
            self.inputConnections.append(connection)
            self.updateInDegree(1)
        else:
            raise Exception("Connection 的两端未包含当前Opt, Connection："
                            + connection.getSourceOpt().toString()
                            + " -> "
                            + connection.getTargetOpt().toString()
                            + "。当前Operator"
                            + self.name)

    def disconnectFrom(self, connection):
        if connection in self.inputConnections:
            self.inputConnections.remove(connection)
            self.updateInDegree(-1)

    @abstractmethod
    def execute(self):
        pass
