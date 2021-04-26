from basics.model.Connection import Connection


class OperatorBase:
    def __init__(self, name, ID, inputKeys, outputKeys, Params):
        self.name = name
        self.ID = ID
        self.inputData = {keys: None for keys in inputKeys}
        self.outputData = {keys: None for keys in outputKeys}
        self.params = Params
        self.outputConnections = []
        self.inputConnections = []
        self.inDegree = 0

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

    def executor(self):
        pass
