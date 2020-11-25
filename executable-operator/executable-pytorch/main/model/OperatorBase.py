
class OperatorBase:
    def __init__(self, name, ID, inputKeys, outputKeys, Params):
        self.name = name
        self.ID = ID
        self.inputData = {keys: None for keys in inputKeys}
        self.outputData = {keys: None for keys in outputKeys}
        self.params = Params
        self.outputConnections = []
        self.inputConnections = []

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

    def executor(self):
        pass
