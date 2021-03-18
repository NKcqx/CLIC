
"""
@ProjectName: CLIC
@Time       : 2020/11/25 下午7:07
@Author     : zjchen
@Description: 
"""


class Connection:
    def __init__(self, sourceOpt, sourceKey, targetOpt, targetKey):
        self.sourceOpt = sourceOpt
        self.sourceKeys = [sourceKey]
        self.targetOpt = targetOpt
        self.targetKeys = [targetKey]

    def addKey(self, sourceKey, targetKey):
        self.sourceKeys.append(sourceKey)
        self.targetKeys.append(targetKey)

    def getKeys(self):
        res = list(zip(self.sourceKeys, self.targetKeys))
        return res

    def getSourceOpt(self):
        return self.sourceOpt

    def getSourceKeys(self):
        return self.sourceKeys

    def getTargetOpt(self):
        return self.targetOpt

    def getTargetKeys(self):
        return self.targetKeys