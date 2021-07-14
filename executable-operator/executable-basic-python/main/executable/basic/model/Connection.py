"""
@ProjectName: CLIC
@Time       : 2020/11/25 下午7:07
@Author     : zjchen
"""


class Connection:
    """
    Description:
        连接两个operator用，完成两个operator的key的映射
    Attributes:
        1. sourceOpt: 计算完成的opt
        2. sourceKey: 输出结果的名字
        3. targetOpt: 下一跳opt
        4. targetKey: 下一跳opt所需要的数据名称
    """
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
