
"""
@ProjectName: CLIC
@Time       : 2020/11/25 上午10:04
@Author     : zjchen
@Description: 
"""

from utils.ReflectUtil import ReflectUtil


class ParamsModel:
    def __init__(self, functionModel):
        self.functionModel = functionModel
        self.functionClasspath = None

    def setFunctionClasspath(self, functionClasspath):
        self.functionClasspath = functionClasspath

    def getFunctionModel(self):
        if self.functionModel is None:
            self.functionModel = ReflectUtil.createInstanceAndMethodByPath(self.functionClasspath)
        return self.functionModel