from operators.PdCsvSource import PdCsvSource
from operators.PdConcat import PdConcat
from operators.PdStandardization import PdStandardization
from operators.PdFillNa import PdFillNa
from operators.PdDummies import PdDummies
import argparse
from model.FunctionModel import FunctionModel
from utils.ReflectUtil import ReflectUtil
from model.ParamsModel import ParamsModel
"""
@ProjectName: CLIC
@Time       : 2020/11/24 下午3:35
@Author     : zjchen
@Description: 
"""

if __name__ == '__main__':
    pass

# class ExecutePytorchOperator():
#     def __init__(self, udfpath, dagpath):
#         self.udfpath = udfpath
#         self.dagpath = dagpath
#
#     def execute(self):
#         functionModel = ReflectUtil.createInstanceAndMethodByPath(self.udfpath)
#         try:
#             inputArgs = ParamsModel(functionModel)
#
#         except Exception as e:
#             print(e)
#
#
# print()
