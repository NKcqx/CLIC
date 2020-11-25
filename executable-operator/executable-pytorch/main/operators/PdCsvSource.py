import pandas as pd
import traceback
from model.OperatorBase import OperatorBase
from model.ParamsModel import ParamsModel

"""
@ProjectName: CLIC
@Time       : 2020/11/23 下午6:35
@Author     : zjchen
@Description: 文件路径参数inputPath, 输出结果result
"""


class PdCsvSource(OperatorBase):
    def __init__(self, ID, inputKeys, outputKeys, Params):
        super().__init__("PdCsvSource", ID, inputKeys, outputKeys, Params)

    def execute(self):
        try:
            self.setOutputData("result", pd.read_csv(self.params["input_Path"]))
        except Exception as e:
            print(e.args)
            print("="*20)
            print(traceback.format_exc())

