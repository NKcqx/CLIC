import traceback
from model.OperatorBase import OperatorBase
"""
@ProjectName: CLIC
@Time       : 2020/12/13 上午11:42
@Author     : zjchen
@Description: 根据列名获得DataFrame中的某一列，返回结果是Series格式
"""


class PdGetSeries(OperatorBase):
    def __init__(self, ID, inputKeys, outputKeys, Params):
        super().__init__("PdGetSeries", ID, inputKeys, outputKeys, Params)

    def execute(self):
        try:
            self.setOutputData("result", self.getInputData("data")[self.params["value"]])
        except Exception as e:
            print(e.args)
            print("="*20)
            print(traceback.format_exc())
