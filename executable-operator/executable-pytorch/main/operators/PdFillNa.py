import traceback
from model.OperatorBase import OperatorBase

"""
@ProjectName: CLIC
@Time       : 2020/11/24 下午4:20
@Author     : zjchen
@Description: 
"""


class PdFillNa(OperatorBase):
    def __init__(self, ID, inputKeys, outputKeys, Params):
        super().__init__("PdFillNa", ID, inputKeys, outputKeys, Params)

    def execute(self):
        try:
            self.setOutputData("result", self.getInputData("data").fillna(self.params["value"]))
        except Exception as e:
            print(e.args)
            print("="*20)
            print(traceback.format_exc())