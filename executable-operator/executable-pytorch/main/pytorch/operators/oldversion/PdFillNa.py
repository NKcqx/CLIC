from executable.basic.model.OperatorBase import OperatorBase

"""
@ProjectName: CLIC
@Time       : 2020/11/24 下午4:20
@Author     : zjchen
@Description: 将目标dataFrame中对null值填上value
"""


class PdFillNa(OperatorBase):
    def __init__(self, ID, inputKeys, outputKeys, Params):
        super().__init__("PdFillNa", ID, inputKeys, outputKeys, Params)

    def execute(self):
        self.setOutputData("result", self.getInputData("data").fillna(float(self.params["value"])))
