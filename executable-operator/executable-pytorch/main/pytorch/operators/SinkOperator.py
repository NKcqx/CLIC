import pandas as pd

from executable.basic.model.OperatorBase import OperatorBase

"""
@ProjectName: CLIC
@Time       : 2020/12/21 下午6:49
@Author     : zjchen
@Description: 将tensor或dataFrame写CSV文件
"""


class SinkOperator(OperatorBase):
    def __init__(self, ID, inputKeys, outputKeys, Params):
        super().__init__("SinkOperator", ID, inputKeys, outputKeys, Params)

    def execute(self):
        data = self.getInputData("data")
        if self.params["type"] == "dataFrame":
            pass
        elif self.params["type"] == "tensor":
            data = pd.DataFrame(data.numpy())
        data.to_csv(self.params["outputPath"])

        self.setOutputData("result", "Successful!")
