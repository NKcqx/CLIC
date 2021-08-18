import pandas as pd
from executable.basic.model.OperatorBase import OperatorBase

"""
@ProjectName: CLIC
@Time       : 2020/11/25 上午11:48
@Author     : zjchen
@Description: 按照行列切分一个dataFrame
"""


class PdIloc(OperatorBase):
    def __init__(self, ID, inputKeys, outputKeys, Params):
        super().__init__("PdIloc", ID, inputKeys, outputKeys, Params)

    def execute(self):
        row_from = self.params["row_from"]
        row_from = int(row_from) if (row_from != "None") else None
        row_to = self.params["row_to"]
        row_to = int(row_to) if (row_to != "None") else None
        col_from = self.params["col_from"]
        col_from = int(col_from) if (col_from != "None") else None
        col_to = self.params["col_to"]
        col_to = int(col_to) if (col_to != "None") else None

        dataFrame = pd.DataFrame(self.getInputData("data"))
        dataFrame = dataFrame.iloc[row_from: row_to,
                                   col_from: col_to]
        self.setOutputData("result", dataFrame)

