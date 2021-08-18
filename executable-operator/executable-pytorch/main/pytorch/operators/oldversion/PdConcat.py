import pandas as pd
import traceback
from executable.basic.model.OperatorBase import OperatorBase

"""
@ProjectName: CLIC
@Time       : 2020/11/23 下午7:28
@Author     : zjchen
@Description: 将两个dataFrame连接起来
"""


class PdConcat(OperatorBase):
    def __init__(self, ID, inputKeys, outputKeys, Params):
        super().__init__("PdConcat", ID, inputKeys, outputKeys, Params)

    def execute(self):

        self.setOutputData("result", pd.concat([self.getInputData("data1"),
                                                self.getInputData("data2")],
                                               # ignore_index=self.params["ignore_index"],
                                               # keys=self.params["keys"],
                                               # names=self.params["names"],
                                               # sort=self.params["sort"],
                                               # join=self.params["join"],
                                               # axis=self.params["axis"],
                                               # verify_integrity=self.params["verify_integrity"]
                                               ))

    def setInputData(self, key, data):
        if self.inputData["data1"] is None:
            self.inputData["data1"] = data
        elif self.inputData["data2"] is None:
            self.inputData["data2"] = data




