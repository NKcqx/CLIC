import pandas as pd
import traceback
from model.OperatorBase import OperatorBase

"""
@ProjectName: CLIC
@Time       : 2020/11/23 下午7:28
@Author     : zjchen
@Description: 
"""


class PdConcat(OperatorBase):
    def __init__(self, ID, inputKeys, outputKeys, Params):
        super().__init__("PdConcat", ID, inputKeys, outputKeys, Params)

    def execute(self):
        try:
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
        except Exception as e:
            print(e.args)
            print("="*20)
            print(traceback.format_exc())

    def setInputData(self, key, data):
        if self.inputData["data1"] is None:
            self.inputData["data1"] = data
        elif self.inputData["data2"] is None:
            self.inputData["data2"] = data
        else:
            raise Exception("Concat算子的输入数据已准备好")



