import torch
import traceback
from model.OperatorBase import OperatorBase

"""
@ProjectName: CLIC
@Time       : 2020/11/25 下午12:02
@Author     : zjchen
@Description: 将输入转换成tensor
"""


class TensorConverter(OperatorBase):
    def __init__(self, ID, inputKeys, outputKeys, Params):
        super().__init__("TensorConverter", ID, inputKeys, outputKeys, Params)

    def execute(self):
        try:
            print(type(self.getInputData("data").values))
            self.setOutputData("result", torch.tensor(self.getInputData("data").values,
                                                      dtype=self.params["dtype"]
                                                      ))
            print(type(self.outputData["result"]))
        except Exception as e:
            print(e.args)
            print("=" * 20)
            print(traceback.format_exc())
