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
            self.setOutputData("result", torch.tensor(self.getInputData("input_Data"),
                                                      dtype=self.params["dtype"]
                                                      ))
        except Exception as e:
            print(e.args)
            print("=" * 20)
            print(traceback.format_exc())
