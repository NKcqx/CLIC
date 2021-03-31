import torch
import traceback
from model.OperatorBase import OperatorBase

"""
@ProjectName: CLIC
@Time       : 2020/11/25 下午12:02
@Author     : zjchen
@Description: 将输入转换成tensor
"""
def mapping_type(str_type):
    dic = {
        "float": torch.float,
        "double": torch.double,
        "int16": torch.int16,
        "int32": torch.int32,
        "int64": torch.int64
    }
    return dic[str_type]


class TensorConverter(OperatorBase):
    def __init__(self, ID, inputKeys, outputKeys, Params):
        super().__init__("TensorConverter", ID, inputKeys, outputKeys, Params)

        

    def execute(self):
        try:
            self.setOutputData("result", torch.tensor(self.getInputData("data").values,
                                                      dtype= mapping_type(self.params["dtype"])
                                                      ))
        except Exception as e:
            print(e.args)
            print("=" * 20)
            print(traceback.format_exc())
