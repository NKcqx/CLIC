import pandas as pd
import traceback
from model.OperatorBase import OperatorBase
"""
@ProjectName: CLIC
@Time       : 2020/12/21 下午6:49
@Author     : zjchen
@Description: 
"""


class FileSink(OperatorBase):
    def __init__(self, ID, inputKeys, outputKeys, Params):
        super().__init__("FileSink", ID, inputKeys, outputKeys, Params)

    def execute(self):
        try:
            # data = self.getInputData("data")
            # data.to_csv(self.params["inputPath"])
            self.setOutputData("result", "Successful!")
        except Exception as e:
            print(e.args)
            print("="*20)
            print(traceback.format_exc())