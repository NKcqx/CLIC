import traceback

from model.OperatorBase import OperatorBase

"""
@ProjectName: CLIC
@Time       : 2020/11/30 11:46
@Author     : jimmy
@Description: 
"""


class DataframeDrop(OperatorBase):
    def __init__(self, ID, inputKeys, outputKeys, Params):
        super().__init__("DataframeDrop", ID, inputKeys, outputKeys, Params)

    def execute(self):
        try:
            df = self.getInputData("data")
            drop_col = self.params["drop_col"]

            self.setOutputData("result", df.drop(drop_col))

        except Exception as e:
            print(e.args)
            print("=" * 20)
            print(traceback.format_exc())