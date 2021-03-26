import traceback

from model.OperatorBase import OperatorBase

"""
@ProjectName: CLIC
@Time       : 2020/11/26 15:05
@Author     : jimmy
@Description: 
"""


class DataframeUnion(OperatorBase):
    def __init__(self, ID, inputKeys, outputKeys, Params):
        super().__init__("DataframeUnion", ID, inputKeys, outputKeys, Params)

    def execute(self):
        try:
            df_1 = self.getInputData("data_1")
            df_2 = self.getInputData("data_2")

            self.setOutputData("result", df_1.union(df_2))

        except Exception as e:
            print(e.args)
            print("=" * 20)
            print(traceback.format_exc())
