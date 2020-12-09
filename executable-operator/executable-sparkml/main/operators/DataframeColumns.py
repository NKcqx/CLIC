import traceback

from model.OperatorBase import OperatorBase

"""
@ProjectName: CLIC
@Time       : 2020/11/30 11:35
@Author     : jimmy
@Description: 获取dataframe的列名
"""


class DataframeColumns(OperatorBase):
    def __init__(self, ID, inputKeys, outputKeys, Params):
        super().__init__("DataframeColumns", ID, inputKeys, outputKeys, Params)

    def execute(self):
        try:
            df = self.getInputData("data")

            self.setOutputData("result", df.columns)

        except Exception as e:
            print(e.args)
            print("=" * 20)
            print(traceback.format_exc())
