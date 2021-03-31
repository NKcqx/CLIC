import traceback

from model.OperatorBase import OperatorBase

"""
@ProjectName: CLIC
@Time       : 2020/11/30 10:56
@Author     : jimmy
@Description: 取Dataframe的前number行
"""


class DataframeLimit(OperatorBase):
    def __init__(self, ID, inputKeys, outputKeys, Params):
        super().__init__("DataframeLimit", ID, inputKeys, outputKeys, Params)

    def execute(self):
        try:
            df = self.getInputData("data")
            number = int(self.params["number"])

            self.setOutputData("result", df.limit(number))

        except Exception as e:
            print(e.args)
            print("=" * 20)
            print(traceback.format_exc())
