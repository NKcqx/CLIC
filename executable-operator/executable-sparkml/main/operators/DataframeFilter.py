import traceback

from model.OperatorBase import OperatorBase

"""
@ProjectName: CLIC
@Time       : 2020/12/9 15:17
@Author     : jimmy
@Description: 对Dataframe按条件过滤
"""


class DataframeFilter(OperatorBase):
    def __init__(self, ID, inputKeys, outputKeys, Params):
        super().__init__("DataframeFilter", ID, inputKeys, outputKeys, Params)

    def execute(self):
        try:
            df = self.getInputData("data")
            condition = self.params["condition"]

            self.setOutputData("result", df.filter(condition))

        except Exception as e:
            print(e.args)
            print("=" * 20)
            print(traceback.format_exc())
