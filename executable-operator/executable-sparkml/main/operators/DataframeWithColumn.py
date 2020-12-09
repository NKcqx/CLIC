import traceback

from model.OperatorBase import OperatorBase
"""
@ProjectName: CLIC
@Time       : 2020/12/9 15:01
@Author     : jimmy
@Description: 在Dataframe中添加新列
"""


class DataframeWithColumn(OperatorBase):
    def __init__(self, ID, inputKeys, outputKeys, Params):
        super().__init__("DataframeWithColumn", ID, inputKeys, outputKeys, Params)

    def execute(self):
        try:
            df = self.getInputData("data")
            col_name = self.params["col_name"]
            col = self.params["col"]

            self.setOutputData("result", df.withColumn(col_name, col))

        except Exception as e:
            print(e.args)
            print("=" * 20)
            print(traceback.format_exc())
