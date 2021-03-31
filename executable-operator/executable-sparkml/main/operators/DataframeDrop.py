import traceback

from model.OperatorBase import OperatorBase

"""
@ProjectName: CLIC
@Time       : 2020/11/30 11:46
@Author     : jimmy
@Description: 删除dataframe的指定列(可一次去除多个列，参数以列表形式传入)
"""


class DataframeDrop(OperatorBase):
    def __init__(self, ID, inputKeys, outputKeys, Params):
        super().__init__("DataframeDrop", ID, inputKeys, outputKeys, Params)

    def execute(self):
        try:
            df = self.getInputData("data")
            drop_cols = self.params["drop_cols"]
            drop_cols = drop_cols.split(",")

            for col in drop_cols:
                df = df.drop(col)

            self.setOutputData("result", df)

        except Exception as e:
            print(e.args)
            print("=" * 20)
            print(traceback.format_exc())
