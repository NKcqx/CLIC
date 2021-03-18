import traceback

from model.OperatorBase import OperatorBase

"""
@ProjectName: CLIC
@Time       : 2020/11/25 19:49
@Author     : jimmy
@Description: 对dataframe的空值填充指定值
"""


class DataframeFillNa(OperatorBase):
    def __init__(self, ID, inputKeys, outputKeys, Params):
        super().__init__("DataframeFillNa", ID, inputKeys, outputKeys, Params)

    def execute(self):
        try:
            df = self.getInputData("data")
            value = eval(self.params["value"])      # TODO 暂时不支持str

            self.setOutputData("result", df.fillna(value))

        except Exception as e:
            print(e.args)
            print("=" * 20)
            print(traceback.format_exc())
