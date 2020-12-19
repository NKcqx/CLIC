import traceback

from model.OperatorBase import OperatorBase
import joblib

"""
@ProjectName: CLIC
@Time       : 2020/12/8 15:50
@Author     : jimmy
@Description: Spark 拟合数据
"""


class SparkTransform(OperatorBase):
    def __init__(self, ID, inputKeys, outputKeys, Params):
        super().__init__("SparkTransform", ID, inputKeys, outputKeys, Params)

    def execute(self):
        try:
            model = self.getInputData("model")
            data = self.getInputData("data")

            self.setOutputData("result", model.transform(data))

        except Exception as e:
            print(e.args)
            print("=" * 20)
            print(traceback.format_exc())
