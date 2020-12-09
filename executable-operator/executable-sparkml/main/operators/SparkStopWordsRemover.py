import traceback

from model.OperatorBase import OperatorBase
from pyspark.ml.feature import StopWordsRemover

"""
@ProjectName: CLIC
@Time       : 2020/12/7 13:51
@Author     : jimmy
@Description: Spark去停用词
"""


class SparkStopWordsRemover(OperatorBase):
    def __init__(self, ID, inputKeys, outputKeys, Params):
        super().__init__("SparkStopWordsRemover", ID, inputKeys, outputKeys, Params)

    def execute(self):
        try:
            df = self.getInputData("data")
            input_col = self.params["input_col"]
            output_col = self.params["output_col"]

            remover = StopWordsRemover(inputCol=input_col, outputCol=output_col)

            self.setOutputData("result", remover.transform(df))

        except Exception as e:
            print(e.args)
            print("=" * 20)
            print(traceback.format_exc())
