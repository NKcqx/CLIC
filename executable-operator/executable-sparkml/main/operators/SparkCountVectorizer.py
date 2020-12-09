import traceback

from model.OperatorBase import OperatorBase
from pyspark.ml.feature import CountVectorizer

"""
@ProjectName: CLIC
@Time       : 2020/12/7 13:59
@Author     : jimmy
@Description: Spark统计词频
"""


class SparkCountVectorizer(OperatorBase):
    def __init__(self, ID, inputKeys, outputKeys, Params):
        super().__init__("SparkCountVectorizer", ID, inputKeys, outputKeys, Params)

    def execute(self):
        try:
            df = self.getInputData("data")
            input_col = self.params["input_col"]
            output_col = self.params["output_col"]

            vectorizer = CountVectorizer(inputCol=input_col, outputCol=output_col)

            self.setOutputData("result", vectorizer.fit(df).transform(df))

        except Exception as e:
            print(e.args)
            print("=" * 20)
            print(traceback.format_exc())
