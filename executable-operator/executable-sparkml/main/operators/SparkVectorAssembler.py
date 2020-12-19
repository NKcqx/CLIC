import traceback

from model.OperatorBase import OperatorBase
from pyspark.ml.feature import VectorAssembler
"""
@ProjectName: CLIC
@Time       : 2020/12/8 16:04
@Author     : jimmy
@Description: 
"""


class SparkVectorAssembler(OperatorBase):
    def __init__(self, ID, inputKeys, outputKeys, Params):
        super().__init__("SparkVectorAssembler", ID, inputKeys, outputKeys, Params)

    def execute(self):
        try:
            df = self.getInputData("data")
            input_cols = self.params["input_cols"]
            output_col = self.params["output_col"]

            assembler = VectorAssembler(inputCols=input_cols, outputCol=output_col)

            self.setOutputData("result", assembler.transform(df))

        except Exception as e:
            print(e.args)
            print("=" * 20)
            print(traceback.format_exc())
