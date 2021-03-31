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
            handle_invalid = self.params["handle_invalid"] if "handle_invalid" in self.params else None
            handle_invalid = 'skip' if handle_invalid is not None and str(handle_invalid).lower() == 'skip' else 'keep'

            assembler = VectorAssembler(inputCols=input_cols, outputCol=output_col, handleInvalid=handle_invalid)

            self.setOutputData("result", assembler.transform(df))

        except Exception as e:
            print(e.args)
            print("=" * 20)
            print(traceback.format_exc())
