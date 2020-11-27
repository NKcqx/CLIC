import traceback

from pyspark.ml.feature import StringIndexer, OneHotEncoder
from pyspark.ml import Pipeline
from model.OperatorBase import OperatorBase

"""
@ProjectName: CLIC
@Time       : 2020/11/25 15:07
@Author     : jimmy
@Description: pyspark对dataframe进行onehot编码
"""


class SparkOneHotEncode(OperatorBase):
    def __init__(self, ID, inputKeys, outputKeys, Params):
        super().__init__("SparkOneHotEncode", ID, inputKeys, outputKeys, Params)

    def execute(self):
        try:
            df = self.getInputData("input_data")
            cols = self.params["cols"]

            indexers = [StringIndexer(inputCol=c, outputCol=c + '-idx') for c in cols]
            encoders = [OneHotEncoder(inputCol=c + '-idx', outputCol=c + "-onehot", dropLast=False) for c in cols]
            pipeline = Pipeline(stages=indexers + encoders)
            df = pipeline.fit(df).transform(df)
            for c in cols:
                df = df.drop(c).drop(c + '-idx')
                df = df.withColumnRenamed(c + "-onehot", c)

            self.setOutputData("result", df)

        except Exception as e:
            print(e.args)
            print("=" * 20)
            print(traceback.format_exc())
