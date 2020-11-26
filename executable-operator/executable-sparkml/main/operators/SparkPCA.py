import traceback

from pyspark.ml.feature import VectorAssembler, PCA
from pyspark.ml import Pipeline
from model.OperatorBase import OperatorBase

"""
@ProjectName: CLIC
@Time       : 2020/11/25 19:29
@Author     : jimmy
@Description: 使用pyspark对dataframe做PCA特征提取
"""


class SparkPCA(OperatorBase):
    def __init__(self, ID, inputKeys, outputKeys, Params):
        super().__init__("SparkPCA", ID, inputKeys, outputKeys, Params)

    def execute(self):
        try:
            df = self.getInputData("input_Data")
            cols = self.params["cols"]
            k = self.params["k"]
            output_label = self.params["output_label"]

            assembler_lr = VectorAssembler() \
                .setInputCols(cols) \
                .setOutputCol("features_all")

            # PCA
            pca = PCA().setK(k) \
                .setInputCol("features_all") \
                .setOutputCol(output_label)

            # 建立管道
            pipeline = Pipeline(stages=[assembler_lr, pca])
            df = pipeline.fit(df) \
                .transform(df) \
                .drop("features_all")

            self.setOutputData("result", df)

        except Exception as e:
            print(e.args)
            print("=" * 20)
            print(traceback.format_exc())