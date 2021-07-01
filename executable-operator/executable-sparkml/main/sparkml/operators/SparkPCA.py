import traceback

from pyspark.ml.feature import VectorAssembler, PCA
from pyspark.ml import Pipeline
from executable.basic.model.OperatorBase import OperatorBase
from executable.basic.utils.Logger import Logger

"""
@ProjectName: CLIC
@Time       : 2020/11/25 19:29
@Author     : Jimmy
@Description: Spark ML PCA
"""

logger = Logger('OperatorLogger').logger


class SparkPCA(OperatorBase):
    def __init__(self, ID, inputKeys, outputKeys, Params):
        super().__init__("SparkPCA", ID, inputKeys, outputKeys, Params)

    def execute(self):
        try:
            df = self.getInputData("data")
            cols = [x.strip() for x in self.params["cols"].split(',')]
            k = int(self.params["k"])
            output_col = self.params["outputCol"]
            handle_invalid = self.params["handleInvalid"] if "handleInvalid" in self.params else None
            handle_invalid = 'skip' if handle_invalid is not None and str(handle_invalid).lower() == 'skip' else 'keep'

            assembler_lr = VectorAssembler() \
                .setHandleInvalid(handle_invalid) \
                .setInputCols(cols) \
                .setOutputCol("features_all")

            # PCA
            pca = PCA().setK(k) \
                .setInputCol("features_all") \
                .setOutputCol(output_col)

            # 建立管道
            pipeline = Pipeline(stages=[assembler_lr, pca])
            df = pipeline.fit(df) \
                .transform(df) \
                .drop("features_all")

            self.setOutputData("result", df)

        except Exception as e:
            logger.error(e.args)
            logger.error(traceback.format_exc())
