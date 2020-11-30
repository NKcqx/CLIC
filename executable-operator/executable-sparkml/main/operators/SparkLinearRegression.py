import traceback

from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline
from pyspark.ml.regression import LinearRegression
from model.OperatorBase import OperatorBase

"""
@ProjectName: CLIC
@Time       : 2020/11/25 19:24
@Author     : jimmy
@Description: Spark ML库线性回归
"""


class SparkLinearRegression(OperatorBase):
    def __init__(self, ID, inputKeys, outputKeys, Params):
        super().__init__("SparkLinearRegression", ID, inputKeys, outputKeys, Params)

    def execute(self):
        try:
            training_data = self.getInputData("training_data")
            test_data = self.getInputData("test_data")
            training_features = self.params["training_features"]
            label = self.params["label"]
            # max_iter = self.params["max_iter"]
            # predict_label = self.params["predict_label"]

            # 将所有特征拼接成一个feature向量
            assembler_lr = VectorAssembler() \
                .setInputCols(training_features) \
                .setOutputCol("features_lr")

            # 定义线性回归模型
            lr = LinearRegression() \
                .setFeaturesCol("features_lr") \
                .setPredictionCol(self.params["predict_label"]) \
                .setLabelCol(label) \
                .setFitIntercept(True) \
                .setMaxIter(self.params["max_iter"]) \
                .setRegParam(0.3) \
                .setElasticNetParam(0.8)

            # 建立管道
            pipeline_lr = Pipeline(stages=[assembler_lr, lr])
            lr_model = pipeline_lr.fit(training_data)

            # 预测线性回归模型的值
            predictions_lr = lr_model.transform(test_data) \
                .drop("features_lr")

            self.setOutputData("result", predictions_lr)

        except Exception as e:
            print(e.args)
            print("=" * 20)
            print(traceback.format_exc())
