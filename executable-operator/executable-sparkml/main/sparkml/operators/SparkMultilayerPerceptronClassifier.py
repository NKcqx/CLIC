import traceback

from executable.basic.model.OperatorBase import OperatorBase
from executable.basic.utils.Logger import Logger
from pyspark.ml.classification import MultilayerPerceptronClassifier

"""
@ProjectName: CLIC
@Time       : 2021/4/12 17:58
@Author     : Jimmy
@Description: Spark ML 多层感知机分类器
"""

logger = Logger('OperatorLogger').logger


class SparkMultilayerPerceptronClassifier(OperatorBase):
    def __init__(self, ID, inputKeys, outputKeys, Params):
        super().__init__("SparkMultilayerPerceptronClassifier", ID, inputKeys, outputKeys, Params)

    def execute(self):
        try:
            df = self.getInputData("data")  # 输入的 dataframe
            features_col = self.params["featuresCol"]  # 训练的feature列
            label_col = self.params["labelCol"]  # 训练的label列
            predict_col = self.params["predictCol"]  # 输出的predict列
            max_iter = int(self.params['maxIter'])  # 最大迭代次数
            layers = eval(self.params['layers']) if 'layers' in self.params else None  # 网络结构，此参数可选
            seed = self.params['seed'] if 'seed' in self.params else None  # 随机种子，此参数可选
            tol = float(self.params['tol']) if 'tol' in self.params else 1e-6  # 迭代算法的收敛容忍阈值
            solver = self.params['solver'] if 'solver' in self.params else 'l-bfgs'  # 优化算法，此参数可选
            step_size = float(self.params['stepSize']) if 'stepSize' in self.params else 0.03  # 每次优化迭代使用的步长
            block_size = int(self.params['blockSize']) if 'blockSize' in self.params else 128  # block大小

            classifier = MultilayerPerceptronClassifier() \
                .setMaxIter(max_iter) \
                .setLayers(layers) \
                .setSeed(seed) \
                .setTol(tol) \
                .setSolver(solver) \
                .setStepSize(step_size) \
                .setBlockSize(block_size) \
                .setFeaturesCol(features_col) \
                .setLabelCol(label_col) \
                .setPredictionCol(predict_col)

            self.setOutputData("result", classifier.fit(df))

        except Exception as e:
            logger.error(e.args)
            logger.error(traceback.format_exc())
