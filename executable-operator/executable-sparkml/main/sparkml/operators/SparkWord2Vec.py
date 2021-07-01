import traceback

from executable.basic.model.OperatorBase import OperatorBase
from executable.basic.utils.Logger import Logger
from pyspark.ml.feature import Word2Vec

"""
@ProjectName: CLIC
@Time       : 2021/4/12 18:39
@Author     : Jimmy
@Description: Spark Word2Vec
"""

logger = Logger('OperatorLogger').logger


class SparkWord2Vec(OperatorBase):
    def __init__(self, ID, inputKeys, outputKeys, Params):
        super().__init__("SparkWord2Vec", ID, inputKeys, outputKeys, Params)

    def execute(self):
        try:
            df = self.getInputData("data")  # 输入的 dataframe
            input_col = self.params["inputCol"]  # 特征输入列
            output_col = self.params["outputCol"]  # 词向量输出列名
            vector_size = int(self.params["vectorSize"])  # 词向量维度
            min_count = int(self.params["minCount"])  # 出现在word2vec模型中的最小词频
            seed = self.params['seed'] if 'seed' in self.params else None  # 随机种子，此参数可选
            step_size = float(self.params['stepSize']) if 'stepSize' in self.params else 0.03  # 每次优化迭代使用的步长
            max_iter = int(self.params['maxIter']) if 'maxIter' in self.params else 1  # 最大迭代次数
            num_partitions = int(self.params['numPartitions']) if 'numPartitions' in self.params else 1  # 分词的句子的个数
            # 输入数据中每个句子的最大单词数，超出阈值会被分割
            max_sentence_length = int(self.params['maxSentenceLength']) if 'maxSentenceLength' in self.params else 1000

            word2vec = Word2Vec() \
                .setInputCol(input_col) \
                .setOutputCol(output_col) \
                .setVectorSize(vector_size) \
                .setMinCount(min_count) \
                .setSeed(seed) \
                .setStepSize(step_size) \
                .setMaxIter(max_iter) \
                .setNumPartitions(num_partitions) \
                .setMaxSentenceLength(max_sentence_length)

            self.setOutputData("result", word2vec.fit(df))

        except Exception as e:
            logger.error(e.args)
            logger.error(traceback.format_exc())
