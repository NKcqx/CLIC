import Executor
from basic.SparkOperatorFactory import SparkOperatorFactory
from utils.SparkInitUtil import SparkInitUtil

from pyspark.conf import SparkConf
import random
import time
import datetime

"""
@ProjectName: CLIC
@Time       : 2020/12/1 16:00
@Author     : jimmy
@Description: 
"""


def RandID():
    return random.seed(time.process_time())


if __name__ == "__main__":
    program_start_time = datetime.datetime.now()

    # 初始化OperatorFactory
    factory = SparkOperatorFactory()

    # 手动初始化所有Operator
    # 构造 SparkSession
    conf = SparkConf().setAppName("CLIC_LDA").setMaster("local")
    spark = SparkInitUtil(conf=conf)

    # 读取csv文件
    source = factory.createOperator("SparkReadCSV", RandID(), [], ["result"],
                                    {"inputPath": r"E:\Project\CLIC_ML\data\Data_LDA\LDA_documents.csv",
                                     "header": True,
                                     "infer_schema": False,
                                     "nan_value": "NA"})

    # 分词
    tokenizer = factory.createOperator("SparkRegexTokenizer", RandID(), ["data"], ["result"],
                                       {"input_col": "documents",
                                        "output_col": "documents_token",
                                        "pattern": r'\s+|[,.\"]'})

    # 去停用词
    remover = factory.createOperator("SparkStopWordsRemover", RandID(), ["data"], ["result"],
                                     {"input_col": "documents_token",
                                      "output_col": "documents_stop"})

    # 统计词频
    vectorizer = factory.createOperator("SparkCountVectorizer", RandID(), ["data"], ["result"],
                                        {"input_col": "documents_stop",
                                         "output_col": "documents_count"})

    # LDA
    lda = factory.createOperator("SparkLDA", RandID(), ["data"], ["result"],
                                 {"k": 2,
                                  "col": "documents_count",
                                  "optimizer": "online",
                                  "output_label": "LDA_res"})

    # 手动构建DAG图
    source.connectTo("result", tokenizer, "data")
    tokenizer.connectFrom("data", source, "result")

    tokenizer.connectTo("result", remover, "data")
    remover.connectFrom("data", tokenizer, "result")

    remover.connectTo("result", vectorizer, "data")
    vectorizer.connectFrom("data", remover, "result")

    vectorizer.connectTo("result", lda, "data")
    lda.connectFrom("data", vectorizer, "result")

    # 拓扑排序
    Executor.execute([source])

    # 任务结束
    program_end_time = datetime.datetime.now()
    print("Finish!")
    print("Total time in program: {} s.".format(str((program_end_time - program_start_time).total_seconds())))

    res = lda.getOutputData("result")
    res.show(truncate=False)
