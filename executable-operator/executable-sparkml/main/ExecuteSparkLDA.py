from basic.SparkOperatorFactory import SparkOperatorFactory
from utils.SparkInitUtil import SparkInitUtil
from utils.TopoTraversal import TopoTraversal
from pyspark.conf import SparkConf
import random
import time

"""
@ProjectName: CLIC
@Time       : 2020/12/1 16:00
@Author     : jimmy
@Description: 
"""


def RandID():
    return random.seed(time.process_time())


if __name__ == "__main__":
    # start = time.process_time()
    start = time.time()

    # 初始化OperatorFactory
    factory = SparkOperatorFactory()

    # 手动初始化所有Operator
    # 构造 SparkSession
    conf = SparkConf().setAppName("CLIC_LDA").setMaster("local")
    spark = SparkInitUtil(conf=conf)

    # 读取csv文件
    source = factory.createOperator("SparkReadCSV", RandID(), [], ["result"],
                                    {"input_path": r"E:\Project\CLIC_ML\data\Data_LDA\LDA_documents.csv",
                                     "header": True,
                                     "infer_schema": False,
                                     "nan_value": "NA"})

    # 分词
    tokenizer = factory.createOperator("SparkRegexTokenizer", RandID(), ["data"], ["result"],
                                       {"col": "documents", "pattern": r'\s+|[,.\"]'})

    # 去停用词
    remover = factory.createOperator("SparkStopWordsRemover", RandID(), ["data"], ["result"],
                                 {"col": "documents"})

    # 统计词频
    vectorizer = factory.createOperator("SparkCountVectorizer", RandID(), ["data"], ["result"],
                                        {"col": "documents"})

    # LDA
    lda = factory.createOperator("SparkLDA", RandID(), ["data"], ["result"],
                                 {"k": 2, "col": "documents", "optimizer": "online", "output_label": "LDA_res"})

    # 手动构建DAG图
    source.connectTo("result", tokenizer, "data")
    tokenizer.connectFrom("data", source, "result")

    tokenizer.connectTo("result", remover, "data")
    remover.connectFrom("data", tokenizer, "result")

    remover.connectTo("result", vectorizer, "data")
    vectorizer.connectFrom("data", remover, "result")

    vectorizer.connectTo("result", lda, "data")
    lda.connectFrom("data", vectorizer, "result")

    # headNode
    headOperators = [source]

    # 拓扑排序
    topoTraversal = TopoTraversal(headOperators)
    while topoTraversal.hasNextOpt():
        curOpt = topoTraversal.nextOpt()
        print('*' * 100 + '\n' + 'Current operator is ' + curOpt.name)
        curOpt.execute()
        try:
            print('*' * 100 + '\n' + 'Current result: ' + curOpt.getOutputData("result").show(truncate=False))
        except Exception as e:
            print(e)
        connections = curOpt.getOutputConnections()
        for connection in connections:
            targetOpt = connection.getTargetOpt()
            topoTraversal.updateInDegree(targetOpt, -1)
            keyPairs = connection.getKeys()
            for keyPair in keyPairs:
                result = curOpt.getOutputData(keyPair[0])
                targetOpt.setInputData(keyPair[1], result)

    # 任务结束
    # end = time.process_time()
    end = time.time()
    print("Finish!")
    print("Start: " + str(time.localtime(start)))
    print("End: " + str(time.localtime(end)))

    res = lda.getOutputData("result")
    res.show(truncate=False)


