from basic.SparkOperatorFactory import SparkOperatorFactory
from utils.TopoTraversal import TopoTraversal
import random
import time

"""
@ProjectName: CLIC
@Time       : 2020/11/30 10:48
@Author     : jimmy
@Description: 使用CLIC中已扩展的算子完成PCA的操作
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
    spark_session = factory.createOperator("CreateSparkSession", RandID(), [], ["result"],
                                           {"app_name": "CLIC_demo", "master": "local"})

    # 读取csv文件
    source = factory.createOperator("SparkReadCSV", RandID(), ["spark_session"], ["result"],
                                    {"input_path": r"E:\Project\CLIC_ML\data\Data_PCA\hotel_bookings.csv",
                                     "header": True,
                                     "infer_schema": True,
                                     "nan_value": "NA"})
    # 去除无用列
    df_drop = factory.createOperator("DataframeDrop", RandID(), ["data"], ["result"],
                                     {"drop_col": "reservation_status_date"})

    # 提取部分dataframe
    df_part = factory.createOperator("DataframeLimit", RandID(), ["data"], ["result"], {"number": 8000})

    # 获取dataframe的列名
    df_columns = factory.createOperator("DataframeColumns", RandID(), ["data"], ["result"], {})


    # 标准化
    standard_scaler = factory.createOperator("SparkStandardScaler", RandID(), ["data", "cols"], ["result"], {})

    # onehot编码
    onehot_encoder = factory.createOperator("SparkOneHotEncode", RandID(), ["data", "cols"], ["result"], {})

    # PCA特征提取
    pca = factory.createOperator("SparkPCA", RandID(), ["data", "cols"], ["result"],
                                 {"k": 10, "output_label": "PCA_res"})

    # 手动构建DAG图
    spark_session.connectTo("result", source, "spark_session")
    source.connectFrom("spark_session", spark_session, "result")

    source.connectTo("result", df_drop, "data")
    df_drop.connectFrom("data", source, "result")

    df_drop.connectTo("result", df_part, "data")
    df_part.connectFrom("data", df_drop, "result")

    df_drop.connectTo("result", df_columns, "data")
    df_columns.connectFrom("data", df_drop, "result")

    df_part.connectTo("result", standard_scaler, "data")
    standard_scaler.connectFrom("data", df_part, "result")

    df_columns.connectTo("result", standard_scaler, "cols")
    standard_scaler.connectFrom("cols", df_columns, "result")

    standard_scaler.connectTo("result", onehot_encoder, "data")
    onehot_encoder.connectFrom("data", standard_scaler, "result")

    df_columns.connectTo("result", onehot_encoder, "cols")
    onehot_encoder.connectFrom("cols", df_columns, "result")

    onehot_encoder.connectTo("result", pca, "data")
    pca.connectFrom("data", pca, "result")

    df_columns.connectTo("result", pca, "cols")
    pca.connectFrom("cols", df_columns, "result")

    # headNode
    headOperators = [spark_session]

    # 拓扑排序
    topoTraversal = TopoTraversal(headOperators)
    while topoTraversal.hasNextOpt():
        curOpt = topoTraversal.nextOpt()
        print('*' * 100 + '\n' + 'Current operator is ' + curOpt.name)
        curOpt.execute()
        try:
            print('*' * 100 + '\n' + 'Current result: ' + curOpt.getOutputData("result").show())
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

    res = pca.getOutputData("result")
    res.show()
    print(res.count())


