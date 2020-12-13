from basic.PytorchOperatorFactory import PytorchOperatorFactory
import random
import time
import Executor
"""
@ProjectName: CLIC
@Time       : 2020/12/13 上午11:05
@Author     : zjchen
@Description: 
"""


def RandomID():
    return random.seed(time.process_time())


if __name__ == '__main__':
    start = time.process_time()

    # 初始化OptFactory
    Factory = PytorchOperatorFactory()

    # 初始化operator
    source = Factory.createOperator("PdCsvSource", RandomID(), [], ["result"],
                                    {"input_Path": "/Users/zjchen/PycharmProjects/Pytorch/Word2Vec/examiner-date-tokens.csv"})

    iloc = Factory.createOperator("PdIloc", RandomID(), ["data"], ["result"], {"row_from": 0,
                                                                               "row_to": 1000,
                                                                               "col_from": 1,
                                                                               "col_to": 2})

    getSeriesByName = Factory.createOperator("PdGetSeries", RandomID(), ["data"], ["result"], {
        "value": "headline_tokens"
    })

    word2vec = Factory.createOperator("Word2Vec", RandomID(), ["data"], ["result"], {
        "min_count": 5,
        "max_window_size": 5,
        "batch_size": 512,
        "num_workers": 0,
        "loss": "SigmoidBinaryCrossEntropyLoss",
        "embed_size": 8,
        "network": "Word2VecNet",
        "lr": 0.025,
        "num_epochs": 10,
        "device": "cpu",
        "optimizer": "Adam"
    })

    # getSimilarTokens =
    source.connectTo("result", iloc, "data")
    iloc.connectFrom("data", source, "result")

    iloc.connectTo("result", getSeriesByName, "data")
    getSeriesByName.connectFrom("data", iloc, "result")

    getSeriesByName.connectTo("result", word2vec, "data")
    word2vec.connectFrom("data", getSeriesByName, "result")

    headOperators = [source]

    Executor.execute(headOperators)
