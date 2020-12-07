from basic.PytorchOperatorFactory import PytorchOperatorFactory
import random
import time
import Executor

"""
@ProjectName: CLIC
@Time       : 2020/11/24 下午3:35
@Author     : zjchen, NKCqx
@Description: 
"""


def RandomID():
    return random.seed(time.process_time())


if __name__ == '__main__':
     # 初始化OptFactory
    Factory = PytorchOperatorFactory()
    # 手动初始化所有Operator
    # 根据path生成dataFrame
    # train_source = Factory.createOperator("Source", RandomID(), [], ["result"],
    #                                 {"input_Path": "/Users/jason/Documents/Study_Study/DASLab/Cross_Platform_Compute/practice/torch_example/data/MNIST/processed/training.pt"})
    # test_source = Factory.createOperator("Source", RandomID(), [], ["result"],
    #                                 {"input_Path": "/Users/jason/Documents/Study_Study/DASLab/Cross_Platform_Compute/practice/torch_example/data/MNIST/processed/test.pt"})
    
    mnist = Factory.createOperator("TorchNet", RandomID(), [], ["result"], {
        "network": "operators.NNs.MNISTNet", # 相对路径，根目录为当前文件所在路径
        "data-path": "/Users/zjchen/IdeaProjects/CLIC/executable-operator/executable-pytorch/data/",
        "train": True,
        "lr": 0.01,
        "device": "cpu",
        "batch-size": 64,
        "epochs": 2,
        "gamma": 0.7,
        "log-interval": 10,
        "loss_function": "nll_loss",
        "optimizer": "Adadelta"
    })

    mnist_test = Factory.createOperator("TorchNet", RandomID(), [], ["result"], {
        "network": "operators.NNs.MNISTNet",
        "data-path": "/Users/zjchen/IdeaProjects/CLIC/executable-operator/executable-pytorch/data/",
        "train": False,
        "device": "cpu",
        "batch-size": 1000,
        "loss_function": "nll_loss",
    })

    mnist.connectTo(None, mnist_test, None)
    mnist_test.connectFrom(None, mnist, None)

    Executor.execute([mnist])

    # train_source.connectTo("result", mnist, "train-data")
    # mnist.connectFrom("train-data", train_source, "result")

    # test_source.connectTo("result", mnist, "test-data")
    # mnist.connectFrom("test-data", test_source, "result")

    # Executor.execute([train_source, test_source])


