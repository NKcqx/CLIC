import logging


class Logger(object):
    """
    Description:
        日志类，用来记录输出
    Attributes:
        构造参数：
        1. name: Logger的名字
        2. level: logger的级别
    """
    def __init__(self, name, level=logging.DEBUG):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(level)
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        ch.setFormatter(formatter)
        self.logger.addHandler(ch)

