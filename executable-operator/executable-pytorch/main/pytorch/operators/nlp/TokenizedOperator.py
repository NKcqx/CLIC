
import traceback
from executable.basic.model.OperatorBase import OperatorBase
from executable.basic.utils.Logger import Logger


"""
Time       : 2021/8/6 2:56 下午
Author     : zjchen
Description:
"""


logger = Logger('TokenizedOperatorLogger').logger


class TokenizedOperator(OperatorBase):
    def __init__(self, ID, inputKeys, outputKeys, Params):
        super().__init__("TokenizedOperator", ID, inputKeys, outputKeys, Params)

    def execute(self):
        try:
            self.setOutputData("result", get_tokenized_imdb(self.getInputData("data")))
        except Exception as e:
            logger.error(traceback.format_exc())


def get_tokenized_imdb(data):
    """
    data: list of [string, label]
    """
    def tokenizer(text):
        return [tok.lower() for tok in text.split(' ')]
    return [tokenizer(review) for review, _ in data]
