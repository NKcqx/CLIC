import traceback
from model.OperatorBase import OperatorBase
import torch
"""
@ProjectName: CLIC
@Time       : 2020/12/13 下午3:51
@Author     : zjchen
@Description: 
"""


class GetSimilarTokens(OperatorBase):
    def __init__(self, ID, inputKeys, outputKeys, Params):
        super().__init__("GetSimilarTokens", ID, inputKeys, outputKeys, Params)

    def execute(self):
        try:
            pass
            # self.setOutputData("result", self.getInputData("data")[self.params["value"]])
            # W = embed.weight.data
            # x = W[token_to_idx[query_token]]
            # # 添加的1e-9是为了数值稳定性
            # cos = torch.matmul(W, x) / (torch.sum(W * W, dim=1) * torch.sum(x * x) + 1e-9).sqrt()
            # _, topk = torch.topk(cos, k=k+1)
            # topk = topk.cpu().numpy()
            # for i in topk[1:]:  # 除去输入词
            #     print('cosine sim=%.3f: %s' % (cos[i], (idx_to_token[i])))

        except Exception as e:
            print(e.args)
            print("="*20)
            print(traceback.format_exc())
