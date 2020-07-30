# -*- coding: utf-8 -*-
import pickle

from utility.ArgParseFunction import ArgParseFunctionImpl
from utility.OperatorFactory import OperatorFactoryImpl

'''
这个类用来组装算子的 pipeline ；而不是算子的 DAG
'''


class PipelineAssemblerImpl:
    @staticmethod
    def assemble_pipeline(operator_args_list):
        # 获取udf
        udf = None
        if operator_args_list[0].startswith("--udfPath"):
            udf_path = operator_args_list[0]
            udf = pickle.load(udf_path)  # dict(name,func)
            operator_args_list = operator_args_list[1:]  # remove udfPath

        predecessor = None

        for op in operator_args_list:
            op_name = ArgParseFunctionImpl.parse_operator_name(op)
            op_arg = ArgParseFunctionImpl.parse_operator_args(op)
            create_op_func = (OperatorFactoryImpl.operator_factory())[op_name]

            current_op = create_op_func(op_arg, predecessor)
            predecessor = current_op

        return predecessor  # Operator 类
