# -*- coding: utf-8 -*-
import re

'''
这个类用来解析描述 pipeline 字符串；而不是描述 DAG 的字符串
'''


class ArgParseFunctionImpl:
    '''
    从原始参数中分离出各个operator及其参数
    '''

    @staticmethod
    def parse_operators(str_args):

        str_args = str_args.strip()

        idx = [m.start() for m in re.finditer('--operator', str_args)]

        operator_args_list = []

        # append udf path
        if str_args.startswith("--udfPath"):
            operator_args_list.append(str_args[0:idx[0]])

        start = idx[0]
        for ids in idx[1:]:  # remove first and last index
            if start != ids:
                operator_args_list.append(str_args[start:ids])
                start = ids
        operator_args_list.append(str_args[start:])

        # for e in operator_args_list:
        #     print(e)

        return operator_args_list

    '''
        提取各个operator的参数
    '''

    @staticmethod
    def parse_operator_args(operator_args):
        operator_args = operator_args.strip()  # remove extra whitespace
        assert operator_args.startswith("--operator="), "parameter illegal: should not contain operator name any more"
        parameters_map = {}  # (string,string)

        # 切分（k,v）
        parameters_list = operator_args.split("\n")
        for r in parameters_list:
            [(k, v)] = re.findall(r'--(.*?)=(.*?)$', r)
            parameters_map[k] = v
        return parameters_map

    '''
        提取operator name
    '''

    @staticmethod
    def parse_operator_name(operator_arg):
        start = operator_arg.index("=") + 1
        end = operator_arg.index("\n")
        op_name = operator_arg[start:end]
        return op_name


if __name__ == "__main__":
    arg = '''
--udfPath=/Users/edward/Code/Lab/executable-operator/executable-basic/target/classes/edu/daslab/exectuable/udf/TestWordCountsFunc.class
--operator=file_source
--input=/Users/edward/Code/Lab/executable-operator/executable-basic/src/main/resources/data/test.csv
--operator=filter
--udfName=filterFunc
--operator=map
--udfName=mapFunc
--operator=reduce_by_key
--udfName=reduceFunc
--keyName=reduceKey
--operator=sort
--udfName=sortFunc
--operator=file_sink
--output=/Users/edward/Code/Lab/executable-operator/executable-basic/src/main/resources/data/output.csv
'''

    ArgParseFunctionImpl.parse_operators(arg)
