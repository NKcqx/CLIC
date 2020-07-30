# -*- coding: utf-8 -*-
from operators.FileSink import FileSinkImpl
from operators.FileSource import FileSourceImpl


class OperatorFactoryImpl:
    __operatorFactory = {}

    @classmethod
    def operator_factory(cls):
        cls.__operatorFactory["file_source"] = create_filesource
        cls.__operatorFactory["file_sink"] = create_filesink
        return cls.__operatorFactory


def create_filesink(op_arg, input_operators):
    return FileSinkImpl(op_arg, input_operators)


def create_filesource(op_arg, input_operators):
    return FileSourceImpl(op_arg)
