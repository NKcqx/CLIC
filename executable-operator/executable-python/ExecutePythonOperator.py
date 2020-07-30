from utility.ArgParseFunction import ArgParseFunctionImpl
from utility.PipelineAssembler import PipelineAssemblerImpl
import sys


if __name__ == "__main__":
    arg=""
    for i,ele in enumerate(sys.argv):
        if i==0:
            continue
        arg=arg+"\n"+ele
    arg=arg.strip()
#     arg = '''
# --operator=file_source
# --input=test_input.csv
# --operator=file_sink
# --output=test_output.csv
# '''
    op_list = ArgParseFunctionImpl.parse_operators(arg)
    tail_op = PipelineAssemblerImpl.assemble_pipeline(op_list)
    tail_op.execute()
