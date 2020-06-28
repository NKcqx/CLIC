#coding=utf-8
import argparse
from pathlib import Path

def apply(input_data, output_data, is_reverse):
    if type(input_data) is list: # 不确定要不要做类型判断
        return sorted(input_data, reverse=is_reverse)
    else:
        return []


parser = argparse.ArgumentParser(description="parser for `sort`")

parser.add_argument("Dinput_data_path", type=str) # 第一个参数，且必须
parser.add_argument("Doutput_data_path", type=str) # 第二个参数，且必须

parser.add_argument("--Dis_reverse", type=bool, default=False) # 是否倒序，可选参数，默认为从小到大排序
parser.add_argument("--Ddelimiter", type=str, default=" ") # 分隔符，可选参数，默认为空格

args = parser.parse_args()

input_data = []
with open(args.input_data_path, 'r') as lines:
    for line in lines:
        line_data = line.split(args.delimiter)
        input_data.extend(int(data) for data in line_data)
output_data = apply(input_data, "", args.is_reverse)

# Creating the directory where the output file will be created (the directory may or may not exist).
Path(args.output_data_path).parent.mkdir(parents=True, exist_ok=True)

with open(args.output_data_path, 'w') as output_file:
    for data in output_data:
        output_file.write("%i%s"%(data, args.delimiter) )