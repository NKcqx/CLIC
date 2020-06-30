#! /bin/bash
# 本脚本用来将udf的.java文件编译成.class文件
cur_dir=$(pwd)
echo ${cur_dir}
function compile_udf_class(){
    cd executable-operator
    # 先判断原来是否已存在输出.class文件的目录
    rm -rf output-class
    # 再创建输出.class文件的目录
    echo "create output-class dir"
    mkdir output-class
    # 开始编译
    echo "begin compile"
    cd executable-basic/src/main/java/edu/daslab/executable/udf
    javac -d ../../../../../../../../output-class -encoding UTF-8 TestSmallWebCaseFunc.java
    echo "end compile"
}
compile_udf_class
exit 0