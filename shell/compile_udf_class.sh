#! /bin/bash
# 本脚本用来将udf的.java文件编译成.class文件
cur_dir=$(pwd)
echo ${cur_dir}
function compile_udf_class(){
    cd ../executable-operator/executable-basic/src/main/java/fdu/daslab/executable/udf
    cur_dir=$(pwd)
    echo "Now we are under the dir: "
    echo ${cur_dir}
    # 先判断原来是否已存在输出.class文件的目录
    rm -rf output-class
    # 再创建输出.class文件的目录
    echo "create output-class dir"
    mkdir output-class
    # 开始编译
    echo "begin compile"
    javac -d ./output-class -encoding UTF-8 TestSmallWebCaseFunc.java
    echo "end compile"
}
compile_udf_class
exit 0