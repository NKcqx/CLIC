
## 设计文档

### 环境
python 3

### 输入示例
```
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
```



## 支持的DAG
* 支持多source 单sink。
* 支持存储operators结果。

udf存储的格式是:
```python
{
    "functionName":
        functionObject
}
```
udf序列化的格式是pickle