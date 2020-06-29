## 补充测试参数示例
java JavaOperator参数
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

java JavaOperator参数（web case）
```
--udfPath=D:/executable-operator/executable-basic/target/classes/edu/daslab/exectuable/udf/TestSmallWebCaseFunc.class
--operator=file_source
--input=D:/executable-operator/executable-basic/src/main/resources/data/webClick.csv
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
--output=D:/executable-operator/executable-basic/src/main/resources/data/webOutput.csv
```
executable-java/src/main/resources/data文件夹下

网站信息表webClick.csv（点击者id，网站url）

黑名单表blackList.csv（网站一级域名，是否是黑名单）

输出是webOutput.csv（一级域名，点击数）

## quick-operator
简易operator集合，用于项目测试。暂不用担心目前的做法与“一个operator通过不同xml配置文件定义成不同operator”做法的矛盾，把不同operator类代码封装成不同jar包再用xml配置文件指明映射关系就行。

### TODO
* 用户定义函数
* image之间怎么串起来？怎么用这些image跑通case？
* case的设计改怎么优化？哪些case的潜在优化空间多？有哪些潜在优化空间？
* 如何部署到k8s，如何搭建通信框架

### 构建docker image
#### 1. 将xxxOperator.java打包成jar包
#### 2. 写Dockerfile
```xml
FROM java:8
WORKDIR /
ADD xxxOperator.jar xxxOperator.jar
EXPOSE 36728
CMD java -jar xxxOperator.jar
```

#### 3. 创建image
```shell
docker build -t xxxOperator:v1 .
```
#### 4. 创建container
```shell
docker run --name xxxOperatorOne xxxOperator:v1
```
