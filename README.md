# IRDEMO：简易Workflow动态优化系统

## Usage
### **一、导入项目 & 加载依赖**
使用IDEA的 “Import Project” ，在打开的目录中选择 “pom.xml”即可导入项目并自动下载依赖【Eclipse同理】

### **二、运行Demo程序**
Demo程序位于framework/src/main/java目录下，导入项目后运行其中的main函数即可

### **三、使用API构建Workflow**
可参考 *CLIC文档*
-------

## Documentation
详情参考 *CLIC文档*
### **API**
为用户提供统一的构建Workflow的接口，使用户无需关心如何创建Operator（例如如何传Data Channel、ID等复杂结构）即可构建Workflow

### **basic**
系统的基础组件，包括：
* 每个Operator的抽象类：为每种运算如sort、map等定义一个类，内部除了做具体运算外还包括ID、JavaBean等通用功能
* ExecutableOperator的接口：所有平台提供可执行Operator（即Executable Operator）时必须实现的接口，内部提供了一个所供不同平台实现自己计算方式的运行接口，由系统负责统一执行
* Platform的抽象：对所有底层平台的抽象，目前仅包括一个MappingOperator接口，用于各平台将抽象Operator映射到平台实现的Operator上

### **platforms**
IRDemo底层支持的各种平台，以SparkPlatform为例，主要包括：
* Operators：内部是Spark支持的各种运算符的实现
* SparkPlatform：继承自basic中提供的Platform，提供抽象Operator到Spark Operator的映射接口


## Spark Backend
本项目中间的语言（IR）可以用Apache Spark作为计算引擎。Spark提供各种丰富的physical operator。用户使用Spark作为后端，只需以下步骤：
* 将各个计算节点进行连接，存储到PlanBuilder中
* 将PlanBuilder作为参数，调用SparkPlatform.SparkRunner()，即可得到最终结果

## 可视化Workflow前端
### JSON格式介绍
格式介绍
```JSON
[
{
    "id": "",     //算子唯一标识
    "name": "",   //算子名称，指明算子如何计算
    "parameters": {  //指定算子的静态参数，如算法循环此时、join的predicate等
    },
    "incoming": [   //JSON数组，每个数组对象存储本节点的上一跳信息
    ],
    "outgoing": [  //JSONN 数组，每个数组对象存储本节点的下一跳信息
    ]
  },
]
```

实例
```json
{
    "id": "4",
    "name": "ProjectOperator",
    "parameters": {
      "predicate": "o_totalprice"
    },
    "incoming": [
      {
        "id": "3",
        /**
     * 给this的opt添加一个新的上一跳opt
     * @param incoming 上一跳opt
     * @param params_pair 指定和上一跳Opt的输出的哪个数据的key链接，格式为 <incoming.output_key, this.input_key>；为null时默认拿到其所有的输出
     * @return 当前已链接的incoming channel的数量，即代表有多少个上一跳了
     */
        "params_pair": {
          //对应 DataQuanta的outgoin接口
        }
      }
    ],
    "outgoing": [
      {
        "id": "5",
        "params_pair": {
          //对应 DataQuanta的outgoin接口
        }
      }
    ]
  },
```



