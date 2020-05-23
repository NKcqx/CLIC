# IRDEMO：简易Workflow动态优化系统

## Usage
### **一、导入项目 & 加载依赖**
使用IDEA的 “Import Project” ，在打开的目录中选择 “pom.xml”即可导入项目并自动下载依赖【Eclipse同理】

### **二、运行Demo程序**
demo程序位于src/main/java目录下，导入项目后运行其中的main函数即可

### **三、使用API构建Workflow**
可参考 api.PlanBuilder中的（私有）函数接口，目前支持的运算有 map, sort, filter, collect。
由于该系统仅做流程演示使用，故上述运算操作并无太多实际作用。

首先生成PlanBuilder对象，使用Stream的形式构建workflow，构建完成后调用PlanBuilder的execute函数开始运行。

-------

## Documentation
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

