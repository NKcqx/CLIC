# 新平台引入思想

每个语言（java / python）维护一个和平台无关的core（由我们自己实现），其中包含以下逻辑：
- 解析输入参数：负责解析命令行参数，包含每个平台公用的参数 和 各个平台自己的参数
- 解析operator：负责将参数中的dagPath指定的dag，解析成该平台的物理的可执行的operator
- 维持master client：负责将信息通知给master
- 执行dag：负责根据dagPath执行实际的逻辑

为了引入平台方便，对外仅暴露一个DagExecutor的接口，平台传入以下参数：
- 命令行参数：String[] args
- 构建operator的工厂类，其中包含所有operator的name和其实现的映射map：xxxOperatorFactory
然后直接执行即可


# 引入新的java语言平台的规范

在java core的基础上，任何人基于以下规范都可以实现引入一个新的平台(source code，不包含对应的xml文件)：

1.实现所有的operator，实现逻辑是：继承OperatorBase<InputType, OutputType>，然后实现其中的execute方法，其中
- 输入参数在：this.params
- 输出结果存放在：this.setOutputData

2.指定operator的name和实现的映射关系，实现逻辑是：继承OperatorFactory，在构造器中覆盖operatorMap，其中
- key是在xml文件配置中的operator的名字
- value是具体对应这个平台上实现的类的class对象

3.实现一个main函数，使用DagExecutor并传入参数即可：
```shell script
    DagExecutor executor = new DagExecutor(args, new XxxOperatorFactory());
    executor.execute();
```

4.可选：在平台执行前后，可能会有一些特殊的处理逻辑，可实现DagHook方法，并覆盖其中的preHandler和postHandler方法即可,
然后传入到DagExecutor中
