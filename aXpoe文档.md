# AxPoe文档
*Automated Cross Platform Optimizaiton Engine*

#### 目录
* [1. 导言](#1)
* [2. 系统设计和组成](#2)
  * [2.1 系统主要流程](#2.1)
  * [2.2 系统架构](#2.2)
  * [2.3 系统设计和说明](#2.3)
* [3. 系统实现](#3)
  * [3.1 使用API构建Workflow](#3.1)
  * [3.2 实现和引入新的Operator](#3.2)
  * [3.3 Channel的实现和用法](#3.3)
  * [3.4 Visitor以及优化策略](#3.4)
  * [3.5 引入新的计算框架](#3.5)
  * [3.6 AxPoe中的任务部署](#3.6)

## 1、导言
近年来，大数据和人工智能相关技术被广泛应用于新一代数据处理任务中，大数据、人工智能已经成为推动创新和社会进步的基石。在大数据、人工智能领域存在着许多种不同的计算平台，每个平台都有其各自的优势，例如，*Flink* 能更好的支持流计算、*Graphchi* 在图计算方面效率很高、*Spark* 作为一个经典的大数据处理引擎也在业界得到广泛应用，*MPI* 适合运行高性能计算任务，*Tensorflow*用于处理深度学习任务。每个平台都有其专长，因此一个特定的计算任务需要为其选择合适的处理平台才能达到理想的效率。这要求相关人员不仅需要熟悉各个平台的特点、所擅长的场景，还需要掌握不同平台的用法，包括语法、API等，其中带来的学习成本很高。

同时，当前应用所处理的数据多样，任务复杂，常常需要基于一个或多个大数据及人工智能的计算平台。例如，一个广告点击预测的任务，对大量数据的清洗和整理的任务使用Spark平台，为访存密集型任务；同时，回归模型训练为计算密集型任务，适合使用MPI加速。这就常需要多平台相互合作，共同构建一个工作流（`Workflow`），来完成指定的任务。多计算平台的协同处理需要涉及到大量数据传输、格式转换等高开销的操作，且任务之间的执行顺序能够影响整体性能。然而在实际场景中，一般用户很难为计算任务选择最优的平台，人工设定的工作流往往难以达到最优性能，这其中可能存在着大量的性能提升空间。

如以下例子，回归模型预测广告点击中，分别进行1）数据清洗、编码，2）数据整理、连接列数据，3）生成矩阵，4）规则化，5）训练回归模型，5个基本算子组成工作流依次运行。然而，因为广告数据中所有广告和所有用户所生成的矩阵为稀疏矩阵，如果对其中数据做规则化操作，则需要访问大量为0的元素，因此一个更优的工作流实现是将规则化放在第一步之后执行，工作流变为1）数据清洗、编码，2）规则化，3）数据整理、连接列数据，4）生成矩阵，5）训练回归模型。重新调度算子后的工作流能够使得规则化的开销成数量级的减小，从而有效提升整体性能。

我们设计和实现了`AxPoe`，一个跨平台的处理框架，旨在解决上述问题。它会针对一个特定的工作流任务，自动化地为其选择最优的一个甚至多个平台来协同处理。对于组成一个工作流的多个算子，`AxPoe`会根据算子之间的关系，寻找优化机会，动态对算子进行调度、融合，选择合适的执行平台，并部署在`k8s`上运行。`AxPoe`被设计为具有着高可扩展性，能够非常方便地添加更多的算子和新的计算平台，并且能够灵活添加优化策略，用户只需要使用`AxPoe`提供的（和具体平台无关的）API来构建执行计划即可。

## 2、系统设计和组成

### 2.1 系统主要流程
<img src="https://s1.ax1x.com/2020/07/16/Urh2Yd.png" style="zoom:30%;" />

`AxPoe`系统提供一个中间语言描述/可视化界面的抽象，用户可利用系统中已实现的算子，用其构建执行特定任务的工作流。系统分为三个主要模块，分别是逻辑方案生成层，物理方案生成层，以及平台部署层。

具体来说，整个系统的执行流程具体由如下六步组成。

1. **用户使用API构建Workflow**：由用户创建单个计算节点、连接多个计算节点，生成工作流（Workflow）。
2. **系统将Workflow组织为Logical Plan**：系统将API中涉及的运算映射为对应的Operator，最后通过Channnel连接所有Operator生成逻辑计划（Logical Plan）。这里Channel代表着Operator间的数据传输方法，包括但不限于内存传输、文件传输、高速网络RDMA传输。
3. **系统动态优化Logical Plan**：系统对Logical Plan进行重写，包括算子融合（Operator fusion）, 算子调度（Operator pushdown / pullup）等，得到优化后的Logical Plan。其中系统对于Logical Plan的操作都经过Visitor进行处理和优化。
4. **系统拆分Logical Plan并为每部分选择最优运算平台**：系统根据优化后Logical Plan中不同Operator所进行的计算任务的特点为其选择最佳的平台，为所有Operator选择了计算平台，使用Visitor的方法为Logical Plan生成物理计划（Physical Plan）。
5. **系统将Physical Plan输出为Argo Workflow**：系统通过加载各个平台的描述文件将平台转化为Argo Workflow文件中的`template`字段，之后传入要在该平台下计算的Physical Plan作为Argo Workflow文件中的`Parameter`字段，最终得到YAML格式的*Argo Workflow*文件。
6. **系统将Workflow提交至K8S进行运算**

[![AxPoe执行流程](https://s1.ax1x.com/2020/07/14/UUQPIg.md.png)](https://imgchr.com/i/UUQPIg)

### 2.2 系统架构

AxPoe的结构主要包括以下4部分

**Core**

Core是AxPoe的核心模块，其中定义了Operator和Channel这两个构成Workflow的重要部件。它的主要功能为：

1. 提供Operator，通过读取配置文件来生成具有不同计算功能的Operator
2. 提供Channel，用于Operator间的数据传输、异质数据格式转换
3. 提供Planbuilder，利用Operator和Channel来构建Workflow（Logical Plan）
4. 提供Visitor基类，使开发者可基于此创建各种Visitor来访问Logical Plan
5. 其它必要插件，如Cost Model、XML Parser等

**Visitors**

该模块用于提供用于访问Workflow的各类Visitor，较为重要的Visitor有：

1. PlanScheduler，用于针对Logical Plan进行 *算子调度* 优化，如 *Operator pushdown / pullup, Operator fusion* 等
2. PlatformSelector，用于将Logical Plan转化为Physical Plan，其内部会为每个Logical Operator选择最优计算平台，将其转为Physical Operator
3. WorkflowGenerator，将整个Physical Plan划分为多个SubPlan，一个SubPlan中的所有Physical Operator都属于同一个计算平台，SubPlan将会被传入对应平台的Image中运行；

**Images**

该模块包含了所有独立的计算平台，属于底层模块，每个平台主要包含两部分：

1. Docker Image：每个计算平台都将被封装为Docker Image，在实际运行时被部署到K8S集群中，并由AxPoe将要运行的Physical Plan传入对应平台的Docker Image中
2. 平台的配置文件：AxPoe对不同计算平台的使用方法是通过读取平台对应的配置文件获得的，例如平台的计算性能、运行Docker Image所需的输入输出参数、环境变量等

**GUI / API**

提供交互式的UI或者API，让用户以描述性语言构建Workflow。



下图为AxPoe系统结构的主要模块：

<img src="https://s1.ax1x.com/2020/07/23/UqBoFA.md.png" alt="UqBoFA.md.png" style="zoom:67%;" />



### 2.3 系统设计和说明

#### 2.3.1 Logical DataType & Physical DataType

**Logical Data Type**

Logical DataType是AxPoe对所有计算平台上的数据类型做的统一抽象。因为不同计算平台上对数据类型的描述有所不同，例如32位 *int* 型数据在 *Java* 中为 "int" ；而在 C 中为 "int32_t"。因此为了在构建Workflow时能统一描述数据，AxPoe将常见的数据类型做了抽象，称为Logical DataType。

AxPoe目前所支持的逻辑数据类型如下表所示。这些数据类型都为抽象说明，使用于Logical Operator中，每个Logical Operator的输入输出类型都必须为以下抽象数据类型，系统按照名字进行识别。针对于每一个Physical Operator来说，均有所实现语言中与简单类型一一对应的数据类型，如INT64对应C语言中的int64_t，对应Java语言中的long。

| 简单类型 |
| ----  |
| INT  |
| FLOAT  |
| CHAR  |


| 复杂类型 |
| ----  |
| Matrix  |
| Array  |
| KeyValue |
| Relation |
| ...  |



|      |      |      |      |      |      |
| :--- | :--- | ---- | ---- | ---- | ---- |
|      |      |      |      |      |      |
|      |      |      |      |      |      |
|      |      |      |      |      |      |
|      |      |      |      |      |      |
|      |      |      |      |      |      |
|      |      |      |      |      |      |
|      |      |      |      |      |      |
|      |      |      |      |      |      |

**Physical Data Type**

Physical DataType 则是 Logical DataType 对应到具体计算平台上的数据类型。该表列出了AxPoe在各平台上的Physical Data Type，以及他们所支持的相应的Logical Data Type。需要指出的是一个Physical Data Type可以对应着多个Logical Data Type，比如RDD可以用来实现Matrix、Array、KeyValue、Relation等复杂数据类型。在系统实现中，如果一个Physical Operator的输入输出数据类型能够对应于其Logical Operator中相应的类型，那么该Physical Operator就可以用来Materialize该Logical Operator。AxPoe会存储不同Physical DataType间的转换函数，在进行跨平台数据传输时，AxPoe会在上述对照表中查找两个不同的平台上的数据类型，并对数据使用对应的转换函数，以此完成数据格式转换。


|  平台/语言   | Physical Data Type  | Logical Data Type(s)  |
|  ----  | ----  | ----  |
| Spark/Java  |  long  |   INT  |
| Spark/Java  |  double |  FLOAT  |
| Spark/Java  |  char  |   CHAR   |
| Spark/Java  |  JavaRDD<Matrix/Vector/Map>  |   Matrix/Array/KeyValue/Relation   |
| MPI/C |  int64_t  | INT |
| MPI/C |  double  | FLOAT |
| MPI/C |  char  |  CHAR |
| MPI/C |  void *  |  Matrix/Array/KeyValue/Relation |
| ...  |  ... | ... |

系统为了实现跨平台数据格式转换，定义了一个由Logical Datatype 转换为 Physical Datatype时的 “数据类型对照表”，如下所示：

| Logical | Java          | MPI             | Python  | Spark (in Java)        | C++11       |
| :------ | :------------ | --------------- | ------- | ---------------------- | ----------- |
| int     | int           | int32_t         | int()   | int                    | int         |
| long    | long          | int64_t         | -       | long                   | long        |
| double  | double        | float           | float() | Float                  | double      |
| boolean | Boolean       | uint8_t         | bool()  | Boolean                | bool        |
| string  | String        | char*           | str()   | String                 | string      |
| Vector  | List<?>       | -               | list()  | JavaRDD<Vector>(mllib) | vector      |
| Matrix  | List<List<?>> | int32_t\[\]\[\] |         | JavaRDD<Matrix>(mllib) | int\[\]\[\] |
| Tuple   | javatuples     | -               | tuple() | javatuples               | tuple       |

[javatuples](https://www.javatuples.org/index.html)

**计算平台**
系统所支持的平台以及计划支持的平台如下表所示：

| 已经支持的平台 |
| -------------- |
| Spark          |
| Java           |

| 计划支持的平台 |
| -------------- |
| Tensorflow     |
| SunwayMPI      |
| Flink          |

#### 2.3.2 Logical Operator & Physical Operator

运算符Operator分为Logical Operator 和 Physical Operator。Logical Operator是对各种运算功能的抽象，用于描述运算符的名称、参数列表等通用属性，和具体平台无关。Logical Operator是由用户所描述的Workflow转换而来，系统使用所实现的Visitor对Logical Operator所组成的Workflow进行整体性能优化并指定每个Logical Operator的运行平台。一个Operator可以在多个平台上实现，Physical Operator指的是Operator在某个平台上的的具体实现。

**Logical Operator**

Logical Operator是对各种运算功能的抽象，用于描述Operator的一些通用属性。对于一个特定的Logical Operator，它主要包含以下属性：

1. **Operator Name**：运算符的名称，如 `filter`, `sort`, `groupbykey`等等。
2. **输入参数列表**：Logical Operator计算所需的所有输入参数类型，其类型必须为Logical Data Type中的一个。
3. **输出数据列表**：Logical Operator的所有运算结果类型，其类型必须为Logical Data Type中的一个。
4. **Implementations**：该Logical Operator对应的所有Physical Operator，也即该Logical Operator在所有平台上的具体实现。

下图为Logical Operator的结构，其中Input / Output Data List 即为输入/输出参数列表，其值由Input / Output Channel提供。

<img src="https://s1.ax1x.com/2020/07/16/UrhuJs.png" style="zoom:25%;" />



**Physical Operator**

Physical Operator是Logical Operator在特定平台上的实现，即Logical Operator的Implementation，它主要借助于平台的API来实现具体功能。例如，抽象运算符 `filter`可在 *Java* 平台中使用`stream.filter`实现、而在 Spark 平台中可使用 `JavaRDD.filter`来实现。

对于一个特定的Physical Operator，它主要包含以下属性：

1. **Operator Name**：运算符名称
2. **Platform Name**：所处平台的名称
3. **Language**：编程语言，如Platform同为Spark，语言可以为Java、Python、Scala等
4. **输入参数列表**：Physical Operator计算所需的所有输入参数类型，其类型必须为Physical Data Type中的一个。
5. **输出参数列表**：Physical Operator的所有运算结果类型，其类型必须为Physical Data Type中的一个。
6. **优化参数**：Physical Operator在当前平台中运行时所需的额外参数，例如`sort`在 *Spark* 平台中需要指定 *PartationNum*，可用于Workflow的调度、性能优化。

   

#### 2.3.3 Logical Channel & Physical Channel

Workflow中的节点是由Operator组成的，而边则使用Channel实现。Channel定义了不同Operator之间的交互机制，它不仅描述了Operator间的依赖关系，还包括Operator间数据传输、格式转换等相关内容。

和Operator类似，Channel也分为Logical Channel 和 Physical Channel。Logical Channel仅用于表达Logical Operator之间的关系，如依赖关系、输入-输出数据的映射等，因为Logical Operator和平台无关，因此Logical Channel也和平台无关；Physical Channel用于表达Physical Operator之间的关系，除了上述的链接关系外还包括：异质数据的格式转换、数据传输协议、具体传输协议的参数等。

**Logical Channel**

Logical Channel用于表达Logical Operator之间的关系，它包括：

1. **Source / Target Operator**：Channel连接的两个Logical Operator
2. **Source Logical Data Type**：Channel的输入Logical Data Type
3. **Target Logical Data Type**：Channel的输出Logical Data Type
2. **输入-输出数据的 Key Pair**：当Source - Target Operator存在依赖关系，即Target Operator所需的输入数据是Source Operator的输出数据时，需要使用一个Key Pair声明上述关系。Key Pair的第一个Key为Source Operator输出数据的Key，第二个Key 为Target Operator输入数据的Key

因为用于链接的Logical Operator和平台无关，所以Logical Channel也和平台无关。这意味着Logical Channel无需关注两端Operator的数据是否是异质的（即无需关注数据格式转换的问题）也无需关注数据文件将如何在两个Operator之间传输。对于用户来说，上述链接Operator的过程和函数间的相互调用很类似： Source函数调用Target函数并把Source的计算结果作为参数传给Target，而函数内部只需要关注如何对传入的数据进行计算并将结果放入指定位置。

**Physical Channel**

Logical Channel仅描述了Operator间的链接关系和两端Operator要传输的数据，并不涉及两端Operator的数据格式、数据传输所使用的协议（例如UDP, RDMA）。除此之外，当Logical Operator被一一映射到不同平台上成为Physical Operator后，相邻的两个Physical Operator很有可能处在不同的计算平台，而不同平台所使用的数据类型也是不同的，此时称两个Operator所使用的数据是异质的，而异质数据的传输需要额外进行格式转换。因此，对于Physical Operator间的链接需要使用Physical Channel。

Physical Channel用于表达Physical Operator之间的关系，它在Logical Channel的基础上还包括了：对异质数据进行格式转换、选择数据传输方式、生成相关参数等功能。

给定一个Physical Channel，主要包含以下属性：

1. **Channel ID**：该Channel的唯一标识符
2. **Source Physical Data Type**：Channel的输入Physical Operator所使用的数据类型
3. **Target Physical Data Type**：Channel两端Physical Operator所使用的数据类型
3. **数据传输方式**：RDMA、Socket、HDFS、内存
4. **相关参数**：传输所需的控制信息



以下图中涉及的数据传输为例：

<img src="https://s1.ax1x.com/2020/07/21/U5MhqA.md.png" alt="U5MhqA.md.png" style="zoom:70%;" />

以上图中的DAG为例，数据共经过了以下几个阶段的传输

1. **Spark平台** 读取数据并传输到 **Spark平台** 的 Sort Operator中。该阶段为**同一平台内同质数据的传输**，因此不需要进行数据格式转换；并且同一平台内的数据可直接在内存间传输，故此时使用 **RDMA Channel**进行连接。
2. 将**Spark平台**排序后的数据传输到 **MPI平台**上的 Filter Operator 和 Map Operator。该阶段涉及 **两种平台间异质数据的传输**，平台间的数据无法直接传输并使用。假设数据量较大，系统使用 **Socket Channel**进行连接。此时，Socket Channel不仅需包含维护 *Socket* 连接所需的两端进程的IP地址、端口；还需要将Spark平台中RDD形式的数据转换为 字节流，并且传输后再将字节流转换为MPI中的数据结构。
3. **MPI**平台将Filter 和 Map的结构传输到 **MPI平台**的Collect Operator，该阶段为**同一平台内同质数据的传输**，过程同Spark -> Spark间的传输，不再赘述。

#### 2.3.4 Workflow

用户要执行的全部运算任务称为Workflow，它由许多子运算任务相互链接组成，例如下图所示：


<img src="https://s1.ax1x.com/2020/07/20/U44uOU.md.png" alt="U44uOU.md.png" style="zoom:70%;" />

Workflow整体结构是一个有向无环图（以下简称DAG）。

DAG中的节点表示（子）运算任务，由 *Operator* 和其参数组成，其内部负责对接收的数据做相应计算，在2.3.2节中已对*Operator*的功能做了阐述。

DAG中的边表示运算任务之间的依赖关系，不同的计算任务会有先后顺序或依赖关系，依赖关系又包括一对一、一对多等，而这些是在DAG中通过 *Channel* 链接两个或多个存在上述关系的计算任务来实现，*Channel* 提供了数据类型转换、存储格式转换等功能。在2.3.4节中已对 *Channel* 的功能做了阐述。


#### 2.3.5 Workflow的优化重构策略

由用户编写的Workflow其结构不一定是最优的，因此在实际运行前AxPoe首先需要对Workflow进行优化重构。AxPoe主要使用了以下几种优化策略。

**Operator Fusion**

Operator fusion基于的设想是，通过融合两个或多个`Operator`来减少`Operator`之间数据传输、封装带来的性能损失。

以 `map` fusion为例：两个相邻的 `map ` Operator 分别执行了不同的`UDF`，但由于是两个`Operator`，不仅在优化时需要AxPoe分别计算代价并选择平台，而且在运行时需要进行两次封装、解封。因此我们合并两个`map` Operator，即将两段`UDF`合并放到一个`map` Operator中来省去上述多余的操作。如下图所示：

<img src="https://s1.ax1x.com/2020/07/16/UB0vb6.png" alt="map operator fusion.png" style="zoom: 67%;" />

**Operator Pushdown**

不同的`Operator`对数据元素的处理、性能消耗上有各自的特点，例如 `filter`不改变元素的值仅改变集合中元素的数量、`join`改变元素的值、数量且计算代价高昂、`sort`仅改变元素的相当位置，计算代价取决于数据量。

因此一个朴素的想法就是，在Logical Plan中将诸如`filter`的运算符进行pushdown（更早计算），将`sort`、`join`运算符进行pullup（更晚运算），这样可做的在不影响计算结果的前提下尽量减少运算时间。如下图所示：

<img src="https://s1.ax1x.com/2020/07/16/UD5vB8.md.png" alt="UD5vB8.png" style="zoom: 50%;" />

#### 2.3.6 最优平台选择

用户构建的Workflow中的每个节点实际是由Logical Operator构成的，而该Workflow的实际运行是需要依赖于具体平台的。因此Workflow还需要完成由 Logical Operator 到 Physical Operator的映射，也即 **Operator的平台选择**。

从2.3.2节中可知，Logical Operator可能对应于多个Physical Operator，即该Logical Operator有多个平台的实现。例如下图所示，一个抽象的`sort`运算符（图中左侧蓝色的 "Sort"）在 Java, Spark, MPI平台上均有对应实现。

<img src="https://s1.ax1x.com/2020/07/16/UrhKWn.png" style="zoom:30%;" />

当我们把上图中所有的抽象运算符均映射到（多个）具体平台上时，便可得到类似下图的结构：

![U4L5Af.md.png](https://s1.ax1x.com/2020/07/20/U4L5Af.md.png)

而AxPoe会针对当前任务，为每个Logical Operator从所有平台的实现中选择最优实现。以上图中经过平台映射后的Workflow为例，为其选择了最优平台后将得到类似下图的结构：

<img src="https://s1.ax1x.com/2020/07/21/U5MhqA.md.png" alt="U5MhqA.md.png" style="zoom:50%;" />

## 3、系统实现

### 3.1  使用API构建Workflow


根据2.3.1节中的内容可知，在构建Workflow时用户首先需要分别创建节点，然后通过连接各个节点来构造DAG结构。两步操作分别通过两个类实现：`DataQuanta`、`Planbuilder`。其中`DataQuanta`是对DAG中节点的封装，主要负责节点的创建和链接；`Planbuilder`负责维护和管理Workflow，包括存储DAG结构、维护Workflow运行状态、维护各类`Visitor`等，是用户监控和管理Workflow的工具类。以下将分别介绍上述两个类的功能和使用方法。

#### DataQuanta

`DataQuanta`是对DAG中的节点的封装，用于向用户提供节点创建、链接的接口。

##### 节点创建

```java
public static DataQuanta createInstance(String ability, Map<String, String> params)
```

* `ability`：1.1节中提到的运算符的名称，例如`map`, `filter`,`groupby`等。所有可用的运算符定义在配置文件：*OperatorTemplates/OperatorMapping.xml* 中。
* `params`：K-V形式的运算符所需的参数，例如`map`运算所需的`UDF`， `join`运算所需的`Key`。每个运算符所需的参数定义在配置文件：*Operator/xxOperator.xml*中。

> 之所以说`DataQuanta`是对节点的封装，是因为在之后几章可知，DAG中的每个节点实际上是一个`Operator`类。但因为该类属于系统内部类，其创建和使用较复杂，并不适合用户直接接触。因此在API中对`Operator`进行了封装，向上提供了更简单易用的接口。

#### Planbuilder

`Planbuilder`**是用户用来监控和管理DAG的工具类**，<u>对于用户来说</u>它主要包含以下几个功能

**初始化计算任务**

创建一个新的计算任务时，**用户需要传入配置文件来初始化系统上下文**，默认使用的配置文件为*default-config.properties*，内容如下：

```properties
# 系统加载以下配置文件来分别获得所有可用的Operator及Platform
operator-mapping-file = OperatorTemplates/OperatorMapping.xml
platform-mapping-file = Platform/PlatformMapping.xml
# 系统将自动编译该目录下的文件 并将编译后的.class文件放到相同的路径下（之后多语言时应该还要有所区分）
udfs-path = udf/TestSmallWebCaseUDFs.java
# 系统会将生成的Argo Workflow保存在该路径下
yaml-output-path = /tmp/irdemo_output/
# 创建Argo任务时的任务名前缀
yaml-prefix = job-
```

此外，我们规定所有的Workflow都要以一个数据源开始。因此在开始一个新的计算任务时，用户需要首先使用`Planbuilder`声明数据源。

**声明数据源**

数据源实际上就是一个 `source`运算符，同其他运算符一样`source`也有对应的参数。下面的声明数据源的接口只是代替了直接创建`source DataQuanta` 的工作，起到初始化数据源的效果。

```Java	
public DataQuanta readDataFrom(Map<String, String> params)
```

* `params`：`source`的参数列表
* 返回值：包含`source`运算符的`DataQuanta`

**运行Workflow**

用户在构建完Workflow后便可调用`execute`接口开始实际运算

以上是`PlanBuilder`面向用户提供的功能。此外，`Planbuilder`在**<u>系统层面上</u>**还负责一项重要的工作，即调用各类`Visitor`来访问DAG。例如：

* `optimizePlan()`接口负责调用`OptimizeVisitor`实现对DAG结构的优化重组
* `executePlan()`接口负责调用`WorkflowVisitor`将Physical Plan转化为 Argo Workflow

本节介绍了Workflow的概念及如何使用API构建Workflow，需要指出的是，**<u>Workflow的概念是完全面向用户的，即它的定义是为了让用户能够使用系统，和AxPoe的具体实现没有关系</u>**。

**Logical Plan**

在系统中，由用户创建的Workflow可直接转译为Logical Plan。区别在于，Workflow中的节点是`DataQuanta`，而Logical Plan中的节点是`Operator`，两者均用于描述一个运算符。因为系统在优化、转译DAG时操作的是`Operator`而不是`DataQuanta`。而称一个"Plan"为"Logical"是因为，每个运算符在最终实际运行时需依赖某个特定的平台。而此时的Plan并没有经过优化和平台选择，每个`Operator`无法实际运行，因此称此时的`Operator`构成的Plan为"Logical Plan"。

### 3.2 实现并引入新的Operator

本章以实现一个`MapOperator`为例，从Logical 到Physical 来介绍如何为AxPoe引入一个新的Operator。

我们已知Operator分为Logical Operator和Physical Operator。Logical Operator是系统对运算符的抽象，用于提供给用户统一的描述；Physical Operator则是Logical Operator在各个平台上的具体实现（Implementation）。因此，为AxPoe创建一个新的Operator时首先需要创建Logical Operator并向系统注册，然后再在某一平台上具体实现它。

#### 3.2.1 创建Logical Operator

AxPoe中所有的Operator均是通过加载描述文件中的属性来创建的，因此区分不同的Operator仅通过在描述文件中提供不同的信息。创建Logical Operator主要需要以下几步：

**1. 为Logical Operator编写描述文件**

描述文件中包含某一个具体`Operator`的信息，如前面提到的参数列表、计算性能、元信息等。以`map`为例，其描述文件 *MapOperator.xml* 如下：

```xml
<Operator ID="MapOperator" name="MapOperator" kind="transformer">

    <platforms>
        <platform ID="java">
            <path>Operator/Map/conf/JavaMapOperator.xml</path>
        </platform>

        <platform ID="spark">
            <path>Operator/Map/conf/SparkMapOperator.xml</path>
        </platform>
    </platforms>
    
    <parameters>
        <parameter kind="input" name="udfName"  data_type="string" required="true"> </parameter>
        <parameter kind="output" name="result" data_type="string"> </parameter>
    </parameters>
    
</Operator>
```

几个标签的含义如下：

* **Operator**：`Operator`根标签，用于声明一个新的Operator对象，标签中的`name`, `ID`等属性为该Operator的元信息
* **platforms**：提供从Logical Operator到Physical Operator的映射
  * Platform：声明一个Physical Operator所处的平台
  * Platform.path：声明在当前平台下Physical Operator的描述文件路径，用于系统加载
* **parameters**：`Operator`的参数列表，定义了参数的名称、类型等必要信息
  * Parameter.kind：区分参数是输入参数还是输出参数，取值范围：["input", "output"]
  * Parameter.name：参数名，String类型
  * Parameter.data_type：参数值的数据类型，取值范围：[尚未使用，可随意]
  * Parameter.required：是否必须（由用户）传入，取值范围：["true", "false"]
  * Paramter.default：参数的默认值

系统中的`Operator`类会解析上述XML文件，加载各个属性到当前创建的`Operator`实例中，此时便通过描述文件构造了一个有实际含义的Logical Operator。

**2. 向AxPoe注册该Operator**

在提供了Map Operator的描述文件后，还需要向AxPoe注册该Operator使其能够被系统调度使用。而系统是通过加载一个名为 *OperatorMapping.xml* 的配置文件来取得所有可用的Operator。所以，向AxPoe注册新的Operator只需要在配置文件中添加对应信息即可， 在*OperatorMapping.xml* 中添加的内容如下：

```xml
<mappings>
  ...
  <pair>
    <ability>map</ability>
    <template>Operator/Map/conf/MapOperator.xml</template>
  </pair>
</mappings>
```

标签含义如下：

* **Mappings**：根标签，用于声明一个Operator列表
* **Pair**：用于声明一个Operator，使用类似K-V的方式表示一个Operator，其中Key为*ability*标签， Value为*template*标签
  * ability：表示Operator的功能（或者说名称），标签内的值将直接用于在API中创建该Operator
  * template：该Logical Operator的描述文件的路径

系统中的`OperatorFactory`类会解析上述XML文件，加载所有 *pair* 到内存中用于根据指定 *Key* 创建 Operator的时候使用。

**3. 使用AxPoe的API创建Logical Operator**

下面的代码片段展示了用户使用API来创建我们刚刚声明的 Map Operator：

```java
DataQuanta mapNode = DataQuanta.createInstance("map", new HashMap<String, String>() {{
	put("udfName", "mapFunc");
}});
```

在 `DataQuanta.createInstance`函数中传入了两个参数：

1.  "Map" ，即为我们在 *OperatorMapping.xml* 中添加的 "ability"字段里的值；

2.  HashMap，要传入Map Operator的参数列表，AxPoe在找到 "map"对应的Operator后会加载其描述文件并找到对应的参数列表；HashMap中传入的 "udfName"即为在*MapOperator.xml*中添加的  "parameter"字段

#### 3.2.2 创建Physical Operator

在使用API创建了Logical Operator后，系统需要再将其映射到一个具体平台上才能实际运行，即将Logical Operator映射为Physical Operator。为此，我们还需要以下两步：

**1. 在特定平台上实现该Operator**

这主要通过调用不同平台的API完成，实现的代码中需要定义该Operator接收的参数、计算过程、返回值等所有实际计算时所需的内容。以Spark平台为例，下面的代码片段为使用Spark的API定义 Map Operator：

```java
@Parameters(separators = "=")
public class MapOperator implements BasicOperator<JavaRDD<List<String>>> {
    // 通过指定路径来获取代码的udf
    @Parameter(names = {"--udfName"})
    String mapFunctionName;

    @Override
    public void execute(ParamsModel<JavaRDD<List<String>>> inputArgs, ResultModel<JavaRDD<List<String>>> result) {
        MapOperator mapArgs = (MapOperator) inputArgs.getOperatorParam();
        @SuppressWarnings("unchecked") final JavaRDD<List<String>> nextStream = result.getInnerResult()
                .map(data -> {
                    // 因为无法序列化，只能传入可序列化的ParamsModel
                    FunctionModel functionModel = inputArgs.getFunctionModel();
                    return (List<String>) functionModel.invoke(mapArgs.mapFunctionName, data);
                });
        result.setInnerResult(nextStream);
    }
}
```

* execute函数接口：每个*Operator* 需要实现`execute`接口，在`execute`中**定义该*Operator*的计算过程**；该接口接收两个参数：`inputArgs`、`result`
  1. `inputArgs`：`ParamsModel<JavaRDD<?>>`类型，对输入参数的封装，包含该Opt进行计算时需要用到的所有输入参数，如输入数据、UDF等。具体用法见下面对 `ParamsModel`的介绍
  2. `result`：`ResultModel<JavaRDD<?>>`类型，对Opt计算结果的封装，具体用法见下面对 `ResultModel`的介绍

* `ParamsModel<?>`：对所有Opt输入参数的封装，由子系统（`executable-spark`的主程序）负责封装，在编写Opt时只需要调用其以下两个方法：
  * `getFunctionModel`：获取UDF（以UDFModel的形式）
  * `getOperatorParam()`：获取除UDF外的所有其他参数。由于该函数内部是通过访问当前Opt的成员变量的方式获取参数的，因此需要将返回值进行类型转换，转换为当前Opt的类型，可参考下面的例子。

> 所有输入参数（包括UDF Name），都需要同时在XML和该Opt的成员变量中定义才可在此处获得

* `ResultModel<?>`：对输出结果的封装，是该平台内所有Opt之间数据传输的媒介。
  * `getInnerResult()`：获取内部数据，即获取上一跳Opt的输出结果
  * `setInnerResult()`：设置内部数据，即设置当前Opt的计算结果

* 参数成员变量：Operator的参数需要定义为成员变量并使用 `@Parameter`注解声明其为参数；其中的`name={xxx}`表示在命令行参数中对应的名称，详情可参考 [JCommander Parameter](https://jcommander.org)。

> 需要指出的是，具体平台上的代码实现和AxPoe是完全解耦的。AxPoe实际并不关心Map在Spark上是如何实现的，例如其中的`execute`接口如何调用、`ParamsModel`是什么等。这里贴出代码仅是为了以Spark平台的实现为例展示整体开发流程。



**2. 编写Physical Operator的描述文件**

**AxPoe对Physical Operator使用仅限于根据其描述文件中的属性来生成对应参数**，因此在Spark平台上实现了Physical的Map Operator后还需要向系统提供Physical Operator的描述文件。**该描述文件对应于Logical Operator的配置文件中的 `platforms.platform.path`字段**。上述代码片段对应的 *SparkMapOperator.xml* 文件的内容如下：

```xml
<Operator ID="MapOperator" name="MapOperator" kind="shuffler">
    <platform ID="spark">
        <language>java</language>
<!--  cost是该operator的cost，此处先简单设定-->
        <cost>2.99901</cost>
    </platform>
</Operator>
```

文件中的 `cost` 字段描述了MapOperator在Spark平台上的性能，AxPoe会比较MapOperator在各个平台上的cost来选择是否将将 Map Operator映射到Spark平台。 



### 3.3 Channel

#### 3.3.1 Logical Channel的创建和使用

Operator间的链接是有向的，用户在API中使用 `outgoing`接口链接下一跳，使用`incoming`接口链接上一跳。此外，在2.3.3节的Logical Channel中提到，创建Channel时还需要提供两端Operator参数的 Key Pair。下面的代码片段是使用`outgoing`为`MapOperator`添加下一跳：

```java
 mapNode.outgoing(reduceNode, new HashMap<String, String>(){{
   put("outputData", "inputData");
 }});
```

其中，"outputData"为`MapOperator`计算结果的Key；"InputData"为`reduceOperator`需要的输入数据的Key

#### 3.3.2 Physical Channel的实现

Physical Channel是实现Operator之间数据传输、格式转换的重要一步。在生成Physical Plan的时候，Logical Channel会被转译生成为Physical Channel，包括Materialize两端的Physical Data Type。这里，针对跨平台中数据抽象、数据格式不一致的现象，Physical Channel会进行相应的数据处理和转换，以连接两个Operator。

**数据抽象**：【根据Physical Data Type】如果是跨平台的Operator连接，那Operator所使用的数据类型可能发生改变，如RDD和Stream，这时候Physical Channel应该调用相应的转换函数，对平台间的数据抽象进行类型转换。

**数据格式**：【根据Logical Data Type】如果两个Operator的数据格式不同，那么同样需要根据Logical Data Type的类型进行相应的转换，如从Row-Store转换为Column Store。

**数据传输**：Physical Channel可实现多种底层数据传输方式，包括但不限于内存、文件、Socket、RDMA。

因此，Physical Channel作为连接Operator进行平台内和跨平台上数据传里的关键，不但需要考虑数据抽象、数据格式等数据问题，还需要实现相应的数据传输机制。



### 3.4 Visitor以及优化策略

#### 3.4.1 Visitor简介

Visitor是一种设计模式，其主要优点是：“它可在不改变数据结构的情况下为其定义新的Operation”。关于Visitor设计模式的具体概念可参考[Wikipedia](https://en.wikipedia.org/wiki/Visitor_pattern)，在此不再赘述。

在AxPoe中，Visitor的使用主要是为了在DAG上进行多种操作的同时保持与DAG的解耦，例如平台选择、优化、转译等操作均可在不修改DAG（即Operator）的前提下完成。

#### 3.4.2 遍历DAG

AxPoe提供了多种对DAG的遍历方式，包括：DFS、BFS、Topological，分别由不同的类来实现。以BFS遍历为例，它对应于 `BFSTraversal`，核心代码如下：

```java
public Operator nextOpt() {
  if (this.hasNextOpt()) {
    Operator res = this.queue.poll(); // 队列头部元素
		// 遍历所有channel，找到子节点并push到队列中
    for (Channel channel : res.getOutputChannel()) {
      this.queue.add(channel.getTargetOperator());
    }
    return res;
  } else {
    return null;
  }
}
```



#### 3.4.3 Visitor示例

下面以`PrintVisitor`为例，该Visitor实现遍历DAG并打印节点信息的功能:

```java
@Override
public void visit(Operator opt) {
  if (!isVisited(opt)) {
    this.logging(opt.toString());
    this.visited.add(opt);
  }

  if (planTraversal.hasNextOpt()) {
    planTraversal.nextOpt().acceptVisitor(this);
  }
}
```
其中：

* `public void visit`是对 `Visitor `基类中抽象接口的实现，它接收一个`Operator`并在内部由不同的`Visitor`子类定义对它的处理方法。
* ```this.logging(opt.toString())```对节点进行打印。
* ```planTraversal```负责遍历DAG，其内部实现了DFS, BFS, Topological等多种遍历方法；这里的`planTraversal`使用BFS，其`hasNextOpt()`方法用于返回当前节点在BFS时的下一个节点。
* `acceptVisitor(this)`用于令Visitor迭代地访问BFS中的下一个`Operator`。因此，系统只需要为`PrintVisitor`传入DAG的根节点即可，其内部将使用该方法迭代地遍历整个DAG。


#### 3.4.4 对Logical Plan进行优化

Visitor的一个主要任务是对Logical Plan的优化，自动化地根据不同平台的特点和运算量为不同运算任务选择最佳平台。优化重构了Logical Plan之后，需要为每个`Operator`选择最佳运算平台。在选择了运算平台后，每个`Operator`便可以被实际执行，此时的Logical Plan就转化为了**Physical Plan**。
不同的 `Operator` 所执行的计算任务不同，对应的最佳平台也不同。**AxPoe基于cost model综合考虑、计算任务的*selectivity*、平台配置等参数来选择最佳平台**。

**Selectivity**

数据库中的概念，在此用于粗略估算需要计算的数据量。计算公式：

```c
Selectivity of index = cardinality/(number of rows) * 100%
```

**平台配置**

不同平台的硬件参数，主要包括：

1. 集群节点数量(machines)
2. 单个节点的核心数(cpu-core)
3. CPU主频(cpu-hz)
4. 常数

**Cost Model**

AxPoe从运行日志中学习cost model各个输入的权重参数，训练完成后便可用于平台选择。

> 以上对选择平台的讨论也只是为了实现方案的初步设想，当前阶段我们只是在各个平台的Operator的配置文件中指定了一个固定的cost，在为Operator选择平台时直接选择具有最小cost的平台。



### 3.5 引入一个新的计算框架

从上文创建Physical Operator中可以看到，一个平台的Physical Operator编写好后只需要通过XML配置文件通知系统即可。实际上，一个完整的计算平台和系统（framework）的耦合度也很低，只需要通过命令行参数就可实现framework和平台的交互。首先介绍引入一个平台时所涉及的概念：

* **一个平台实际上是一个完整的、可独立运行的docker image。该平台上实现的全部Operator可看作一个“运算库”，被包装到了Image中**
* 平台内除了各种Operator外，还需要提供一个类似 `Composer` 功能的类，该类用来**接收由命令行参数传递的Workflow、内部解析命令行参数并用自己的Operator来实现对应的Workflow**
* 而平台（image）内，各个Operator之间如何传递参数、数据和framework完全无关，可自由定义
* Image对外接收传递数据时，需要通过两类Operator实现，分别是：`FileSource`、`FileSink`
  * `FileSource`用于从（csv）文件中读取特定格式的数据，并转换成该平台上所使用的数据类型。
  * `FileSink`用于将平台内最终的计算结果（数据）以特定格式写入文件中，供framework再提供给其他平台

和编写一个Physical Operator的过程类似，引入一个新计算平台一共也分为两步：

1. 编写一个独立的子系统，并将其打包为可docker image
2. 创建相应的配置文件并将其注册到framework中

下面将分别介绍这两步

**一、Source Code**

即一个能独立运行的子系统，包括但不限于以下几部分

1. 各类Operator的集合
2. Operator内部交互时涉及的数据结构，如前面在SparkMapOperator中提到的`ParamModel`、`ResultModel`等
3. `Composer`，用于接收命令行参数，并用Operator构造对应的Plan（可用入口主函数充当）
4. 一个能根据UDF的函数名，找到并调用对应UDF的工具类。在上面Spark的例子中，该功能由`ParamModel`完成，可参考相应代码

其中，唯一需要遵循统一标准的就是 ：

1. `Composer`接收的命令行参数，其格式由系统定义
2. `FileSource`、`FileSink`中读写的数据格式



> 另外值得一提的是**UDF的传递问题**，目前的做法是：将Plan所需的所有UDF统一写在一个类中，并将其编译为.class文件(在C/C++中可以是.dylib或.dll），以动态链接库的方式挂载到各平台的Image当中。当一个Operator需要传入UDF时，只需传入UDF的函数名称和参数值即可，计算平台会从挂载的动态链接库中查找对应函数名称的实现。

**二、将Source Code编译、打包为.jar、.exe...再封装为docker image**

**三、编写平台的描述文件**

该描述文件的性质同前面Operator的配置文件类似，以下代码片段展示了Spark平台的描述文件：

```xml
<platform>
  <docker-image>executable-spark:v0</docker-image>

  <execution>
    <environment>/bin/sh -c</environment> <!--  -c: 从string中读取命令  -->
    <executor>/spark-runner/bin/spark-submit</executor>
    <args>
      <!-- 平台级运行时参数 由用户或系统填入 -->
      <!-- 按序读取、写入 -->
      <arg name="--master" required="true" with-name="true">k8s://https://10.141.221.219:6443</arg>
      <arg name="--deploy-mode" required="true" with-name="true">client</arg>
      <arg name="--name" required="true" with-name="true">spark-running</arg>
      <arg name="--class" required="true" with-name="true">fdu.daslab.executable.spark.ExecuteSparkOperator</arg>
      <arg name="--conf=spark.executor.instance" required="true" with-name="true">5</arg>
      <arg name="--conf=spark.kubernetes.container.image" required="true" with-name="true">spark:v0</arg>
      <arg name="package" required="true" with-name="false">executable-spark.jar</arg>
      <arg name="--udfPath" required="true" with-name="true"> </arg>
    </args>
  </execution>

  <!-- 平台自定义属性，framework仅负责传递，不进去看都有什么 -->
  <properties>
    <property name="master">local[2]</property>
  </properties>
</platform>
```

其中各个字段的含义如下：

各个Field的含义：

* `name`：平台名称，用于framework实现name -> platform的映射
* `platform`：一个平台的实现，包含其所有属性及参数
  * `docker-image`：平台打包为docker image后的名称，格式: repo/image-name:version，如：`store/splunk/universalforwarder:7.3`
  * `execution`：Image运行时的各类参数
    * `environment`：平台的运行环境，如 /bin/sh -c 表示在image内调出命令行。（还没想到其他的）
    * `executor`：解释器、应用程序等，如Java平台下为`java -jar`、python中为`python`、Spark中为`spark submit`等
    * `args`：解释器和平台的运行参数，如MPI中的 `-n`、平台对应的可执行文件`alchemist`、 平台所需的运行参数`--udfPath`等
      * `arg`：参数的实际定义
        * `name`：参数名称
        * `required`：是否必须传入
        * `with-name`：在生成命令行参数时是否需要加入`name`。例如加入时，生成的命令行参数格式为：`--name=value`；不加入时仅为`value`
        * `default`：参数的默认值
  * `properties`：用来描述平台的其他属性的集合，例如 集群规模、节点核心数等描述属性用于framework估算该平台的性能



### 3.6 AxPoe中的任务部署

#### 3.6.1 AxPoe中的计算平台

在当前的实现中，一个计算平台（如Spark, MPI）在AxPoe中是一个完整的、可独立运行的docker image；Image中不仅包含计算平台的运行环境，更重要的是还包含该平台实现的所有 (Physical) Operator。

因为所有的计算任务最终都会部署在k8s中运行，而k8s是基于容器的，所以每个计算任务最终都会被封装到Docker Image（以下简称Image）内。

一个简单的设想是，我们可以把每个Operator在不同平台上的实现都独立的封装为一个Image，每个Image内包含平台的运行环境，并且需要定义输入输出以及对输入输出进行解析、封装。这种做法下，无论两个相邻的`Operator`是否在同一个平台上运行，都会被封装为两个独立的Image。而同一个平台上`Operator`之间的数据传输原本可以在内存进行，此时却变为了Image之间的传输，带来了额外的延迟。

因此AxPoe的做法是：提前将一个平台上所有的`Operator`封装成为一个运算库，同平台环境一起封装到Image内，运算库由image内的 *Composer* 程序来调度。

TODO: 具体如何将Operator设计为运算库，AxPoe如何通过命令行参数使用Operator，Composer如何接受参数并调度相应的Operator等，可参考[附录](#appendix)。

#### 3.6.2 AxPoe中对不同平台的调用

在2.2和3.5节中均有提到，AxPoe通过Image的描述文件来获知其对应平台的相关信息，如：平台名称、Image（在repository中）的地址、Image的入口命令、Image的系统参数等。

在AxPoe生成了Physical Plan后，每个Physical Operator都被指派了具体的计算平台。此时，AxPoe会根据描述文件，为相邻的、在同一计算平台上的Operator所组成的sub-plan添加一些必要的运行参数；该参数将连通sub-plan一起作为参数传入平台的Image中。

而平台的实际运行是基于Argo Workflow的，因此AxPoe在为平台的Image生成了参数后，下一步就是将平台属性和参数一起，生成Argo Workflow所需的YAML文件以便部署到k8s。

下图为Sub-plan的示意图，其中虚线框内即为某一平台上的sub-plan：

<img src="https://s1.ax1x.com/2020/07/21/U5QCGT.md.png" alt="U5QCGT.md.png" style="zoom:50%;" />

#### 3.6.3 由Physical Plan生成Argo Workflow并部署到k8s

AxPoe基于[Argo Workflow](https://argoproj.github.io/argo/)来将所有计算任务部署到k8s，计算任务在Argo中被称为Workflow，而Workflow最终又以YAML文件的形式被提交到k8s。**因此，AxPoe需要将Physical Plan数据结构转译为Workflow的形式，并输出为YAML文件，这通过`ArgoVisitor`来完成。**

Argo 中支持DAG形式的Workflow，DAG中的节点是一个 `template`代表一个docker image；节点之间通过`dependencies`字段链接，每个`dependencies`中声明上下游节点。

得益于上一章中提到的“平台已被提前封装为image”，在这里**`template`也可提前生成**，下面的代码片段为预先为java平台生成的template字段：

```yaml
name: java-template
inputs:
  parameters:
    - name: javaArgs
container:
  image: executable-java:v1
  imagePullPolicy: IfNotPresent
  command: ["/bin/sh","-c"]
  args: ["{{inputs.parameters.javaArgs}}"]
  volumeMounts:
    - mountPath: /data
      name: job-volume
```

因此AxPoe在转译时仅需要完成两件事：

1. 向`template`传入参数

   要传入的参数包括：平台启动时的运行参数、平台接收的Plan 序列。参数格式已在附录中举例列出，在此不再赘述。

2. 将依赖关系输出为`dependencies`

下面的代码片段是向`template`中传入上述参数后生成的YAML文件：

```yaml
apiVersion: argoproj.io/v1alpha1
kind: Workflow
metadata:
  generateName: compute-scheduling-flow
spec:
  entrypoint: my-flow
  volumes:
  - name: job-volume
    hostPath:
      path: /home/dqh/codes/k8s_basics/executable-java/data
      type: Directory
  templates:
  - name: my-flow
    dag:
      tasks:
      - name: Job-java-SourceFilter573-1
        template: java-template
        arguments:
          parameters:
          - name: javaArgs
            value: 'java -jar executable-java.jar --udfPath=/data/TestSmallWebCaseFunc.class --operator=SourceOperator --input=data/test.csv --separator=, --operator=FilterOperator --udfName=filterFunc --operator=SinkOperator --output=data/1.csv --separator=, '
      - name: Job-spark-Map588-2
        template: spark-template
        arguments:
          parameters:
          - name: sparkArgs
            value: 'spark submit executable-spark.jar --udfPath=/data/TestSmallWebCaseFuncSpark.class --operator=SourceOperator --input=data/1.csv --separator=, --operator=MapOperator --udfName=mapFunc --operator=SinkOperator --output=data/2.csv --separator=, '
        dependencies:
        - Job-java-SourceFilter573-1
      - name: Job-java-ReduceByKeySortSink434-3
        template: java-template
        arguments:
          parameters:
          - name: javaArgs
            value: 'java -jar executable-java.jar --udfPath=/data/TestSmallWebCaseFunc.class --operator=SourceOperator --input=data/2.csv --separator=, --operator=ReduceByKeyOperator --keyName=reduceKey --udfName=reduceFunc --operator=SortOperator --udfName=sortFunc --operator=SinkOperator --output=data/output.csv --separator=, '
        dependencies:
        - Job-spark-Map588-2
  - name: java-template
    inputs:
      parameters:
      - name: javaArgs
    container:
      image: executable-java:v1
      imagePullPolicy: IfNotPresent
      command:
      - /bin/sh
      - -c
      args:
      - '{{inputs.parameters.javaArgs}}'
      volumeMounts:
      - mountPath: /data
        name: job-volume
  - name: spark-template
    inputs:
      parameters:
      - name: sparkArgs
    container:
      image: executable-spark:v1
      imagePullPolicy: IfNotPresent
      command:
      - /bin/sh
      - -c
      args:
      - '{{inputs.parameters.sparkArgs}}'
      volumeMounts:
      - mountPath: /data
        name: job-volume
```

## 附录

