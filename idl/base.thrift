// 定义有关整个平台通用的
namespace java fdu.daslab.thrift.base
namespace py fdu.daslab.thrift.base

// 平台，表示一个具体的物理平台的实体类，比如spark、tensorflow等
struct Platform {
    1: string name // 平台名称
    2: string defaultImage // 默认使用的image
    3: bool useOperator // 是否使用kubernetes operator的方式创建平台
    4: string execCommand // 执行的命令
    16: map<string, string> params // 平台所需要的补充的信息，比如平台的配置信息
}

// 算子的结构
enum OperatorStructure {
    SOURCE // 表示0个输入，1个输出
    SINK // 表示1个输入，0个输出
    MAP // 表示1个输入，1个输出
    JOIN   // 表示有多个输入，一个输出
    FORK // 表示一个输入，多个输出，结构不常见
    // 可能还需要添加其他的，向后添加
}

// 算子，表示一个具体操作的实体类，比如MapOperator等
// TODO: operator的参数
struct Operator {
    1: string name // 算子的名称
    2: list<string> possiblePlatforms // 算子对应的平台的所有可能实现
//    3: string platformName // 所属的平台，如果为空，表示可以在任意平台上运行，也就是logical operator
    4: OperatorStructure operatorStructure // 算子的结构
    5: string operatorType // 算子的计算类型
    6: string computeComplex // 算子的计算复杂度
    7: list<string> inputKeys // 输入参数，只作为下游实现时使用，和用户无关
    8: list<string> outputKeys // 输出参数，只作为下游实现时使用，和用户无关
    16: map<string, string> params // 算子所需要的参数
}

// 执行计划的节点
struct PlanNode {
    1: i32 nodeId // 节点id
    2: Operator operatorInfo // 节点信息
    3: list<i32> inputNodeId   // 依赖的父节点的id
    4: list<i32> outputNodeId // 依赖的孩子节点的id
    5: string platformName // 该计划的执行的平台
}

// 执行计划，用来描述一次用户提交的具体的执行逻辑
struct Plan {
    1: map<i32, PlanNode> nodes // 保存该计划的所有节点，key=nodeId
    2: list<i32> sourceNodes // 所有开始节点的id
}

enum ExecutionStatus {
    PENDING // 已经提交并等待运行
    RUNNING // 正在运行
    COMPLETED   // 运行完成
    FAILURE // 运行失败
}

// 描述一个SubPlan，其所有的node都在一个平台上，是最终执行的单位
struct Stage {
    1: i32 stageId
    2: Plan planInfo // subplan的相关信息
    3: string platformName // 所在的运行的平台
    4: list<i32> inputStageId // 前驱的stageid
    5: list<i32> outputStageId // 后继的stageId
    6: ExecutionStatus stageStatus // stage的运行状态
    7: string startTime // stage的开始时间
    8: string endTime // stage的结束时间
    9: string jobName // 所属的job名称
}

// 描述一个优化好的plan，其由若干subplan组成
struct Job {
    1: string jobName // 这一个任务的任务名
    2: map<i32, Stage> subplans // 保存这个plan的所有subplan
    3: list<i32> sourceStages // 该计划的所有开始节点
    4: ExecutionStatus jobStatus // 任务的执行状态
    5: string startTime // 任务的开始时间
    6: string endTime // 任务的结束时间
    16: map<string, string> others // 其他的补充信息
}