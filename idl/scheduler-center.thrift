namespace java fdu.daslab.thrift.schedulercenter
namespace py fdu.daslab.thrift.schedulercenter

include 'base.thrift'

// 调度器的类型
enum PluginType {
    SORT_PLUGIN // 对调度器进行排序
    FILTER_PLUGIN // 筛选出可能的调度选择，类似于剪枝
    BIND_PLUGIN // 决定每一个调度的结果
}

// 调度器定义
struct SchedulerModel {
    1: string name
    2: i32 priority // 调度器的优先级
    3: PluginType pluginType // 调度器类型
    4: string host // 调度器对应的host
    5: i32 port // 调度器对应的端口
    16: map<string, string> others // 其他的补充信息
}

/*
    调度决定了两件事：1.不同任务之间的顺序 2.任务的放置位置
 */

// 定义有关调度器的相关微服务接口
service SchedulerService {
    void registerScheduler(1: SchedulerModel schedulerInfo) // 添加一个调度器
    void schedule(1: base.Job job) // 对任务进行调度并执行
    void postStatus(1: string jobName, 2: i32 stageId, 3: base.ExecutionStatus status) // 更新当前任务的状态
}

// 定义一个实现插件式的拓展调度器的基础service
// 排序调度器，返回调度器调度后的顺序
// 可能需要其他参数
service SortPlugin {
    list<base.Stage> sort(1: list<base.Stage> stageList) // 对区间内的任务进行排序
}
// 预选调度器，返回可能的调度结果，可能需要底层集群的信息
service FilterPlugin {
    map<string, list<string>> filter(1: list<base.Stage> stageList)
}
// 获取调度的可能结果，可能需要底层集群的信息
service BindPlugin {
    map<string, string> bind(1: list<base.Stage> stage)
}
