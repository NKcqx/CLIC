namespace java fdu.daslab.thrift.schedulercenter
namespace py fdu.daslab.thrift.schedulercenter

include 'base.thrift'

// 调度器定义
struct SchedulerModel {
    1: string name
    4: string host // 优化器对应的host
    5: i32 port // 优化器对应的端口
    16: map<string, string> others // 其他的补充信息
}

/*
    调度决定了两件事：1.不同任务之间的顺序 2.任务的放置位置
 */
// 调度的结果
struct SchedulerResult {
    1: list<base.Stage> stageList // 返回的调度的顺序
    2: map<string, string> stagePlacement // 任务的放置结果
    16: map<string, string> others // 其他的补充信息
}

// 定义有关调度器的相关微服务接口
service SchedulerService {
    void registerScheduler(1: SchedulerModel schedulerInfo) // 添加一个调度器
    void schedule(1: base.Job job) // 对任务进行调度并执行
}

// 定义一个实现插件式的拓展调度器的基础service
// 上线之前，需要先注册一下
service SchedulerPlugin {

    SchedulerResult schedule(1: list<base.Stage> stageList) // 对区间内的任务进行调度
}
