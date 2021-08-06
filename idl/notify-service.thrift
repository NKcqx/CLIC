namespace java fdu.daslab.thrift.notifyservice
namespace py thrift.notifyservice

/*
    定义一个接口，用于下游向上层同步调度状态
    现在采用rpc的方式通知，为了可能采用更简单的方式
    为什么采用 Push的方式，因为性能相对来说比pull更好，pull需要下游维护一个状态更新，虽然对于kubernetes可以通过
    定时查询api server的方式去获取，但是未来可能存在 其他的底层执行环境，比如 超算等，pull的方式就难以实现
 */

// 将生成的代码直接复制到实现的平台的basic代码下面

// 为什么重新写一个status，只是为了尽量减少下游平台的使用成本
enum StageStatus {
    PENDING // 已经提交并等待运行
    RUNNING // 正在运行
    COMPLETED   // 运行完成
    FAILURE // 运行失败
}
// stage当前的执行情况
struct StageSnapshot {
    1: StageStatus status // 当前的状态
    2: string message = "" // 一些信息，比如错误日志
    16: map<string, string> others = {}// 其他的补充信息
}

service NotifyService {
    // 下层服务向上层上报状态信息
    void postStatus(1: string jobName, 2: i32 stageId, 3: StageSnapshot stageSnapShot) // 更新当前任务的状态
}

