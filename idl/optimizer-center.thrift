namespace java fdu.daslab.thrift.optimizercenter
namespace py fdu.daslab.thrift.optimizercenter

include 'base.thrift'

// 优化器定义
struct OptimizerModel {
    1: string name
    2: i32 priority // 优先级
    3: list<string> allowedPlatforms // 允许优化的平台，空表示所有平台
    4: string host // 优化器对应的host
    5: i32 port // 优化器对应的端口
    16: map<string, string> others // 其他的补充信息
}

// 定义有关优化器的相关微服务接口
service OptimizerService {
    void registerOptimizer(1: OptimizerModel optimizerInfo) // 添加一个优化器，暴露给底层各种优化器执行时使用
    base.Job optimize(1: base.Plan plan) // 调用所有优化器对plan进行优化
}

// 定义一个实现插件式的拓展优化器的基础service
// 上线之前，需要先注册一下，包含其名称、优先级、使用范围等，可以在服务的地方注册下
service OptimizerPlugin {
    base.Plan optimize(1: base.Plan plan, 2: map<string, base.Platform> platforms)
}
