namespace java fdu.daslab.thrift.executorcenter
namespace py fdu.daslab.thrift.executorcenter

include 'base.thrift'

// 将某一个平台放到具体的kubernetes上执行，这个执行是异步的，也就是会立即返回
// 用户可以使用stageId，查询到这一个stage的状态
service ExecutorService {
    void executeStage(1: base.Stage stage)
}