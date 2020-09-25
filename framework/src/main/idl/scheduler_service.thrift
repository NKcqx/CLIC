namespace java driver
namespace py driver

include "base.thrift"

// 只负责接收各个平台发送的消息，然后将事件保存在事件队列中，等待被调度处理
service SchedulerService {
	// stage执行开始，向driver上报（比如数据大小情况可以上报）
	base.ServiceBaseResult postStageStarted(1: i32 stageId,
					2: optional map<string, string> submitMsg);
	// stage数据准备结束（可以进行下一stage的调度）
	base.ServiceBaseResult postDataPrepared(1: i32 stageId);
	// stage执行结束
	base.ServiceBaseResult postStageCompleted(1: i32 stageId,
					2: optional map<string, string> submitMsg);
}