namespace java fdu.daslab.thrift.master
namespace py fdu.daslab.thrift.master

include "base.thrift"

// 只负责接收各个平台发送的消息，然后将事件保存在事件队列中，等待被调度处理
service SchedulerService {
	// stage执行开始，向driver上报（比如数据大小情况可以上报）
	base.ServiceBaseResult postStageStarted(1: string stageId,
					2: optional map<string, string> submitMsg);
	// stage数据准备结束（可以进行下一stage的调度）
	base.ServiceBaseResult postDataPrepared(1: string stageId);
	// stage执行结束
	base.ServiceBaseResult postStageCompleted(1: string stageId,
					2: optional map<string, string> submitMsg);
}

// 负责接收用户发来的信息，包含提交plan，和对plan的状态的查看等
service TaskService {
    // 异步的，直接提交 TODO: 未来driver可能需要常驻后台和master进行交互
    void submitPlan(1: string planName, 2: string planDagPath);
}