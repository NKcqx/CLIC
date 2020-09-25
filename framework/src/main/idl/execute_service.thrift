namespace java fdu.daslab.executable.service
namespace py fdu.daslab.executable.service

include "base.thrift"

// 定义执行的服务，所有的平台的服务都一样（也就是有同样的客户端和服务端代码），只不过调用时使用不同的实现
service ExecuteService {
	// 启动workflow，传入的是数据的传输方式 和 接收需要的信息
	// 统一一般的source和文件的source
	base.ServiceBaseResult execute(1: base.TransParams transParams);
}