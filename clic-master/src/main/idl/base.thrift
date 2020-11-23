namespace java fdu.daslab.thrift.base
namespace py fdu.daslab.thrift.base

//// 数据传输方式的枚举
//enum TransType {
//	RDMA,
//	SOCKET,
//	HDFS
//}
//
//// 传输需要的控制参数信息，thrift不建议使用map直接封装所有参数
//// socket: socket的ip:port; hdfs: hdfs的文件路径和文件名; RDMA: 类似的信息
//struct TransParams {
//	1: TransType transType, // 传输方式
//	2: string location,	// 数据提供方的数据位置参数
//	3: optional map<string, string> others // 其他辅助参数
//}

// 返回结果
enum ResultCode {
	SUCCESS,
	PARAMETER_ERROR,
	EXCEPTION
}

struct ServiceBaseResult {
	1: ResultCode resultCode,
	2: string message
}
