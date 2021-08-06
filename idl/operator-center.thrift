namespace java fdu.daslab.thrift.operatorcenter
namespace py fdu.daslab.thrift.operatorcenter

include 'base.thrift'

// 定义有关算子平台的相关微服务接口
service OperatorCenter {
    // platform相关接口
    void addPlatform(1: base.Platform platform) // 新增一个平台
    base.Platform findPlatformInfo(1: string platformName) // 查询平台的信息
    map<string, base.Platform> listPlatforms() // 查询所有的平台

    // operator相关接口
    void addOperator(1: base.Operator operator) // 新增一个operator
    base.Operator findOperatorInfo(1: string operatorName, 2: string platformName) // 查询算子的信息
}