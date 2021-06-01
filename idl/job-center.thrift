namespace java fdu.daslab.thrift.jobcenter
namespace py fdu.daslab.thrift.jobcenter

include 'base.thrift'

// 定义有关任务中心的相关微服务接口
service JobService {
    void submit(1: base.Plan plan, 2: string jobName) // 提交一个任务
    base.Job findJob(1: string jobName) // 查询job目前的状态
    void updateJob(1: base.Job job) // 更新job状态
}