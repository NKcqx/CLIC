package fdu.daslab.jobcenter;

import fdu.daslab.common.thrift.ThriftServer;
import fdu.daslab.jobcenter.service.JobHandler;
import fdu.daslab.thrift.jobcenter.JobService;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;

/**
 * 任务中心的微服务启动类，任务中心负责管理 任务的提交 和 任务的状态
 *
 * @author 唐志伟
 * @version 1.0
 * @since 5/18/21 2:57 PM
 */
@SpringBootApplication(scanBasePackages = {"fdu.daslab"})
public class JobCenterApplication {

    public static void main(String[] args) {
        ApplicationContext context = SpringApplication.run(JobCenterApplication.class, args);
        try {
            // 处理类
            JobHandler jobHandler = context.getBean(JobHandler.class);
            int port = Integer.parseInt(context.getEnvironment().getRequiredProperty("thrift.port"));
            JobService.Processor<JobService.Iface> processor = new JobService.Processor<>(jobHandler);
            ThriftServer.start(port, processor);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
