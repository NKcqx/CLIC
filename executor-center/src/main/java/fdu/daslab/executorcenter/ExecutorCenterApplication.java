package fdu.daslab.executorcenter;

import fdu.daslab.executorcenter.service.ExecutorHandler;
import fdu.daslab.common.thrift.ThriftServer;
import fdu.daslab.thrift.executorcenter.ExecutorService;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;

/**
 * 按照给定的stage，执行相应的应用，支持 local 和 kubernetes的方式
 *
 * @author 唐志伟
 * @version 1.0
 * @since 5/18/21 7:49 PM
 */
@SpringBootApplication(scanBasePackages = {"fdu.daslab"})
public class ExecutorCenterApplication {

    public static void main(String[] args) {
        ApplicationContext context = SpringApplication.run(ExecutorCenterApplication.class, args);
        try {
            ExecutorHandler executorHandler = context.getBean(ExecutorHandler.class);
            int port = Integer.parseInt(context.getEnvironment().getRequiredProperty("thrift.port"));
            ExecutorService.Processor<ExecutorService.Iface> processor = new ExecutorService.Processor<>(executorHandler);
            ThriftServer.start(port, processor);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
