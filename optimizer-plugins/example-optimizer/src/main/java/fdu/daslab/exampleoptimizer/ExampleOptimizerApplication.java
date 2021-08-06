package fdu.daslab.exampleoptimizer;

import fdu.daslab.exampleoptimizer.client.OptimizerClient;
import fdu.daslab.exampleoptimizer.service.ExampleOptimizerHandler;
import fdu.daslab.common.thrift.ThriftServer;
import fdu.daslab.thrift.optimizercenter.OptimizerPlugin;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;

/**
 * 实现一个简单的实例的拓展optimizer
 *
 * @author 唐志伟
 * @version 1.0
 * @since 5/18/21 2:57 PM
 */
@SpringBootApplication(scanBasePackages = {"fdu.daslab"})
public class ExampleOptimizerApplication {

    public static void main(String[] args) {
        ApplicationContext context = SpringApplication.run(ExampleOptimizerApplication.class, args);
        try {
            // 先注册自己的信息到调度器中心
            OptimizerClient client = context.getBean(OptimizerClient.class);
            client.register();
            // 启动优化器
            ExampleOptimizerHandler exampleOptimizerHandler = context.getBean(ExampleOptimizerHandler.class);
            int port = Integer.parseInt(context.getEnvironment().getRequiredProperty("thrift.port"));
            OptimizerPlugin.Processor<OptimizerPlugin.Iface> processor = new OptimizerPlugin.Processor<>(exampleOptimizerHandler);
            ThriftServer.start(port, processor);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
