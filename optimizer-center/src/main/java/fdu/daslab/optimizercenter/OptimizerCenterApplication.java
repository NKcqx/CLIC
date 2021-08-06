package fdu.daslab.optimizercenter;

import fdu.daslab.common.thrift.ThriftServer;
import fdu.daslab.optimizercenter.service.OptimizerHandler;
import fdu.daslab.thrift.optimizercenter.OptimizerService;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;

/**
 * @author 唐志伟
 * @version 1.0
 * @since 5/18/21 4:28 PM
 */
@SpringBootApplication(scanBasePackages = {"fdu.daslab"})
public class OptimizerCenterApplication {

    public static void main(String[] args) {
        ApplicationContext context = SpringApplication.run(OptimizerCenterApplication.class, args);
        try {
            OptimizerHandler optimizerHandler = context.getBean(OptimizerHandler.class);
            int port = Integer.parseInt(context.getEnvironment().getRequiredProperty("thrift.port"));
            OptimizerService.Processor<OptimizerService.Iface> processor = new OptimizerService.Processor<>(optimizerHandler);
            ThriftServer.start(port, processor);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
