package fdu.daslab.operatorcenter;

import fdu.daslab.common.thrift.ThriftServer;
import fdu.daslab.operatorcenter.service.OperatorHandler;
import fdu.daslab.thrift.operatorcenter.OperatorCenter;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;

/**
 * @author 唐志伟
 * @version 1.0
 * @since 5/18/21 2:44 PM
 */
@SpringBootApplication(scanBasePackages = {"fdu.daslab"})
public class OperatorCenterApplication {

    public static void main(String[] args) {
        ApplicationContext context = SpringApplication.run(OperatorCenterApplication.class, args);
        try {
            OperatorHandler operatorHandler = context.getBean(OperatorHandler.class);
            int port = Integer.parseInt(context.getEnvironment().getRequiredProperty("thrift.port"));
            OperatorCenter.Processor<OperatorCenter.Iface> processor = new OperatorCenter.Processor<>(operatorHandler);
            ThriftServer.start(port, processor);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
