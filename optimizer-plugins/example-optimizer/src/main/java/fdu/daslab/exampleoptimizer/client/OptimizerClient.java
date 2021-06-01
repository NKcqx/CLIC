package fdu.daslab.exampleoptimizer.client;

import fdu.daslab.common.thrift.ThriftClient;
import fdu.daslab.thrift.optimizercenter.OptimizerModel;
import fdu.daslab.thrift.optimizercenter.OptimizerService;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * 优化器的thrift client
 *
 * @author 唐志伟
 * @version 1.0
 * @since 5/18/21 3:16 PM
 */
@Component
public class OptimizerClient extends ThriftClient<OptimizerService.Client> {

    @Value("${thrift.optimizer.host}")
    private String host;

    @Value("${thrift.optimizer.port}")
    private int port;

    @Value("${thrift.host}")
    private String pluginHost;

    @Value("${thrift.port}")
    private int pluginPort;

    @Value("${register.name}")
    private String name;

    @Value("${register.priority}")
    private int priority;

    @Override
    public String getHost() {
        return host;
    }

    @Override
    public int getPort() {
        return port;
    }

    @Override
    protected OptimizerService.Client createClient(TBinaryProtocol protocol) {
        return new OptimizerService.Client(protocol);
    }

    // 注册信息到调度器中心
    public void register() throws TException {
        OptimizerModel model = new OptimizerModel();
        model.setName(name);
        model.setHost(pluginHost);
        model.setPort(pluginPort);
        model.setPriority(priority);
        open();
        getClient().registerOptimizer(model);
        close();
    }

}
