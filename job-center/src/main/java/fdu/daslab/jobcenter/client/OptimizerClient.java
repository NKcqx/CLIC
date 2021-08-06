package fdu.daslab.jobcenter.client;

import fdu.daslab.common.thrift.ThriftClient;
import fdu.daslab.thrift.optimizercenter.OptimizerService;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

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

}
