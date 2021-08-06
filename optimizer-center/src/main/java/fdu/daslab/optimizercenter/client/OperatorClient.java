package fdu.daslab.optimizercenter.client;

import fdu.daslab.common.thrift.ThriftClient;
import fdu.daslab.thrift.operatorcenter.OperatorCenter;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * @author 唐志伟
 * @version 1.0
 * @since 6/11/21 4:16 PM
 */
@Component
public class OperatorClient extends ThriftClient<OperatorCenter.Client> {

    @Value("${thrift.operator.host}")
    private String host;

    @Value("${thrift.operator.port}")
    private int port;

    @Override
    protected OperatorCenter.Client createClient(TBinaryProtocol protocol) {
        return new OperatorCenter.Client(protocol);
    }

    @Override
    protected String getHost() {
        return host;
    }

    @Override
    protected int getPort() {
        return port;
    }
}
