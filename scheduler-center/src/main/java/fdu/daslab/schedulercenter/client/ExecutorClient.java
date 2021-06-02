package fdu.daslab.schedulercenter.client;

import fdu.daslab.common.thrift.ThriftClient;
import fdu.daslab.thrift.executorcenter.ExecutorService;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

/**
 * @author 唐志伟
 * @version 1.0
 * @since 6/1/21 8:30 PM
 */
@Service
public class ExecutorClient extends ThriftClient<ExecutorService.Client> {

    @Value("${thrift.executor.host}")
    private String host;

    @Value("${thrift.executor.port}")
    private int port;

    @Override
    protected ExecutorService.Client createClient(TBinaryProtocol protocol) {
        return new ExecutorService.Client(protocol);
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
